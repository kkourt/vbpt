/*
 * versioned b+(plus) tree
 */

/**
 * Note: most operations are on nodes which are on the last level of the path.
 * That way, we know exactly which the next node should be based on the slot
 * and we can maintain a correct path.
 */

/**
 * TODO:
 *  - add const for tree pointers
 */

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>

#include "misc.h"
#include "ver.h"
#include "refcnt.h"
#include "vbpt.h"

#define MIN(x,y) ((x) < (y) ? (x) : (y))

static inline uint16_t
imba_limit(vbpt_node_t *node)
{
	assert(node->items_total / 2 > 1);
	return node->items_total / 2;
}

static inline bool
node_imba(vbpt_node_t  *node)
{
	return (node->items_nr <= imba_limit(node));
}


char *
vbpt_hdr_str(vbpt_hdr_t *hdr)
{
	static char buff[128];
	snprintf(buff, sizeof(buff), " (%s cnt:%u) ", ver_str(hdr->ver), hdr->h_refcnt.cnt);
	return buff;
}

void
vbpt_leaf_print(vbpt_leaf_t *leaf, int indent)
{
	printf("%*s" "[leaf=%p ->len=%lu ->total_len=%lu] %s\n",
	        indent, " ",
		leaf, leaf->len, leaf->total_len, vbpt_hdr_str(&leaf->l_hdr));
}

static void
vbpt_node_verify(vbpt_node_t *node)
{
	assert(refcnt_(&node->n_hdr.h_refcnt) > 0);
	assert(node->items_nr > 0);
	vbpt_kvp_t *kvp0 = node->kvp;
	for (unsigned i=1; i < node->items_nr; i++) {
		vbpt_kvp_t *kvp = node->kvp + i;
		if (kvp0->val->type != kvp->val->type) {
			fprintf(stderr,
			        "child %u has type %u and child 0 type %u\n",
			        i, kvp->val->type, kvp0->val->type);
			assert(false);
		}
	}

	if (kvp0->val->type == VBPT_LEAF)
		return;

	/** NOTE: we do this only for internal nodes, because vbpt_log_replay()
	 * does not change the version of the leafs */
	for (unsigned i=0; i < node->items_nr; i++) {
		ver_t *child_ver = node->kvp[i].val->ver;
		if (!ver_leq(child_ver, node->n_hdr.ver)) {
			fprintf(stderr,
			        "child has version %s"
			         "and parent has version %s\n",
			         ver_str(child_ver),
			         ver_str(node->n_hdr.ver));
			ver_path_print(child_ver, stderr);
			ver_path_print(node->n_hdr.ver, stderr);
			assert(false);
		}
	}


	for (unsigned i=0; i < node->items_nr; i++) {
		vbpt_kvp_t *kvp = node->kvp + i;
		uint64_t key = kvp->key;
		vbpt_node_t *c = hdr2node(kvp->val);
		uint64_t high_key = c->kvp[c->items_nr - 1].key;
		if (key != high_key) {
			fprintf(stderr,
			        "child %u of node %p has high_key=%lu"
			        " and node has key=%lu\n",
			        i, node, high_key, key);
			assert(false);
		}

	}
}


void
vbpt_node_print(vbpt_node_t *node, int indent, bool verify)
{
	printf("\n%*s" "[node=%p ->items_nr=%u ->items_total=%u imba_limit=%u] %s\n",
	        indent, " ", node,
		node->items_nr, node->items_total, imba_limit(node),
		vbpt_hdr_str(&node->n_hdr));
	for (unsigned i=0; i < node->items_nr; i++) {
		vbpt_kvp_t *kvp = node->kvp + i;
		printf("%*s" "key=%5lu ", indent, " ", kvp->key);
		if (kvp->val->type == VBPT_NODE)
			vbpt_node_print(hdr2node(kvp->val), indent+4, verify);
		else
			vbpt_leaf_print(hdr2leaf(kvp->val), indent+4);
	}

	if (verify)
		vbpt_node_verify(node);
}

void
vbpt_tree_print(vbpt_tree_t *tree, bool verify)
{
	printf("=====| tree: %p %s ================", tree, ver_str(tree->ver));
	if (tree->root == NULL)
		printf("\nroot => %p\n", NULL);
	else
		vbpt_node_print(tree->root, 2, verify);
	printf("=========================================================\n");
}

bool
vbpt_path_verify(vbpt_tree_t *tree, vbpt_path_t *path)
{
	if (path->nodes[0] != tree->root) {
		fprintf(stderr, "first node of the path is not root\n");
		return false;
	}

	for (uint16_t i=1; i<path->height; i++) {
		vbpt_node_t *parent = path->nodes[i-1];
		uint16_t pslot = path->slots[i-1];
		if (parent->kvp[pslot].val != &path->nodes[i]->n_hdr) {
			fprintf(stderr, "******PATH VERIFICATION FAILED\n");
			vbpt_tree_print(tree, 0);
			fprintf(stderr,
			       " parent = path->node[%u]=%p\n"
			       " pslot  = path->slot[%u]=%u\n"
			       " node   = parent->slots[%u]=%p\n"
			       " which is different from path->node[%u] = %p\n",
			       i-1, parent, i-1, pslot,
			       pslot, hdr2node(parent->kvp[pslot].val),
			       i, path->nodes[i]);

			return false;
		}
	}

	return true;
}

void
vbpt_path_print(vbpt_path_t *path)
{
	printf("PATH:%p", path);
	for (uint16_t i=0; i<path->height; i++) {
		printf(" [node: %p slot:%u]", path->nodes[i], path->slots[i]);
	}
	printf("\n");
}


static vbpt_kvp_t *
find_kvp(vbpt_node_t *node, uint64_t key)
{
	vbpt_kvp_t *kvp = node->kvp;
	for (uint16_t i=0; i<node->items_nr; i++, kvp++)
		if (kvp->key <= key)
			break;
	return kvp;
}

/**
 * find slot for key in node
 * Note that this function can return node->items_total (i.e., an out of bounds
 * slot) if the node is full
 */
static uint16_t
find_slot(vbpt_node_t *node, uint64_t key)
{
	vbpt_kvp_t *kvp = node->kvp;
	for (uint16_t i=0; i<node->items_nr; i++, kvp++) {
		if (key <= kvp->key)
			return i;
	}
	return node->items_nr;
}

static inline vbpt_hdr_t *
find_key(vbpt_node_t *node, uint64_t key)
{
	vbpt_kvp_t *kvp = find_kvp(node, key);
	return kvp->val;
}

/*
 * initialize a header
 *  Version's refcount will be increased
 */
static void
vbpt_hdr_init(vbpt_hdr_t *hdr, ver_t *ver, enum vbpt_type type)
{
	hdr->ver = ver_getref(ver);
	refcnt_init(&hdr->h_refcnt, 1);
	hdr->type = type;
}

/*
 * allocate a new node
 *  Version's refcount will be increased
 */
vbpt_node_t *
vbpt_node_alloc(size_t node_size, ver_t *ver)
{
	assert(node_size > sizeof(vbpt_node_t));
	vbpt_node_t *ret = xmalloc(node_size);
	vbpt_hdr_init(&ret->n_hdr, ver, VBPT_NODE);
	ret->items_nr = 0;
	ret->items_total = (node_size - sizeof(vbpt_node_t)) / sizeof(vbpt_kvp_t);
	return ret;
}

/**
 * free a node
 *  version's refcount will be decreased
 *  children's refcount will be decrased
 */
void
vbpt_node_dealloc(vbpt_node_t *node)
{
	vbpt_kvp_t *kvp = node->kvp;
	for (uint16_t i=0; i<node->items_nr; i++) {
		vbpt_hdr_putref(kvp[i].val);
	}
	node->items_nr = 0;
	ver_putref(node->n_hdr.ver);
	free(node);
}

/**
 * allocate a new leaf
 *  Version's refcount will be increased
 */
vbpt_leaf_t *
vbpt_leaf_alloc(size_t leaf_size, ver_t *ver)
{
	assert(leaf_size > sizeof(vbpt_leaf_t));
	vbpt_leaf_t *ret = xmalloc(leaf_size);
	vbpt_hdr_init(&ret->l_hdr, ver, VBPT_LEAF);
	ret->len = 0;
	ret->total_len = (leaf_size - sizeof(vbpt_leaf_t));
	return ret;
}
/**
 * free a leaf
 *  vresion's refcount will be decreased
 */
void
vbpt_leaf_dealloc(vbpt_leaf_t *leaf)
{
	ver_putref(leaf->l_hdr.ver);
	free(leaf);
}

/**
 * allocate (and initialize) a new tree.
 *  Version refcnt is not increased
 */
vbpt_tree_t *
vbpt_tree_alloc(ver_t *ver)
{
	vbpt_tree_t *ret = xmalloc(sizeof(vbpt_tree_t));
	ret->ver = ver;
	ret->root = NULL;
	ret->height = 0;
	return ret;
}

vbpt_tree_t *
vbpt_tree_create(void)
{
	ver_t *ver = ver_create();
	return vbpt_tree_alloc(ver);
}

/**
 * branch a new tree -- grabs a reference of the root node
 */
vbpt_tree_t *
vbpt_tree_branch(vbpt_tree_t *parent)
{
	vbpt_tree_t *ret = xmalloc(sizeof(vbpt_tree_t));
	ret->ver = ver_branch(parent->ver);
	ret->root = vbpt_node_getref(parent->root);
	ret->height = parent->height;
	return ret;
}

/**
 * deallocate a tree descriptor
 *  decrease version reference count
 */
void
vbpt_tree_dealloc(vbpt_tree_t *tree)
{
	ver_putref(tree->ver);
	if (tree->root != NULL)
		vbpt_node_putref(tree->root);
	free(tree);
}

/**
 * insert a  pointer to a node
 *  input invariance: enough space exists
 *  this function might end  up shifting keys
 *  val's reference count will not be increased
 *
 * If the slot is already occupied with the same key, replace slot and return
 * old value
 */
static vbpt_hdr_t *
insert_ptr(vbpt_node_t *node, uint16_t slot, uint64_t key, vbpt_hdr_t *val)
{
	assert(slot < node->items_total);
	assert(slot <= node->items_nr);
	// this assumption is false: merges might add leafs with other versions
	// TODO: maybe change the assert to an inequality about versions
	//assert(node->n_hdr.ver == val->ver);

	vbpt_kvp_t *kvp = node->kvp + slot;
	if (slot < node->items_nr && kvp->key == key) {
		vbpt_hdr_t *old = kvp->val;
		kvp->val = val;
		return old;
	}

	if (slot > node->items_nr) {
		assert(false); // note that node->items_nr == slot is OK
	} else if (slot < node->items_nr) {
		// need to shift
		memmove(kvp+1, kvp, (node->items_nr - slot)*sizeof(vbpt_kvp_t));
	}

	kvp->key = key;
	kvp->val = val;
	node->items_nr++;
	return NULL;
}


#define insert_ptr_empty(node, slot, key, hdr)               \
do {                                                         \
	vbpt_hdr_t *t__ = insert_ptr(node, slot, key, hdr);  \
	assert(t__ == NULL);                                 \
} while (0)

/**
 * non-static wrapper for insert_ptr()
 */
vbpt_hdr_t *
vbpt_insert_ptr(vbpt_node_t *node, uint16_t slot, uint64_t key, vbpt_hdr_t *val)
{
	return insert_ptr(node, slot, key, val);
}

/**
 * copy key-values in a node -- grabs new references
 */
static void
copy_node(vbpt_node_t *dst, vbpt_node_t *src)
{
	assert(dst->items_total >= src->items_total);
	for (unsigned i=0; i<src->items_nr; i++) {
		dst->kvp[i].key = src->kvp[i].key;
		dst->kvp[i].val = vbpt_hdr_getref(src->kvp[i].val);
	}
	dst->items_nr = src->items_nr;
}

/**
* cow node at @parent_slot in @parent
* The new node will be placed in @parent_slot in @parent and it will have the
* version of the parent
* The old node will be decrefed
* nodes that were referenced in the old node will be increfed
* new node is returned
*/
static vbpt_node_t *
cow_node(vbpt_tree_t *tree, vbpt_node_t *parent, uint16_t parent_slot)
{
	assert(parent_slot < parent->items_nr);
	ver_t *ver = vbpt_tree_ver(tree);
	assert(parent->n_hdr.ver == ver);
	uint64_t key = parent->kvp[parent_slot].key;
	vbpt_node_t *old = hdr2node(parent->kvp[parent_slot].val);
	vbpt_node_t *new = vbpt_node_alloc(VBPT_NODE_SIZE, ver);
	copy_node(new, old);
	insert_ptr(parent, parent_slot, key, &new->n_hdr);
	vbpt_node_putref(old);
	return new;
}



static vbpt_node_t *
cow_root(vbpt_tree_t *tree)
{
	ver_t *ver = vbpt_tree_ver(tree);
	vbpt_node_t *new = vbpt_node_alloc(VBPT_NODE_SIZE, ver);
	vbpt_node_t *old = tree->root;
	copy_node(new, old);
	tree->root = new;
	vbpt_node_putref(old);
	return new;
}

/**
 * get left sibling of @node if it exists, or NULL
 */
static vbpt_node_t *
get_left_sibling(vbpt_node_t *node, vbpt_path_t *path)
{
	vbpt_node_t *ret = NULL;
	vbpt_node_t *pnode = path->nodes[path->height-2];
	uint16_t pslot     = path->slots[path->height-2];
	assert(node == path->nodes[path->height-1]);
	assert(node == hdr2node(pnode->kvp[pslot].val));
	if (pslot > 0)
		ret = hdr2node(pnode->kvp[pslot-1].val);
	return ret;
}

static vbpt_node_t *
get_right_sibling(vbpt_node_t *node, vbpt_path_t *path)
{
	vbpt_node_t *ret = NULL;
	vbpt_node_t *pnode = path->nodes[path->height-2];
	uint16_t pslot     = path->slots[path->height-2];
	assert(node == path->nodes[path->height-1]);
	assert(node == hdr2node(pnode->kvp[pslot].val));
	if (pslot < pnode->items_nr - 1)
		ret = hdr2node(pnode->kvp[pslot+1].val);
	return ret;
}

/**
 * highest (rightmost) key of @node has changed, update its ancestors.
 * node lives in @parent_slot in @parent
 * @node's parent is in level @lvl of @path
 *
 * Note that this can happen for two reasons:
 *  - a value was added in the rightmost place of a node
 *  - a value was removed from the rightmost place of a node
 */
static void
update_highkey(vbpt_node_t *node, uint16_t parent_slot,
               vbpt_path_t *path, uint16_t lvl)
{
	assert(path->nodes[lvl]->kvp[parent_slot].val == &node->n_hdr);

	uint64_t high_k = node->kvp[node->items_nr - 1].key;
	while (true) {
		vbpt_node_t *parent = path->nodes[lvl];
		vbpt_kvp_t  *pkvp   = parent->kvp + parent_slot;
		assert(pkvp->val == &node->n_hdr);
		pkvp->key = high_k;

		// if this is the rightmost item, we need to update the parent
		if (parent_slot < parent->items_nr - 1)
			break;

		if (lvl-- == 0)
			break;

		node = parent;
		parent_slot = path->slots[lvl];
	}
}

/**
 * delete the @node's pointer at slot @slot.
 * The node is included in @path at level @lvl
 * delete_ptr() won't decrease the reference of the pointed node
 * delete_ptr() will decrease the reference of the root node, if this was the
 * last element on the root.
 */
static vbpt_hdr_t *
delete_ptr(vbpt_tree_t *tree, vbpt_node_t *node, uint16_t slot,
           vbpt_path_t *path, uint16_t lvl)
{
	assert(lvl < path->height);
	assert(node == path->nodes[lvl]);
	assert(slot < node->items_nr);

	vbpt_kvp_t *kvp = node->kvp + slot;
	vbpt_hdr_t *ret = kvp->val;
	//uint64_t del_key = kvp->key;

	assert(node->items_nr > 1 || node == tree->root);

	uint16_t copy_items = node->items_nr - 1 - slot;
	node->items_nr--;
	if (copy_items) {
		// note that the rightmost item does not change, and there are
		// still items in the node
		kvpmove(kvp, kvp + 1, copy_items);
	} else if (node->items_nr > 0 && node != tree->root) {
		// we changed the last element, there are still elements in the
		// node, and this is not the root node: we need to update the
		// parent pointer
		assert(lvl > 0);
		update_highkey(node, path->slots[lvl-1], path, lvl-1);
	} else if (node->items_nr == 0 && node == tree->root) {
		// no more elements left in root
		vbpt_node_putref(tree->root);
		tree->root = NULL;
		tree->height = 0;
	} else if (node->items_nr == 0) {
		// deleting the last pointer of a non-root node should never
		// happen due to balancing
		assert(false);
	}
	return ret;
}

/**
 * move @mv_items items from @left to @node
 * @node is the last node in @path
 */
static void
move_items_from_left(vbpt_tree_t *tree,
                     vbpt_node_t *node, vbpt_node_t *left,
                     vbpt_path_t *path, uint16_t mv_items)
{
	assert(path->height > 1);
	assert(mv_items > 0);
	vbpt_node_t *pnode = path->nodes[path->height - 2];
	uint16_t pnode_slot = path->slots[path->height -2];

	// Sanity checks:
	//   @node is @path's last node
	assert(path->nodes[path->height-1] == node);
	//   @left is left of @node
	assert(get_left_sibling(node, path) == left);
	//   no need to COW
	assert(node->n_hdr.ver == left->n_hdr.ver);
	//   there is enough space in node
	assert(node->items_total - node->items_nr >= mv_items);
	//   there are anough items in left
	assert(left->items_nr >= mv_items);

	// update @node, @left
	kvpmove(node->kvp + mv_items, node->kvp, node->items_nr);
	kvpcpy(node->kvp, left->kvp + left->items_nr - mv_items, mv_items);
	left->items_nr -= mv_items;
	node->items_nr += mv_items;

	if (left->items_nr == 0) {
		vbpt_hdr_t *d;
		d = delete_ptr(tree, pnode, pnode_slot -1, path, path->height - 2);
		assert(get_left_sibling(node, path) != left);
		assert(d == &left->n_hdr);
		vbpt_node_putref(left);
	} else {
		update_highkey(left, pnode_slot -1, path, path->height -2);
	}
}

/**
 * move @mv_items items from @left to @node
 * @node is the last node in @path
 */
static void
move_items_from_right(vbpt_tree_t *tree,
                      vbpt_node_t *node, vbpt_node_t *right,
                      vbpt_path_t *path, uint16_t mv_items)
{
	assert(path->height > 1);
	assert(mv_items > 0);
	vbpt_node_t *pnode = path->nodes[path->height - 2];
	uint16_t pnode_slot = path->slots[path->height -2];

	// Sanity checks:
	//   @node is @path's last node
	assert(path->nodes[path->height-1] == node);
	//   @right is right of @node
	assert(pnode->kvp[pnode_slot +1].val == &right->n_hdr);
	//   no need to COW
	assert(node->n_hdr.ver == right->n_hdr.ver);
	//   there is enough space in node
	assert(node->items_total - node->items_nr >= mv_items);
	//   there are anough items in left
	assert(right->items_nr >= mv_items);

	// update @node, @right
	kvpcpy(node->kvp + node->items_nr, right->kvp, mv_items);
	node->items_nr += mv_items;
	right->items_nr -= mv_items;
	if (right->items_nr > 0) {
		kvpmove(right->kvp, right->kvp + mv_items, right->items_nr);
	} else {
		vbpt_hdr_t  *d;
		d = delete_ptr(tree, pnode, pnode_slot +1, path, path->height - 2);
		assert(d == &right->n_hdr);
		vbpt_node_putref(right);
	}

	update_highkey(node, pnode_slot, path, path->height -2);
}


/**
 * move @mv_items from @node to @left
 * @node is the last node in @path
 * @left is the left sibling of @node
 * @path is updated
 */
static void
move_items_left(vbpt_tree_t *tree,
                vbpt_node_t *node, vbpt_node_t *left,
	        vbpt_path_t *path, uint16_t mv_items)
{
	assert(path->height > 1);
	assert(mv_items > 0);
	vbpt_node_t *pnode = path->nodes[path->height - 2];
	uint16_t pnode_slot = path->slots[path->height -2];
	uint16_t node_slot = path->slots[path->height -1];

	// Sanity checks:
	//   @node is @path's last node
	assert(path->nodes[path->height-1] == node);
	//   @left is left of @node
	assert(pnode->kvp[pnode_slot -1].val == &left->n_hdr);
	//   no need to COW
	assert(node->n_hdr.ver == left->n_hdr.ver);
	//   there are enough items in node
	assert(node->items_nr >= mv_items);
	//   there is enough space in @left
	assert(left->items_total - left->items_nr >= mv_items);

	// update @left
	kvpcpy(left->kvp + left->items_nr, node->kvp, mv_items);
	left->items_nr += mv_items;
	// update @node
	uint16_t node_items = node->items_nr - mv_items;
	if (node_items > 0) { // move remaining @node items
		kvpmove(node->kvp, node->kvp + mv_items, node_items);
		node->items_nr = node_items;
	} else {                 // node is now empty
		vbpt_hdr_t *d;
		node->items_nr = 0;
		d = delete_ptr(tree, pnode, pnode_slot, path, path->height - 2);
		assert(d == &node->n_hdr);
		vbpt_node_putref(node);
	}
	// update @path
	if (node_slot > mv_items) { // stayed in the same node
		node_slot = node_slot - mv_items;
	} else { // changed node
		node_slot = left->items_nr - mv_items + node_slot;
		path->nodes[path->height - 1] = left;
		path->slots[path->height - 2] = pnode_slot -1;
	}
	path->slots[path->height - 1] = node_slot;
	// update parent high value
	update_highkey(left, pnode_slot -1, path, path->height - 2);
	assert(vbpt_path_verify(tree, path));
}

/**
 * move @mv_items from @node to @right
 * @node is the last node in @path
 * @right is the right sibling of @node
 * @path is updated
 */
static void
move_items_right(vbpt_tree_t *tree,
                 vbpt_node_t *node, vbpt_node_t *right,
                 vbpt_path_t *path, uint16_t mv_items)
{
	assert(path->height > 1);
	vbpt_node_t *pnode = path->nodes[path->height - 2];
	uint16_t pnode_slot = path->slots[path->height -2];
	uint16_t node_slot = path->slots[path->height -1];

	// Sanity checks:
	//   @node is @path's last node
	assert(path->nodes[path->height-1] == node);
	//   @right is right of @node
	assert(get_right_sibling(node, path) == right);
	//   no need to COW
	assert(node->n_hdr.ver == right->n_hdr.ver);
	//   there are enough items in @node
	assert(node->items_nr >= mv_items);
	//   there is enough space in @right
	assert(right->items_total - right->items_nr >= mv_items);

	kvpmove(right->kvp + mv_items, right->kvp, right->items_nr);
	kvpcpy(right->kvp, node->kvp + (node->items_nr - mv_items), mv_items);
	node->items_nr -= mv_items;
	right->items_nr += mv_items;
	// update @node
	uint16_t delete_node = 0;
	if (node->items_nr == 0) {
		vbpt_hdr_t *d;
		d = delete_ptr(tree, pnode, pnode_slot, path, path->height -2);
		assert(d == &node->n_hdr);
		delete_node = 1;
	}
	// update @path
	if (node_slot >= node->items_nr) {
		path->nodes[path->height - 1] = right;
		path->slots[path->height - 1] = node_slot - node->items_nr;
		path->slots[path->height - 2] = pnode_slot +1 - delete_node;
	}
	if (delete_node) // release node
		vbpt_node_putref(node);
	else            // update ancestors
		update_highkey(node, pnode_slot, path, path->height -2);
	assert(vbpt_path_verify(tree, path));
}

/**
 * move @left_items from @left, and then @right_items from @right
 * @node is between @left and @right
 * @path's last node is @node
 * @path is updated
 * Maybe use this function for move_items_{left,right} as well
 */
static void
move_items_left_right(vbpt_tree_t *tree, vbpt_node_t  *node,
                      vbpt_node_t *left, uint16_t left_items,
                      vbpt_node_t *right, uint16_t right_items,
                      vbpt_path_t *path)
{
	assert(path->height > 1);
	vbpt_node_t *pnode = path->nodes[path->height -2];
	uint16_t pnode_slot = path->slots[path->height -2];
	uint16_t node_slot = path->slots[path->height -1];

	// Sanity checks:
	//   @node is @path's last node
	assert(path->nodes[path->height-1] == node);
	//   @right is right of @node
	assert(pnode->kvp[pnode_slot+1].val == &right->n_hdr);
	//   @left  is left of @node
	assert(pnode->kvp[pnode_slot-1].val == &left->n_hdr);
	//   no need to COW
	assert(node->n_hdr.ver == left->n_hdr.ver);
	assert(node->n_hdr.ver == right->n_hdr.ver);
	//   there are enough items in @node
	assert(node->items_nr >= right_items + left_items);
	//   there is enough space in @right
	assert(right->items_total - right->items_nr >= right_items);
	//   there is enough space in @left
	assert(left->items_total - left->items_nr >= left_items);
	//   both copies are needed
	assert(left_items > 0 && right_items > 0);

	// update @left
	kvpcpy(left->kvp + left->items_nr, node->kvp, left_items);
	left->items_nr += left_items;
	kvpmove(node->kvp, node->kvp + left_items, node->items_nr);
	node->items_nr -= left_items;
	// update @right
	kvpmove(right->kvp + right_items, right->kvp, right->items_nr);
	kvpcpy(right->kvp, node->kvp + node->items_nr - right_items, right_items);
	right->items_nr += right_items;
	node->items_nr -= right_items;

	uint16_t node_deleted = 0;
	if (node->items_nr == 0) {
		vbpt_hdr_t *d;
		d = delete_ptr(tree, pnode, pnode_slot, path, path->height -2);
		assert(d == &node->n_hdr);
		node_deleted = 1;
	}
	// update @path
	uint16_t left_slot = pnode_slot - 1;
	if (node_slot < left_items) { // left node
		node_slot = left->items_nr - left_items + node_slot;
		path->nodes[path->height -1] = left;
		pnode_slot = pnode_slot -1;
	} else if (node_slot < left_items + node->items_nr) { // middle node
		assert(!node_deleted);
		node_slot = node_slot - left_items;
	} else { // right node
		path->nodes[path->height - 1] = right;
		node_slot -= left_items + node->items_nr;
		pnode_slot = pnode_slot +1 - node_deleted;
	}
	path->slots[path->height -1] = node_slot;
	path->slots[path->height -2] = pnode_slot;
	update_highkey(left, left_slot, path, path->height - 2);

	if (node_deleted)
		vbpt_node_putref(node);
	else
		update_highkey(node, left_slot +1, path, path->height -2);
	assert(vbpt_path_verify(tree, path));

}

/**
 * create a path to a sibling
 * @sibling >0 for right, <0 for left
 * sibling is at @height height
 * after @height fill in nodes from @path, and set slots to zero
 */
static void __attribute__((unused))
make_sibling_path(const vbpt_path_t *path, vbpt_path_t *sibl_path,
                uint16_t height, int sibling)
{
	sibl_path->height = path->height;
	assert(height <= path->height);
	uint16_t i;
	for (i=0; i<height; i++) {
		sibl_path->nodes[i] = path->nodes[i];
		sibl_path->slots[i] = path->slots[i];
	}
	assert(sibling + (int)path->slots[i] < path->nodes[i]->items_nr);
	assert(sibling + (int)path->slots[i] > 0);
	sibl_path->slots[i] += sibling;
	for ( ; i<path->height; i++) {
		sibl_path->nodes[i] = path->nodes[i];
		sibl_path->slots[i] = 0; // dummy
	}
}

/**
 * try balance around without doing any COWs
 *
 * @node is the last node in @path
 * @left is NULL or the left sibling of @node
 * @right is NULL or the right sibling of @node
 */
static void
try_balance_node_nocow(vbpt_tree_t *tree,
                       vbpt_node_t *node, vbpt_node_t *left, vbpt_node_t *right,
                       vbpt_path_t *path)
{
	ver_t *ver = node->n_hdr.ver;
	bool l_merge = left && left->n_hdr.ver == ver;
	bool r_merge = right && right->n_hdr.ver == ver;
	uint16_t l_rem = (l_merge) ? left->items_total  - left->items_nr  : 0;
	uint16_t r_rem = (r_merge) ? right->items_total - right->items_nr : 0;
	if (l_rem >= node->items_nr) {
		// all of @node's item can be placed in @left
		move_items_left(tree, node, left, path, node->items_nr);
		assert(path->nodes[path->height -1] == left);
	} else if (r_rem >= node->items_nr) {
		move_items_right(tree, node, right, path, node->items_nr);
	} else if (r_rem + l_rem >= node->items_nr) {
		uint16_t mv_r = node->items_nr - l_rem;
		move_items_left_right(tree, node, left, l_rem, right, mv_r, path);
	}
}

/**
 * balance @node with @right, by taking elements from @right
 * @node is the last node in path
 */
static void
balance_right(vbpt_tree_t *tree,
              vbpt_node_t *node, vbpt_node_t *right, vbpt_path_t *path)
{
	assert(right->items_nr > 1);
	assert(node = path->nodes[path->height-1]);
	vbpt_node_t *pnode = path->nodes[path->height-2];
	uint16_t pslot     = path->slots[path->height-2];
	if (right->n_hdr.ver != tree->ver) {
		right = cow_node(tree, pnode, pslot+1);
	}
	uint16_t mv_items = right->items_nr / 2;
	move_items_from_right(tree, node, right, path, mv_items);
}

/**
 * balance @node with @left, by taking elements from @left
 * @node is the last node in path
 */
static void
balance_left(vbpt_tree_t *tree,
             vbpt_node_t *node, vbpt_node_t *left, vbpt_path_t *path)
{
	assert(left->items_nr > 1);
	assert(node = path->nodes[path->height-1]);
	vbpt_node_t *pnode = path->nodes[path->height-2];
	uint16_t pslot     = path->slots[path->height-2];
	if (left->n_hdr.ver != tree->ver) {
		left = cow_node(tree, pnode, pslot-1);
	}
	uint16_t mv_items =  left->items_nr / 2;
	move_items_from_left(tree, node, left, path, mv_items);
}


/**
 * try balance around last node in the path -- i.e., pnode
 * input invariant: nodes in the path above are balanced
 * output invariant: node in path is not left with a single item
 */
static void
try_balance_level(vbpt_tree_t *tree, vbpt_path_t *path)
{
	// we can't balance around root -- it has no siblings
	if (path->height == 1)
		return;

	vbpt_node_t *node  = path->nodes[path->height-1];
	if (!node_imba(node)) // continue only if node is imbalanced
		return;

	// get siblings (or NULL)
	vbpt_node_t *left = get_left_sibling(node, path);
	vbpt_node_t *right = get_right_sibling(node, path);
	// try to balance node
	assert(vbpt_tree_ver(tree) == node->n_hdr.ver);
	try_balance_node_nocow(tree, node, left, right, path);

	// reload node
	node = path->nodes[path->height -1];
	// make sure that node is not left with one item
	if (node->items_nr == 1) {
		// reload siblings
		left = get_left_sibling(node, path);
		right = get_right_sibling(node, path);
		// since our parent is balanced, we can expect that it won't
		// have only a single pointer -- i.e., there should be either
		// a left or right node
		if (right != NULL) {
			balance_right(tree, node, right, path);
		} else if (left != NULL) {
			balance_left(tree, node, left, path);
		} else {
			assert(false);
		}
	}
}


static inline bool
node_full(vbpt_node_t *node)
{
	return (node->items_nr == node->items_total);
}

/**
 * add a new root
 *  new root will have a single key, pointing to the current root
 *  path will be updated accordingly
 */
static void
add_new_root(vbpt_tree_t *tree, vbpt_path_t *path)
{
	vbpt_node_t *old_root = tree->root;
	// create a new root with a single key, the maximum (i.e., last) key of
	// current root
	vbpt_node_t *root = vbpt_node_alloc(VBPT_NODE_SIZE, tree->ver);
	uint64_t key_max = old_root->kvp[old_root->items_nr - 1].key;
	root->kvp[0].key = key_max;
	root->kvp[0].val = &old_root->n_hdr; // we already hold a reference
	root->items_nr = 1;
	tree->root = root;
	tree->height++;
	// update path
	assert(path->height == 1);
	assert(path->nodes[0] == old_root);
	path->nodes[1] = path->nodes[0];
	path->slots[1] = path->slots[0];
	path->nodes[0] = root;
	path->slots[0] = 0;
	path->height++;
}

/**
 * split a node
 *  input invariant: parent node has free slots
 *  output invariant: path should be set correctly
 */
static void
split_node(vbpt_tree_t *tree, vbpt_path_t *path)
{
	ver_t *ver = vbpt_tree_ver(tree);

	#if 0
	printf("***** BEFORE SPLIT\n");
	vbpt_path_print(path);
	vbpt_node_print(tree->root, 1, false);
	#endif

	if (path->height == 1) {
		add_new_root(tree, path);
	}

	vbpt_node_t *node = path->nodes[path->height - 1];
	assert(ver_eq(ver, node->n_hdr.ver));
	uint16_t node_slot = path->slots[path->height -1];

	vbpt_node_t *parent = path->nodes[path->height - 2];
	assert(ver_eq(ver, parent->n_hdr.ver));
	uint16_t parent_slot = path->slots[path->height - 2];

	vbpt_node_t *new = vbpt_node_alloc(VBPT_NODE_SIZE, ver);
	uint16_t mid = (node->items_nr + 1) / 2;

	/* no need to update references, just memcpy */
	uint16_t new_items_nr = node->items_nr - mid;
	memcpy(new->kvp, node->kvp + mid, new_items_nr*sizeof(vbpt_kvp_t));
	new->items_nr = new_items_nr;

	node->items_nr -= new_items_nr;
	parent->kvp[parent_slot].key = node->kvp[node->items_nr -1].key;
	assert(node->items_nr == mid);

	vbpt_hdr_t *old;
	old = insert_ptr(parent, parent_slot+1, new->kvp[new->items_nr - 1].key, &new->n_hdr);
	if (old != NULL) {
		fprintf(stderr, "got an old pointer: %p\n", old);
		if (old->type == VBPT_NODE)
			vbpt_node_print(hdr2node(old), 0, false);
		else
			vbpt_leaf_print(hdr2leaf(old), 0);

		assert(false);
	}

	if (node_slot >= mid) {
		path->nodes[path->height - 1] = new;             // node
		path->slots[path->height - 1] = node_slot - mid; // node slot
		path->slots[path->height - 2] = parent_slot + 1; // parent slot
	}

	#if 0
	printf("***** AFTER SPLIT\n");
	vbpt_path_print(path);
	vbpt_node_print(tree->root, 1, false);
	#endif
}

static inline vbpt_hdr_t *
points_to(vbpt_node_t *node, uint16_t slot)
{
	return node->kvp[slot].val;
}


static inline vbpt_kvp_t *
last_item(vbpt_node_t *node)
{
	return node->kvp + (node->items_nr - 1);
}

/**
 * split a node, and setup the parent key accordingly:
 *  this is called when by vbpt_search() with op > 1 and last node in path is
 *  full
 */
static void
search_split_node(vbpt_tree_t *tree, vbpt_path_t *path, uint64_t key)
{
	split_node(tree, path);
	uint16_t lvl = path->height - 1;
	assert(lvl > 0);
	uint16_t slot = path->slots[lvl];
	vbpt_node_t *node = path->nodes[lvl];
	assert(slot <= node->items_nr);
	// key is going to be added after the last element.
	// In that case the key of the parent node might need to be updated to
	// the smaller value
	if (slot == node->items_nr) {
		vbpt_node_t *parent_node = path->nodes[lvl-1];
		uint16_t parent_slot = path->slots[lvl-1];
		if (parent_node->kvp[parent_slot].key <= key) {
			parent_node->kvp[parent_slot].key = key;
		}
	}
}

// check if we can decrease the height of the tree,
// return 1 if we decreased height
static inline int
try_decrease_height(vbpt_tree_t *tree, vbpt_path_t *path)
{
	if (path->height > 1)     // if we are at the root, bail out
		return 0;

	vbpt_node_t *root = path->nodes[0];
	assert(root == tree->root);

	if (root->items_nr != 1)    // root should have only one item
		return 0;

	vbpt_hdr_t *hdr_next = root->kvp[0].val;
	if (!vbpt_isnode(hdr_next)) // root's item should point to a node
		return 0;

	assert(tree->ver == tree->root->n_hdr.ver); // now COW needed
	assert(tree->ver == hdr_next->ver);         // now COW needed

	vbpt_node_t *next = hdr2node(hdr_next);
	tree->root = next;

	root->items_nr = 0;
	vbpt_hdr_putref(&root->n_hdr);

	tree->height--;
	return 1;
}

static inline bool
cow_needed(vbpt_tree_t *tree, ver_t *ver, int op)
{
	ver_t *tver = tree->ver;
	if (op == 0 || ver_eq(tver, ver))
		return false;

	// @ver must be a child of @tver
	if (!ver_leq(ver, tver)) {
		fprintf(stderr, "Error version:%p not a child of %p\n",
		                 tver, ver);
		assert(false);
	}

	return true;
}

/**
 * create a chain of nodes, return the head, and add @last_hdr to the last
 * levels. All nodes will be inserted at the first slot using @key
 *
 * Reference of @last_hdr will not be increased
 */
vbpt_node_t *
vbpt_node_chain(vbpt_tree_t *tree, uint16_t levels, uint64_t key,
                vbpt_hdr_t *last_hdr)
{
	assert(levels > 0);
	vbpt_node_t *head, *tail;
	head = tail = vbpt_node_alloc(VBPT_NODE_SIZE, tree->ver);
	for  (uint16_t i=0; i<levels - 1; i++) {
		vbpt_node_t *n = vbpt_node_alloc(VBPT_NODE_SIZE, tree->ver);
		insert_ptr_empty(tail, 0, key, &n->n_hdr);
		tail = n;
	}
	insert_ptr_empty(tail, 0, key, last_hdr);

	return head;
}

/* build a chain of nodes */
static void
build_node_chain(vbpt_tree_t *tree, vbpt_path_t *path, uint64_t key)
{
	assert(tree->height > path->height);
	uint16_t levels = tree->height - path->height;

	vbpt_node_t *prev = path->nodes[path->height - 1];
	uint16_t prev_slot = path->slots[path->height - 1];
	for (uint16_t i=0; i<levels; i++) {
		vbpt_node_t *n = vbpt_node_alloc(VBPT_NODE_SIZE, tree->ver);
		vbpt_hdr_t *ret = insert_ptr(prev, prev_slot, key, &n->n_hdr);
		assert(ret == NULL);
		path->nodes[path->height + i] = prev      = n;
		path->slots[path->height + i] = prev_slot = 0;
	}
	path->height = tree->height;
}


/**
* find a leaf to a tree.
*  - @path contains the resulting path from the root
*  - op: 0 if just reading, <0 if removing, >0 if inserting
*/
static void
vbpt_search(vbpt_tree_t *tree, uint64_t key, int op,
            vbpt_path_t *path)
{
	vbpt_node_t *node = tree->root;
	if (cow_needed(tree, node->n_hdr.ver, op))
		node = cow_root(tree);

	for (uint16_t lvl = path->height = 0; ; ) {
		assert(lvl < VBPT_MAX_LEVEL);
		uint16_t slot = find_slot(node, key);
		path->nodes[lvl] = node;
		path->slots[lvl] = slot;
		path->height = lvl + 1;

		assert(node->items_nr > 0);

		if (op < 0) { // deletion
			if (try_decrease_height(tree, path) == 1) {
				assert(lvl == 0);
				node = tree->root;
				continue;
			}
			try_balance_level(tree, path);
			node = path->nodes[lvl];
			slot = path->slots[lvl];
		}

		if (op > 0 && node_full(node)) { // insertion
			search_split_node(tree, path, key);
			// update local variables
			lvl = path->height - 1;
			node = path->nodes[lvl];
			slot = path->slots[lvl];
		}

		// special case: the slot is after the last item
		assert(slot <= node->items_nr);
		if (slot == node->items_nr) {

			//  if op is not an insert, we now that the key does not
			//  exist, so we can just bail out
			if (op <= 0)
				break;

			// if this is an insertion, and this is the last level,
			// we can just bail out
			assert(node->items_nr > 0);
			vbpt_hdr_t *l = node->kvp[slot-1].val;
			if (l->type == VBPT_LEAF)
				break;

			#if 1
			// this is an optimization that tries to maintain as
			// much as possible of the old tree structure. If we
			// need to add a new element after @l, and @l is of an
			// older version, we leave @l as it is, and crate a new
			// node with a single element. Balancing is not so good,
			// but the old structure is maintained.
			// Note that this can be disabled
			if (l->ver != tree->ver) {
				build_node_chain(tree, path, key);
				break;
			}
			#endif

			// the op is insertion and there we point to a node:
			// update the rightmost element to be the key we will
			// insert
			uint16_t last_idx = node->items_nr - 1;
			assert(node->kvp[last_idx].key < key);
			node->kvp[last_idx].key = key;
			slot = path->slots[lvl] = slot - 1;

		}
		assert(slot < node->items_nr);

		vbpt_hdr_t *hdr_next = node->kvp[slot].val;
		if (hdr_next->type == VBPT_LEAF) {
			assert(lvl + 1 == tree->height);
			assert(path->height == tree->height);
			break;
		}

		vbpt_node_t *node_next;
		if (!cow_needed(tree, hdr_next->ver, op)) {
			node_next = hdr2node(hdr_next);
		} else {
			node_next = cow_node(tree, node, slot);
		}
		assert(!cow_needed(tree, node_next->n_hdr.ver, op));
		node = node_next;
		lvl++;
	}
}

static void
make_new_root(vbpt_tree_t *tree, uint64_t key, vbpt_leaf_t *data)
{
	assert(tree->height == 0);
	tree->root = vbpt_node_alloc(VBPT_NODE_SIZE, tree->ver);
	tree->root->kvp[0].key = key;
	tree->root->kvp[0].val = &data->l_hdr;
	tree->root->items_nr++;
	tree->height = 1;
}

/**
 * insert a leaf to the tree
 *  If a leaf already exists for @key:
 *    - it will be placed in @old_data if @old_data is not NULL
 *    - it will be decrefed if @old_data is NULL
 * @data's refcount will not be increased
 */
void
vbpt_insert(vbpt_tree_t *tree, uint64_t key, vbpt_leaf_t *data, vbpt_leaf_t **old_data)
{
	if (tree->root == NULL) {
		make_new_root(tree, key, data);
		if (old_data)
			*old_data = NULL;
		return;
	}

	vbpt_path_t path[VBPT_MAX_LEVEL];
	vbpt_search(tree, key, 1, path);

	assert(path->height > 0);
	vbpt_node_t *last = path->nodes[path->height-1];
	uint16_t last_slot = path->slots[path->height-1];
	vbpt_hdr_t *old = insert_ptr(last, last_slot, key, &data->l_hdr);

	if (old_data)
		*old_data = hdr2leaf(old);
	else if (old != NULL)
		vbpt_hdr_putref(old);
}

/* a non-static wrapper for delete_ptr() */
void
vbpt_delete_ptr(vbpt_tree_t *tree, vbpt_path_t *path, vbpt_hdr_t **hdr_ptr)
{
	uint16_t lvl = path->height -1;
	vbpt_node_t *node = path->nodes[lvl];
	uint16_t slot = path->slots[lvl];
	vbpt_hdr_t *hdr_ret = delete_ptr(tree, node, slot, path, lvl);

	if (hdr_ptr)
		*hdr_ptr = hdr_ret;
	else vbpt_hdr_putref(hdr_ret);
}

void
vbpt_delete(vbpt_tree_t *tree, uint64_t key, vbpt_leaf_t **data)
{
	vbpt_leaf_t *ret = NULL;
	if (tree->root == NULL)
		goto end;

	vbpt_path_t path[VBPT_MAX_LEVEL];
	vbpt_search(tree, key, -1, path);
	uint16_t lvl = path->height - 1;
	uint16_t slot = path->slots[lvl];
	vbpt_node_t *node = path->nodes[lvl];
	if (slot < node->items_nr && node->kvp[slot].key == key) {
		vbpt_hdr_t *hdr_ret = delete_ptr(tree, node, slot, path, lvl);
		ret = hdr2leaf(hdr_ret);
	}

end:
	if (data)
		*data = ret;
	else if (ret != NULL)
		vbpt_leaf_putref(ret);
}


#if defined(VBPT_TEST)
#include "vbpt_gv.h"
#include <stdlib.h>

static vbpt_tree_t * __attribute__((unused))
do_insert_test(int rand_seed, uint64_t ins_nr, uint64_t *ins_buff)
{
	if (rand_seed)
		srand(rand_seed);

	//vbpt_gv_reset();

	ver_t *v = ver_create();
	vbpt_tree_t *t = vbpt_tree_alloc(v);
	//vbpt_leaf_t *l1 = vbpt_leaf_alloc(VBPT_LEAF_SIZE, v);
	//vbpt_leaf_t *l2 = vbpt_leaf_alloc(VBPT_LEAF_SIZE, v);
	//vbpt_insert(t, 42, l1, NULL);
	//vbpt_insert(t, 100, l2, NULL);
	for (uint64_t i=0; i < ins_nr; i++) {
		uint64_t k = (rand_seed) ? rand() % 1024UL : i;
		vbpt_leaf_t *l = vbpt_leaf_alloc(VBPT_LEAF_SIZE, v);
		printf("INSERTING %lu key=%lu [leaf:%p]\n", i, k, l);
		if (ins_buff)
			ins_buff[i] = k;
		vbpt_insert(t, k, l, NULL);
		//printf("AFTER insert %lu key=%lu\n", i, k);
		//vbpt_tree_print(t, true);
	}
	printf("root->items_total=%d\n", t->root->items_total);
	printf("root->items_nr=%d\n", t->root->items_nr);

	//vbpt_gv_add_node(t->root);
	//vbpt_gv_write("test.dot");
	return t;
}

static vbpt_tree_t * __attribute__ ((unused))
do_delete_test(int insert_seed, int delete_seed, uint64_t nr)
{
	uint64_t ins[nr];
	vbpt_tree_t *t = do_insert_test(insert_seed, nr, ins);

	if (delete_seed)
		srand(delete_seed);

	//vbpt_tree_print(t, true);
	for (uint64_t i=0; i < nr; i++) {
		uint64_t idx;
		do {
			idx = (delete_seed) ? rand() % nr : i;
		} while (ins[idx] == ~0UL);
		uint64_t k = ins[idx];
		ins[idx] = ~0UL;
		printf("DELETING %lu key=%lu\n", i, k);
		vbpt_delete(t, k, NULL);
		//printf("AFTER delete %lu key=%lu\n", i, k);
		//vbpt_tree_print(t, true);
	}
	return t;
}

static void __attribute__((unused))
insert_test(void)
{
	vbpt_tree_t  *t;
	for (unsigned i=0; i<=666; i++) {
		fprintf(stderr, "INSERTION test i=%u\n", i);
		printf("DOING test i=%u\n", i);
		t = do_insert_test(i, 1024, NULL);
		vbpt_tree_dealloc(t);
	}
}

static void __attribute__((unused))
delete_test(void)
{
	for (unsigned i=0; i<=10; i++)
		for (unsigned j=0; j<=666; j++) {
			//fprintf(stderr, "DELETION test=(%u,%u)\n", i, j);
			vbpt_tree_t *t = do_delete_test(i, j, 128);
			vbpt_tree_dealloc(t);
		}
}

static void __attribute__((unused))
do_mv_ins_test(int ins1_seed, int ins2_seed, uint64_t nr1, uint64_t nr2,
               vbpt_tree_t **t0_ret, vbpt_tree_t **t_ret)
{
	uint64_t ins[nr1];
	vbpt_tree_t *t0 = do_insert_test(ins1_seed, nr1, ins);
	vbpt_tree_t *t = vbpt_tree_branch(t0);
	ver_t *v = vbpt_tree_ver(t);

	if (ins2_seed)
		srand(ins2_seed);

	for (uint64_t i=nr1; i < nr1 + nr2; i++) {
		uint64_t k = (ins2_seed) ? rand() % 1024UL : i;
		vbpt_leaf_t *l = vbpt_leaf_alloc(VBPT_LEAF_SIZE, v);
		printf("INSERTING %lu key=%lu [leaf:%p]\n", i, k, l);
		vbpt_insert(t, k, l, NULL);
		//printf("AFTER insert %lu key=%lu\n", i, k);
		//vbpt_tree_print(t, true);
	}


	if (t0_ret)
		*t0_ret = t0;
	if (t_ret)
		*t_ret = t;
}

static void __attribute__((unused))
mv_ins_test(void)
{

	vbpt_tree_t *t0, *t;
	for (unsigned i=0; i<=10; i++)
		for (unsigned j=0; j<=666; j++) {
			//fprintf(stderr, "DELETION test=(%u,%u)\n", i, j);
			do_mv_ins_test(i, j, 128, 64, &t0, &t);
			vbpt_tree_dealloc(t);
			vbpt_tree_dealloc(t0);
		}
}

static void __attribute__((unused))
do_mv_insdel_test(int ins_seed, int del_seed, uint64_t nr1, uint64_t nr2,
                  vbpt_tree_t **t0_ret, vbpt_tree_t **t_ret)
{
	uint64_t ins[nr1];
	vbpt_tree_t *t0 = do_insert_test(ins_seed, nr1, ins);
	vbpt_tree_t *t = vbpt_tree_branch(t0);

	if (del_seed)
		srand(del_seed);

	assert(nr1>=nr2);
	for (uint64_t i=0; i < nr2; i++) {
		uint64_t idx;
		do {
			idx = (del_seed) ? rand() % nr1 : i;
		} while (ins[idx] == ~0UL);
		uint64_t k = ins[idx];
		ins[idx] = ~0UL;
		printf("DELETING %lu key=%lu\n", i, k);
		vbpt_delete(t, k, NULL);
		//printf("AFTER insert %lu key=%lu\n", i, k);
		//vbpt_tree_print(t, true);
	}


	if (t0_ret)
		*t0_ret = t0;
	if (t_ret)
		*t_ret = t;
}

static void __attribute__((unused))
mv_insdel_test(void)
{

	vbpt_tree_t *t0, *t;
	for (unsigned i=0; i<=10; i++)
		for (unsigned j=0; j<=666; j++) {
			fprintf(stderr, "MV_INSDEL test=(%u,%u)\n", i, j);
			do_mv_insdel_test(i, j, 128, 64, &t0, &t);
			vbpt_tree_dealloc(t);
			vbpt_tree_dealloc(t0);
		}
}

#define UNUSED __attribute__((unused))
int main(int UNUSED argc, const char UNUSED *argv[])
{

	#if 0
	vbpt_tree_t *t_orig = do_insert_test(0, 16, NULL);

	vbpt_tree_t *t = vbpt_tree_branch(t_orig);
	vbpt_leaf_t *l3 = vbpt_leaf_alloc(VBPT_LEAF_SIZE, t->ver);
	printf("INSERTING leaf %p\n", l3);
	vbpt_insert(t, 16, l3, NULL);
	vbpt_tree_print(t, true);
	vbpt_delete(t, 4, NULL);
	vbpt_delete(t, 3, NULL);
	vbpt_tree_print(t, false);
	vbpt_delete(t, 5, NULL);
	//vbpt_tree_print(t_orig, true);
	vbpt_tree_print(t, false);
	#endif

	#if 0
	vbpt_tree_t *t0, *t;
	do_mv_insdel_test(0, 0, 12, 3, &t0, &t);
	vbpt_gv_reset();
	vbpt_gv_add_node(t0->root);
	vbpt_gv_add_node(t->root);
	vbpt_gv_write("mv_ins.dot");
	#endif

	//insert_test();
	//delete_test();
	//mv_ins_test();
	//mv_insdel_test();

	return 0;
}
#endif
