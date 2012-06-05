/*
 * versioned b+(plus) tree
 */

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>

#include "misc.h"
#include "ver.h"
#include "refcnt.h"
#include "vbpt.h"

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
	refcnt_init(&hdr->refcnt, 1);
	hdr->type = type;
}

/*
 * allocate a new node
 *  Version's refcount will be increased
 */
vbpt_node_t *
vbpt_alloc_node(size_t node_size, ver_t *ver)
{
	assert(node_size > sizeof(vbpt_node_t));
	vbpt_node_t *ret = xmalloc(node_size);
	vbpt_hdr_init(&ret->n_hdr, ver, VBPT_NODE);
	ret->items_nr = 0;
	ret->items_total = (node_size - sizeof(vbpt_node_t)) / sizeof(vbpt_kvp_t);
	return ret;
}

/*
 * allocate a new leaf
 *  Version's refcount will be increased
 */
vbpt_leaf_t *
vbpt_alloc_leaf(size_t leaf_size, ver_t *ver)
{
	assert(leaf_size > sizeof(vbpt_leaf_t));
	vbpt_leaf_t *ret = xmalloc(leaf_size);
	vbpt_hdr_init(&ret->l_hdr, ver, VBPT_LEAF);
	ret->len = 0;
	ret->total_len = (leaf_size - sizeof(vbpt_leaf_t));
	return ret;
}

/* allocate (and initialize) a new tree */
vbpt_tree_t *
vbpt_alloc_tree(ver_t *ver)
{
	vbpt_tree_t *ret = xmalloc(sizeof(vbpt_tree_t));
	ret->ver = ver;
	ret->root = NULL;
	ret->height = 0;
	return ret;
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
cow_node(vbpt_node_t *parent, uint16_t parent_slot)
{
	assert(parent_slot < parent->items_nr);
	vbpt_node_t *old = hdr2node(parent->kvp[parent_slot].val);
	vbpt_node_t *new = vbpt_alloc_node(VBPT_NODE_SIZE, parent->n_hdr.ver);
	for (unsigned i=0; i<old->items_nr; i++) {
		new->kvp[i].key = old->kvp[i].key;
		new->kvp[i].val = vbpt_hdr_getref(old->kvp[i].val);
	}
	new->items_nr = old->items_nr;
	vbpt_hdr_putref(&old->n_hdr);
	return new;
}

/**
 * insert a  pointer to a node
 *  input invariance: enough space exists
 *  this function might end  up shifting keys
 *  val's reference count will be increased
 *
 * If the slot is already occupied with the same key, replace slot and return
 * old value
 */
static vbpt_hdr_t *
insert_ptr(vbpt_node_t *node, int slot, uint64_t key, vbpt_hdr_t *val)
{
	assert(slot < node->items_total);

	vbpt_kvp_t *kvp = node->kvp + slot;
	if (kvp->key == key) {
		vbpt_hdr_t *old = kvp->val;
		kvp->val = vbpt_hdr_getref(val);
		return old;
	}

	if (slot > node->items_nr) {
		assert(false); // note that node->items_nr == slot is OK
	} else if (slot < node->items_nr) {
		// need to shift
		memmove(kvp, kvp+1, node->items_nr - slot);
	}

	kvp->key = key;
	kvp->val = vbpt_hdr_getref(val);
	node->items_nr++;
	return NULL;
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
	vbpt_node_t *root = vbpt_alloc_node(VBPT_NODE_SIZE, tree->ver);
	uint64_t key_max = old_root->kvp[old_root->items_nr - 1].key;
	root->kvp[0].key = key_max;
	root->kvp[0].val = &old_root->n_hdr; // we already hold a reference
	root->items_nr = 1;
	tree->root = vbpt_node_getref(root);
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

	vbpt_node_t *node = path->nodes[path->height - 1];
	assert(ver_eq(ver, node->n_hdr.ver));
	uint16_t node_slot = path->slots[path->height -1];

	if (path->height == 1) {
		add_new_root(tree, path);
	}

	vbpt_node_t *parent = path->nodes[path->height - 2];
	assert(ver_eq(ver, parent->n_hdr.ver));
	uint16_t parent_slot = path->slots[path->height - 2];

	vbpt_node_t *new = vbpt_alloc_node(VBPT_NODE_SIZE, ver);
	uint16_t mid = (node->items_nr + 1) / 2;
	uint16_t new_items_nr = node->items_nr - mid;

	/* no need to update references, just memcpy */
	memcpy(new->kvp, node->kvp + mid, new_items_nr*sizeof(vbpt_kvp_t));
	new->items_nr = new_items_nr;

	node->items_nr -= new_items_nr;
	parent->kvp[parent_slot].key = node->kvp[node->items_nr -1].key;
	assert(node->items_nr == mid);

	vbpt_hdr_t *old;
	old = insert_ptr(parent, parent_slot + 1, new->kvp[new->items_nr - 1].key, &new->n_hdr);
	assert(old == NULL);

	if (node_slot > mid) {
		path->nodes[path->height - 1] = new;             // node
		path->slots[path->height - 1] = node_slot - mid; // node slot
		path->slots[path->height - 2] = parent_slot + 1; // parent slot
	}
}

static bool
points_to_node(vbpt_node_t *node, uint16_t slot)
{
	return node->kvp[slot].val->type == VBPT_NODE;
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
	ver_t *cow_ver = vbpt_tree_ver(tree);

	/* check if cow is needed */
	bool do_cow(ver_t *v) {
		bool ret = (op != 0) && !ver_eq(cow_ver, v);
		assert(ver_leq(v, cow_ver));
		return ret;
	}

	for (unsigned i=0; ; i++) {
		assert(i < VBPT_MAX_LEVEL);
		uint16_t slot = find_slot(node, key);

		path->nodes[i] = node;
		path->slots[i] = slot;
		path->height = i + 1;

		// the node should be placed after the end of the current
		if (slot >= node->items_nr) {
			if (op > 0 && points_to_node(node, slot-1)) {
				node->kvp[slot-1].key = key;
				path->slots[i] = slot = slot - 1;
			}
		}

		if (op > 0 && node_full(node)) {
			split_node(tree, path);
		}

		if (slot >= node->items_nr)
			break;

		vbpt_hdr_t *hdr_next = node->kvp[slot].val;
		if (hdr_next->type == VBPT_LEAF) {
			assert(i + 1 == tree->height);
			break;
		}

		vbpt_node_t *node_next;

		if (do_cow(hdr_next->ver))
			node_next = cow_node(node, slot);
		else
			node_next = hdr2node(hdr_next);

		assert(cow_ver == NULL || node->n_hdr.ver == cow_ver);
		node = node_next;
	}
}


/**
 * insert a leaf to the tree
 *  If a leaf already exists for @key:
 *    - it will be placed in @old_data if @old_data is not NULL
 *    - it will be decrefed if @old_data is NULL
 * @data's refcount will be increased
 */
void
vbpt_insert(vbpt_tree_t *tree, uint64_t key, vbpt_leaf_t *data, vbpt_leaf_t **old_data)
{
	if (tree->root == NULL) {
		assert(tree->height == 0);
		tree->root = vbpt_alloc_node(VBPT_NODE_SIZE, tree->ver);
		tree->root->kvp[0].key = key;
		tree->root->kvp[0].val = vbpt_hdr_getref(&data->l_hdr);
		tree->root->items_nr++;
		if (old_data)
			*old_data = NULL;
		tree->height = 1;
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

static void
vbpt_leaf_print(vbpt_leaf_t *leaf, int indent)
{
	printf("%*s" "[leaf=%p ->len=%lu ->total_len=%lu]\n",
	        indent, " ",
		leaf, leaf->len, leaf->total_len);
}


void
vbpt_node_print(vbpt_node_t *node, int indent)
{
	printf("\n%*s" "[node=%p ->items_nr=%u ->items_total=%u]\n",
	        indent, " ",
		node, node->items_nr, node->items_total);
	for (unsigned i=0; i < node->items_nr; i++) {
		vbpt_kvp_t *kvp = node->kvp + i;
		printf("%*s" "key=%lu ", indent, " ", kvp->key);
		if (kvp->val->type == VBPT_NODE)
			vbpt_node_print(hdr2node(kvp->val), indent+4);
		else
			vbpt_leaf_print(hdr2leaf(kvp->val), indent+4);
	}
}


#if defined(VBPT_TEST)
#include "vbpt_gv.h"
int main(int argc, const char *argv[])
{
	ver_t *v = ver_create();
	vbpt_tree_t *t = vbpt_alloc_tree(v);
	//vbpt_leaf_t *l1 = vbpt_alloc_leaf(VBPT_LEAF_SIZE, v);
	//vbpt_leaf_t *l2 = vbpt_alloc_leaf(VBPT_LEAF_SIZE, v);
	//vbpt_insert(t, 42, l1, NULL);
	//vbpt_insert(t, 100, l2, NULL);
	for (uint64_t i=0; i < 64; i++) {
		vbpt_leaf_t *l = vbpt_alloc_leaf(VBPT_LEAF_SIZE, v);
		printf("insert=%lu\n", i);
		vbpt_insert(t, i, l, NULL);
		vbpt_node_print(t->root, 0);
	}
	printf("root->items_total=%d\n", t->root->items_total);
	printf("root->items_nr=%d\n", t->root->items_nr);

	vbpt_gv_add_node(t->root);
	vbpt_gv_write("test.dot");
	return 0;
}
#endif
