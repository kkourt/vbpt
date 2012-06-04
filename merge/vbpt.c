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

/* find slot for key in node */
static uint16_t
find_slot(vbpt_node_t *node, uint64_t key)
{
	vbpt_kvp_t *kvp = node->kvp;
	for (uint16_t i=0; i<node->items_nr; i++, kvp++)
		if (kvp->key <= key)
			return i;
	return node->items_nr - 1;
}

static vbpt_hdr_t *
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



/**
* cow node at @parent_slot in @parent
* The new node will be placed in @parent_slot in @parent and it will have the
* version of the parent
* The old node will be decrefed
* nodes that were referenced in the old node will be increfed
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
}

static void
insert_ptr(vbpt_node_t *node, int slot, uint64_t key, vbpt_hdr_t *val)
{
	assert(slot < node->items_total);

	vbpt_kvp_t *kvp = node->kvp + slot;

	if (slot > node->items_nr) {
		assert(false);
	} else if (slot < node->items_nr) {
		// need to shift
		memmove(kvp, kvp+1, node->items_nr - slot);
	}

	kvp->key = key;
	kvp->val = val;
	node->items_nr++;
}

static inline bool
node_full(vbpt_node_t *node)
{
	return (node->items_nr == node->items_total);
}

/**
 * split a node
 *  invariant: parent node has free slots
 */
static void
split_node(vbpt_tree_t *tree, vbpt_path_t *path)
{
	if (path->height == 1) {
		// split root
	}

	ver_t *ver = vbpt_tree_ver(tree);
	vbpt_node_t *node = path->nodes[path->height - 1];
	assert(ver_eq(ver, node->n_hdr.ver));
	vbpt_node_t *parent = path->nodes[path->height - 2];
	assert(ver_eq(ver, parent->n_hdr.ver));
	uint16_t parent_slot = path->slots[path->height - 2];

	vbpt_node_t *new = vbpt_alloc_node(VBPT_NODE_SIZE, ver);
	uint16_t mid = (node->items_nr + 1) / 2;
	uint16_t new_items_nr = node->items_nr - mid;

	memcpy(new->kvp, node->kvp + mid, new_items_nr*sizeof(vbpt_kvp_t));
	new->items_nr = new_items_nr;

	node->items_nr -= new_items_nr;
	assert(node->items_nr == mid);

	insert_ptr(parent, parent_slot + 1, new->kvp[0].key, &new->n_hdr);
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

	unsigned i;
	for (i=0; ; i++) {
		assert(i < VBPT_MAX_LEVEL);
		uint16_t slot = find_slot(node, key);
		path->nodes[i] = node;
		path->slots[i] = slot;
		path->height = i + 1;

		if (op > 0 && node_full(node)) {
			split_node(tree, path);
		}


		vbpt_hdr_t *hdr_next = node->kvp[slot].val;
		if (hdr_next->type == VBPT_LEAF)
			break;
		vbpt_node_t *node_next;

		if (do_cow(hdr_next->ver))
			node_next = cow_node(node, slot);
		else
			node_next = hdr2node(hdr_next);

		assert(cow_ver == NULL || node->n_hdr.ver == cow_ver);
		node = node_next;
	}
}


void
vbpt_insert(vbpt_node_t *root, uint64_t key, vbpt_leaf_t *data, ver_t *cow_ver)
{
	//vbpt_path_t path[VBPT_MAX_LEVEL];
}


#if defined(VBPT_TEST)
int main(int argc, const char *argv[])
{
	return 0;
}
#endif
