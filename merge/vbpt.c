/*
 * versioned b+(plus) tree
 */

#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>

#include "container_of.h"

#include "misc.h"
#include "ver.h"
#include "refcnt.h"

#define VBPT_NODE_SIZE 1024
#define VBPT_LEAF_SIZE 1024
#define VBPT_MAX_LEVEL 64


enum vbpt_type {
	VBPT_INVALID = 0x0,
	VBPT_NODE    = 0x1, /* internal node */
	VBPT_LEAF    = 0x2, /* leaf */
};

/**
 * metadata for each block. In non-volatile case it might be better to keep it
 * seperately than the objects (e.g., on a table indexed by block number). For
 * now we inline it.
 */
struct vbpt_hdr {
	ver_t           *ver;
	refcnt_t        refcnt;
	enum vbpt_type  type;
};
typedef struct vbpt_hdr vbpt_hdr_t;

vbpt_hdr_t *
vbpt_hdr_getref(vbpt_hdr_t *hdr)
{
	refcnt_inc(&hdr->refcnt);
	return hdr;
}

vbpt_hdr_t *
vbpt_hdr_putref(vbpt_hdr_t *hdr)
{
	refcnt_dec(&hdr->refcnt);
	return hdr;
}

struct vbpt_kvp { /* key-address pair */
	uint64_t key;
	vbpt_hdr_t *val;
};
typedef struct vbpt_kvp vbpt_kvp_t;

/**
 * @kvp: array of key-node pairs.
 *  kvp[i].val has the keys k such that kvp[i-1].key < k <= kvp[i].key
 *  (kvp[-1] == -1)
 */
struct vbpt_node {
	vbpt_hdr_t  n_hdr;
	uint16_t    items_nr, items_total;
	vbpt_kvp_t  kvp[];
};
typedef struct vbpt_node vbpt_node_t;

struct vbpt_leaf {
	struct vbpt_hdr l_hdr;
	size_t len, total_len;
	char data[];
};
typedef struct vbpt_leaf vbpt_leaf_t;



/* root is nodes[0], leaf is nodes[height-1].kvp[slots[height-1]] */
struct vbpt_path {
	vbpt_node_t *nodes[VBPT_MAX_LEVEL];
	uint16_t    slots[VBPT_MAX_LEVEL];
	uint16_t    height;
};
typedef struct vbpt_path vbpt_path_t;

static inline vbpt_node_t *
hdr2node(vbpt_hdr_t *hdr)
{
	assert(hdr->type == VBPT_NODE);
	return container_of(hdr, vbpt_node_t, n_hdr);
}

static inline vbpt_leaf_t *
hdr2leaf(vbpt_hdr_t *hdr)
{
	assert(hdr->type == VBPT_LEAF);
	return container_of(hdr, vbpt_leaf_t, l_hdr);
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


static void
insert_key(vbpt_node_t *node, uint64_t key, vbpt_hdr_t *val)
{
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

/**
* find a leaf to a tree.
*  - @path contains the resulting path from the root
*  - if @cow_ver is not NULL, then nodes on the path with different versions
*    will be COWed
*/
static void
find_leaf(vbpt_node_t *root, uint64_t key, vbpt_path_t *path, ver_t *cow_ver)
{
	assert(cow_ver == NULL || root->n_hdr.ver == cow_ver);
	vbpt_node_t *node = root;

	bool do_cow(ver_t *v) {
		bool ret = (cow_ver != NULL) && !ver_eq(cow_ver, v);
		assert(!v || ver_leq(v, cow_ver));
		return ret;
	}

	unsigned i;
	for (i=0; ; i++) {
		assert(i < VBPT_MAX_LEVEL);
		uint16_t slot = find_slot(node, key);
		vbpt_hdr_t *hdr_next = node->kvp[slot].val;
		path->nodes[i] = node;
		path->slots[i] = slot;
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
	path->height = i + 1;
}


static void
vbpt_insert(vbpt_node_t *root, uint64_t key, vbpt_leaf_t *data)
{
}


#if defined(VBPT_TEST)
int main(int argc, const char *argv[])
{
	return 0;
}
#endif
