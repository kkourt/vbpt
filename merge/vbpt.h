#ifndef VBPT_H_
#define VBPT_H_
#define VBPT_NODE_SIZE 1024
#define VBPT_LEAF_SIZE 1024
#define VBPT_MAX_LEVEL 64

#include <inttypes.h>
#include "refcnt.h"
#include "ver.h"
#include "container_of.h"

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

static inline vbpt_hdr_t *
vbpt_hdr_getref(vbpt_hdr_t *hdr)
{
	refcnt_inc(&hdr->refcnt);
	return hdr;
}

static inline void
vbpt_hdr_putref(vbpt_hdr_t *hdr)
{
	refcnt_dec(&hdr->refcnt);
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
 *
 * [ a  |  b  |  c  |  d   ]
 *   |     |     |     |
 *  <=a   <=b   <=c   <=d
 */
struct vbpt_node {
	vbpt_hdr_t  n_hdr;
	uint16_t    items_nr, items_total;
	vbpt_kvp_t  kvp[];
};
typedef struct vbpt_node vbpt_node_t;

static inline vbpt_node_t *
vbpt_node_getref(vbpt_node_t *node)
{
	refcnt_inc(&node->n_hdr.refcnt);
	return node;
}

static inline void
vbpt_node_putref(vbpt_node_t *node)
{
	refcnt_dec(&node->n_hdr.refcnt);
}

struct vbpt_leaf {
	struct vbpt_hdr l_hdr;
	size_t len, total_len;
	char data[];
};
typedef struct vbpt_leaf vbpt_leaf_t;

static inline vbpt_leaf_t *
vbpt_leaf_getref(vbpt_leaf_t *leaf)
{
	refcnt_inc(&leaf->l_hdr.refcnt);
	return leaf;
}

static inline void
vbpt_leaf_putref(vbpt_leaf_t *leaf)
{
	refcnt_dec(&leaf->l_hdr.refcnt);
}

struct vbpt_tree {
	vbpt_node_t *root; // holds a reference (if not NULL)
	ver_t *ver;        // holds a reference
	uint16_t height;
};
typedef struct vbpt_tree vbpt_tree_t;

static inline ver_t *
vbpt_tree_ver(vbpt_tree_t *tree)
{
	return tree->ver;
}


/* root is nodes[0], leaf is nodes[height-1].kvp[slots[height-1]]
 * Path does not hold references of nodes
 */
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

void
vbpt_node_print(vbpt_node_t *node, int indent);

#endif /* VBPT_H_ */
