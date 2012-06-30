#ifndef VBPT_LOG_H__
#define VBPT_LOG_H__

#include <stdbool.h>
#include "vbpt.h" /* vbpt_leaf_t */

// interface

struct vbpt_log;
typedef struct vbpt_log vbpt_log_t;

/*
 * log operations
 */

vbpt_log_t *vbpt_log_alloc(void);

void vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf);
void vbpt_log_read(vbpt_log_t *log, uint64_t key);

void vbpt_log_finalize(vbpt_log_t *log);

// read set (rs) checks
bool vbpt_log_rs_check_key(vbpt_log_t *log, uint64_t key);
bool vbpt_log_rs_check_range(vbpt_log_t *log, uint64_t key0, uint64_t len);

void vbpt_log_dealloc(vbpt_log_t *log);

/*
 * high-level operations
 */

bool vbpt_log_conflict(vbpt_log_t *log1_rd, vbpt_log_t *log2_wr);

/*
 * transaction trees (to be)
 *  just a log attached to a tree for now...
 */
struct vbpt_txtree {
	vbpt_log_t     *tx_log;
	vbpt_tree_t    *tx_tree;
};
typedef struct vbpt_txtree vbpt_txtree_t;

static inline vbpt_txtree_t *
vbpt_txtree_alloc(vbpt_tree_t *tree)
{
	vbpt_txtree_t *txt = xmalloc(sizeof(vbpt_txtree_t));
	txt->tx_log = vbpt_log_alloc();
	txt->tx_tree = vbpt_tree_branch(tree);
	return txt;
}

static inline void
vbpt_txtree_insert(vbpt_txtree_t *tx,  uint64_t k, vbpt_leaf_t *l, vbpt_leaf_t **o)
{
	if (o)
		vbpt_log_read(tx->tx_log, k);
	vbpt_log_write(tx->tx_log, k, l);
	vbpt_insert(tx->tx_tree, k, l, o);
}

static inline void
vbpt_txtree_delete(vbpt_txtree_t *tx, uint64_t k, vbpt_leaf_t **o)
{
	if (o)
		vbpt_log_read(tx->tx_log, k);
	vbpt_log_write(tx->tx_log, k, NULL);
	vbpt_delete(tx->tx_tree, k, o);
}

void vbpt_log_replay(vbpt_txtree_t *txt, vbpt_log_t *log);

// implementation details:
//  We are using a hash set for sets. This has two problems:
//   - no way to do efficient range checks
//   - iterating values (e.g., for replay) is O(hash table size)
//
//  Judy1 seems like a good ds for something like this, but we don't use it
//  because it is not trivial to make it persistent. It might make sense to do a
//  Judy1 implementation to measure the performance hit.

#include "phash.h"

enum {
	VBPT_LOG_STARTED = 1,
	VBPT_LOG_FINALIZED,
};

struct vbpt_log {
	unsigned state;
	pset_t   rd_set;
	phash_t  wr_set;
};

static inline size_t
vbpt_log_rd_size(vbpt_log_t *log)
{
	return log->rd_set.used;
}

static inline size_t
vbpt_log_wr_size(vbpt_log_t *log)
{
	return log->wr_set.used;
}

#endif
