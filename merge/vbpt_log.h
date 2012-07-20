#ifndef VBPT_LOG_H__
#define VBPT_LOG_H__

#include <stdbool.h>
#include "vbpt.h" /* vbpt_leaf_t */

#include "vbpt_merge.h" // vbpt_range_t

// interface

typedef struct vbpt_log vbpt_log_t;

/*
 * log operations
 */

void vbpt_log_init(vbpt_log_t *log); // initialize log
vbpt_log_t *vbpt_log_alloc(void);    // alocate and initialize log

// record operations on the tree
void vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf);
void vbpt_log_read(vbpt_log_t *log, uint64_t key);
void vbpt_log_delete(vbpt_log_t *log, uint64_t key);

// finalize the log -- only queries can be performed after that
void vbpt_log_finalize(vbpt_log_t *log);

void vbpt_log_destroy(vbpt_log_t *log); // destroy a log (pairs with _init)
void vbpt_log_dealloc(vbpt_log_t *log); // deallocate a log (pairs with _alloc)

// query the log:
//  to allow for different implementations for the logs, we specify that query
//  functions can return false positives (e.g., so that they can be implemented
//  with bloom filters) but not false negatives. In that sense, a check function
//  that always returns true is correct.

// read set (rs) checks
bool vbpt_log_rs_key_exists(vbpt_log_t *log, uint64_t key);
bool vbpt_log_rs_range_exists(vbpt_log_t *log, vbpt_range_t *r);
// write set (ws) checks
bool vbpt_log_ws_key_exists(vbpt_log_t *log, uint64_t key);
bool vbpt_log_ws_range_exists(vbpt_log_t *log, vbpt_range_t *r);
// delete set (ds) checks
bool vbpt_log_ds_key_exists(vbpt_log_t *log, uint64_t key);
bool vbpt_log_ds_range_exists(vbpt_log_t *log, vbpt_range_t *r);

void vbpt_log_dealloc(vbpt_log_t *log);

/*
 * high-level operations
 */

bool vbpt_log_conflict(vbpt_log_t *log1_rd, vbpt_log_t *log2_wr);

/**
 * txtree: transactional operations on trees
 *  The functions are just wrappers that use "hidden" log on version.
 *  At some point, we might want to move them to a different file, but for now
 *  they are very simple.
 */

static inline vbpt_log_t *
vbpt_txtree_getlog(vbpt_tree_t *t)
{
	return &t->ver->log;
}

/**
 * branch off a new version of a tree from @t and return it
 *   the log is initialized
 */
static inline vbpt_tree_t *
vbpt_txtree_branch(vbpt_tree_t *t)
{
	vbpt_tree_t *ret = vbpt_tree_branch(t);
	vbpt_log_t *log = vbpt_txtree_getlog(ret);
	vbpt_log_init(log);
	return ret;
}

static inline void
vbpt_txtree_insert(vbpt_tree_t *t,  uint64_t k, vbpt_leaf_t *l, vbpt_leaf_t **o)
{
	vbpt_log_t *log = vbpt_txtree_getlog(t);
	if (o)
		vbpt_log_read(log, k);
	vbpt_log_write(log, k, l);
	vbpt_insert(t, k, l, o);
}

static inline void
vbpt_txtree_delete(vbpt_tree_t *t, uint64_t k, vbpt_leaf_t **o)
{
	vbpt_log_t *log = vbpt_txtree_getlog(t);
	if (o)
		vbpt_log_read(log, k);
	vbpt_log_delete(log, k);
	vbpt_delete(t, k, o);
}

void vbpt_log_replay(vbpt_tree_t *txt, vbpt_log_t *log);

#include "vbpt_log_internal.h"

#endif
