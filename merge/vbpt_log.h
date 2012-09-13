#ifndef VBPT_LOG_H__
#define VBPT_LOG_H__

#include <stdbool.h>
#include "vbpt.h" /* vbpt_leaf_t */

#include "vbpt_merge.h" // vbpt_range_t

// interface

#include "vbpt_log_internal.h"

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
bool vbpt_log_rs_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth);
bool vbpt_log_rs_range_exists(vbpt_log_t *log, vbpt_range_t *r, unsigned depth);
// write set (ws) checks
bool vbpt_log_ws_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth);
bool vbpt_log_ws_range_exists(vbpt_log_t *log, vbpt_range_t *r, unsigned depth);
// delete set (ds) checks
bool vbpt_log_ds_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth);
bool vbpt_log_ds_range_exists(vbpt_log_t *log, vbpt_range_t *r, unsigned depth);

/*
 * logtree: operations on trees that are recorded on logs
 *  The functions are just wrappers that use "hidden" log on version.
 */

static inline vbpt_log_t *
vbpt_tree_log(vbpt_tree_t *t)
{
	return &t->ver->v_log;
}

static inline void
vbpt_logtree_log_init(vbpt_tree_t *tree)
{
	vbpt_log_t *log = vbpt_tree_log(tree);
	vbpt_log_init(log);
}

/**
 * branch off a new version of a tree from @t and return it
 *   the log is initialized
 */
static inline vbpt_tree_t *
vbpt_logtree_branch(vbpt_tree_t *t)
{
	vbpt_tree_t *ret = vbpt_tree_branch(t);
	vbpt_logtree_log_init(ret);
	return ret;
}


static inline void
vbpt_logtree_finalize(vbpt_tree_t *tree)
{
	vbpt_log_t *log = vbpt_tree_log(tree);
	vbpt_log_finalize(log);
}

static inline void
vbpt_logtree_destroy(vbpt_tree_t *tree)
{
	vbpt_log_t *log = vbpt_tree_log(tree);
	vbpt_log_destroy(log);
}

static inline void
vbpt_logtree_dealloc(vbpt_tree_t *tree)
{
	vbpt_logtree_destroy(tree);
	vbpt_tree_dealloc(tree);
}


static inline void
vbpt_logtree_insert(vbpt_tree_t *t,  uint64_t k, vbpt_leaf_t *l, vbpt_leaf_t **o)
{
	VBPT_START_TIMER(logtree_insert);
	vbpt_log_t *log = vbpt_tree_log(t);
	if (o)
		vbpt_log_read(log, k);
	vbpt_log_write(log, k, l);
	vbpt_insert(t, k, l, o);
	VBPT_STOP_TIMER(logtree_insert);
}

static inline void
vbpt_logtree_delete(vbpt_tree_t *t, uint64_t k, vbpt_leaf_t **o)
{
	vbpt_log_t *log = vbpt_tree_log(t);
	if (o)
		vbpt_log_read(log, k);
	vbpt_log_delete(log, k);
	vbpt_delete(t, k, o);
}

static inline vbpt_leaf_t *
vbpt_logtree_get(vbpt_tree_t  *t, uint64_t k)
{
	VBPT_START_TIMER(logtree_get);
	vbpt_log_t *log = vbpt_tree_log(t);
	vbpt_log_read(log, k);
	vbpt_leaf_t *ret = vbpt_get(t, k);
	VBPT_STOP_TIMER(logtree_get);
	return ret;
}



/*
 * high-level operations
 */

bool vbpt_log_conflict(vbpt_log_t *log1_rd, unsigned depth1,
                       vbpt_log_t *log2_wr, unsigned depth2);

void vbpt_log_replay(vbpt_tree_t *txt, vbpt_log_t *log, unsigned log_depth);


#endif
