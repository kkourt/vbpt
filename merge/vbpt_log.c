#include <stdbool.h>
#include <inttypes.h> /* uintptr_t */

#include "vbpt_log.h"
#include "vbpt_merge.h" // vbpt_range_t

#include "phash.h"
#include "misc.h"

/*
 * TODO: multiple levels of logs
 * TODO: queries on log (e.g., has this range been written) answered based on
 * the internal implementation.
 */

/*
 * pset wrappers.
 */

static bool
pset_key_exists(pset_t *pset, uint64_t key)
{
	return pset_lookup(pset, key);
}

static bool
pset_range_exists(pset_t *pset, uint64_t key, uint64_t len)
{
	// this is a questionable optimization, since at worse case scenario we
	// need to iterate over all buckets in the hash.
	uint64_t set_elems = pset_elements(pset);
	if (set_elems > len) {
		for (uint64_t i=0; i < len; i++)
			if (pset_lookup(pset, key + i))
				return true;
	} else {
		pset_iter_t pi;
		pset_iter_init(pset, &pi);
		for (;;) {
			uint64_t k;
			int ret = pset_iterate(pset, &pi, &k);
			if (!ret) {
				break;
			}
			if (k >= key && k - key < len)
				return true;
		}
	}
	return false;
}

void
vbpt_log_init(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_UNINITIALIZED);
	log->state = VBPT_LOG_STARTED;
	pset_init(&log->rd_set, 8);
	pset_init(&log->rm_set, 8);
	phash_init(&log->wr_set, 8);
}

vbpt_log_t *
vbpt_log_alloc(void)
{
	vbpt_log_t *ret = xmalloc(sizeof(vbpt_log_t));
	ret->state = VBPT_LOG_UNINITIALIZED;
	vbpt_log_init(ret);
	return ret;
}

void
vbpt_log_finalize(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_STARTED);
	log->state = VBPT_LOG_FINALIZED;
}

void
vbpt_log_destroy(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_FINALIZED);
	pset_tfree(&log->rd_set);
	pset_tfree(&log->rm_set);
	phash_tfree(&log->wr_set);
}

void
vbpt_log_dealloc(vbpt_log_t *log)
{
	vbpt_log_destroy(log);
	free(log);
}



/*
 * log actions
 */
void
vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf)
{
	assert(log->state == VBPT_LOG_STARTED);
	phash_insert(&log->wr_set, key, (uintptr_t)leaf);
}

/**
 * log a read action
 * We first check the write set and delete set for @key, since we are interested
 * in detecting conflicts when reading from old state.
 */
void
vbpt_log_read(vbpt_log_t *log, uint64_t key)
{
	ul_t dummy_val;
	assert(log->state == VBPT_LOG_STARTED);
	if (phash_lookup(&log->wr_set, key, &dummy_val))
		return;
	if (pset_key_exists(&log->rm_set, key))
		return;
	pset_insert(&log->rd_set, key);
}

void
vbpt_log_delete(vbpt_log_t *log, uint64_t key)
{
	assert(log->state == VBPT_LOG_STARTED);
	pset_insert(&log->rm_set, key);
}

/*
 * perform queries on logs
 */

static inline size_t
vbpt_log_rd_size(vbpt_log_t *log)
{
	return pset_elements(&log->rd_set);
}

static inline size_t
vbpt_log_wr_size(vbpt_log_t *log)
{
	return phash_elements(&log->wr_set);
}

bool
vbpt_log_ws_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		ul_t dummy_val;
		if (phash_lookup(&log->wr_set, key, &dummy_val))
			return true;
		log = vbpt_log_parent(log);
		assert(log != NULL);
	}
	return false;
}


// read set (rs) checks
bool
vbpt_log_rs_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		if (pset_key_exists(&log->rd_set, key))
			return true;
		log = vbpt_log_parent(log);
		assert(log != NULL);
	}
	return false;
}

bool
vbpt_log_rs_range_exists(vbpt_log_t *log, vbpt_range_t *r, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		if (pset_range_exists(&log->rd_set, r->key, r->len))
			return true;
		log = vbpt_log_parent(log);
		assert(log != NULL);
	}
	return false;
}

// delete set (ds) checks
bool
vbpt_log_ds_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		if (pset_key_exists(&log->rm_set, key))
			return true;
		log = vbpt_log_parent(log);
		assert(log != NULL);
	}
	return false;
}

bool
vbpt_log_ds_range_exists(vbpt_log_t *log, vbpt_range_t *r, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		if (pset_range_exists(&log->rm_set, r->key, r->len))
			return true;
		log = vbpt_log_parent(log);
		assert(log != NULL);
	}
	return false;
}



/*
 * log replay functions
 */

bool
vbpt_log_conflict(vbpt_log_t *log1_rd, unsigned depth1,
                  vbpt_log_t *log2_wr, unsigned depth2)
{
	if (vbpt_log_wr_size(log2_wr) == 0 || vbpt_log_rd_size(log1_rd) == 0)
		return false;

	for (unsigned i=0; i<depth1; i++) {
		pset_t *rd_set = &log1_rd->rd_set;
		pset_iter_t pi;
		pset_iter_init(rd_set, &pi);
		while (true) {
			ul_t key;
			if (pset_iterate(rd_set, &pi, &key)) {
				if (!vbpt_log_ws_key_exists(log2_wr, key, depth2))
					return true;
			} else break;
		}
		log1_rd = vbpt_log_parent(log1_rd);
		assert(log1_rd != NULL);
	}
	return false;
}

void
vbpt_log_replay__(vbpt_tree_t *tree, vbpt_log_t *log)
{
	if (vbpt_log_wr_size(log) == 0)
		return;

	phash_t *wr_set = &log->wr_set;
	phash_iter_t pi;
	phash_iter_init(wr_set, &pi);
	while (true) {
		ul_t key;
		uintptr_t val;
		if (phash_iterate(wr_set, &pi, &key, &val)) {
			vbpt_leaf_t *leaf = (vbpt_leaf_t *)val;
			assert(leaf == NULL || leaf->l_hdr.type == VBPT_LEAF);
			if (leaf)
				vbpt_logtree_insert(tree, key, leaf, NULL);
			else
				vbpt_logtree_delete(tree, key, NULL);
		} else
			break;
	}
}

void
vbpt_log_replay(vbpt_tree_t *tree, vbpt_log_t *log, unsigned depth)
{
	vbpt_log_t *logs[depth];
	for (unsigned i=0; i<depth; i++) {
		assert(log != NULL);
		logs[i] = log;
		log = vbpt_log_parent(log);
	}

	for (unsigned i = depth-1; i<depth; i--) { // NB: backwards
		vbpt_log_replay__(tree, logs[i]);
	}
}
