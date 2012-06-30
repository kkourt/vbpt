#include <stdbool.h>
#include <inttypes.h> /* uintptr_t */

#include "vbpt_log.h"

#include "phash.h"
#include "misc.h"

vbpt_log_t *
vbpt_log_alloc(void)
{
	vbpt_log_t *ret = xmalloc(sizeof(vbpt_log_t));
	ret->state = VBPT_LOG_STARTED;
	pset_init(&ret->rd_set, 8);
	phash_init(&ret->wr_set, 8);
	return ret;
}

/**
 * log a write to the tree -- for deletions leaf=NULL
 */
void
vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf)
{
	assert(log->state == VBPT_LOG_STARTED);
	phash_insert(&log->wr_set, key, (uintptr_t)leaf);
}

bool
vbpt_log_wr_check_key(vbpt_log_t *log, uint64_t key)
{
	ul_t dummy_val;
	return phash_lookup(&log->wr_set, key, &dummy_val);
}

void
vbpt_log_read(vbpt_log_t *log, uint64_t key)
{
	assert(log->state == VBPT_LOG_STARTED);
	if (!vbpt_log_wr_check_key(log, key))
		pset_insert(&log->rd_set, key);
}

void
vbpt_log_finalize(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_STARTED);
	log->state = VBPT_LOG_FINALIZED;
}

// read set (rs) checks
bool
vbpt_log_rs_check_key(vbpt_log_t *log, uint64_t key)
{
	assert(log->state == VBPT_LOG_FINALIZED);
	return pset_lookup(&log->rd_set, key);
}

bool
vbpt_log_rs_check_range(vbpt_log_t *log, uint64_t key0, uint64_t len)
{
	for (uint64_t i=0; i < len; i++)
		if (!pset_lookup(&log->rd_set, key0 + i))
			return false;
	return true;
}

bool
vbpt_log_conflict(vbpt_log_t *log1_rd, vbpt_log_t *log2_wr)
{
	if (vbpt_log_wr_size(log2_wr) == 0 || vbpt_log_rd_size(log1_rd) == 0)
		return false;

	phash_t *rd_set = &log1_rd->rd_set;
	pset_iter_t pi;
	pset_iter_init(rd_set, &pi);
	while (true) {
		ul_t key;
		if (pset_iterate(rd_set, &pi, &key)) {
			if (!vbpt_log_wr_check_key(log2_wr, key))
				return true;
		} else break;
	}
	return false;
}

void
vbpt_log_replay(vbpt_txtree_t *txt, vbpt_log_t *log)
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
				vbpt_txtree_insert(txt, key, leaf, NULL);
			else
				vbpt_txtree_delete(txt, key, NULL);
		} else
			break;
	}
}

void
vbpt_log_dealloc(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_FINALIZED);
	pset_tfree(&log->rd_set);
	pset_tfree(&log->wr_set);
	free(log);
}
