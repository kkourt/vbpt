#include <stdbool.h>
#include <inttypes.h> /* uintptr_t */

#include "vbpt_range.h"
#include "vbpt_log.h"
#include "vbpt_merge.h"

#include "misc.h"

#if !defined(VBPT_LOG_RANGE)
#error ""
#endif

void
vbpt_log_init(vbpt_log_t *log)
{
	assert(log->state == VBPT_LOG_UNINITIALIZED);
	log->state = VBPT_LOG_STARTED;
	log->rd_range.len = log->rm_range.len = log->wr_range.len = 0;
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
}

void
vbpt_log_dealloc(vbpt_log_t *log)
{
	free(log);
}


static void
vbpt_range_add(vbpt_range_t *range, uint64_t key)
{
	if (range->len == 0) {
		range->key = key;
		range->len = 1;
		return;
	}

	if (key < range->key) {
		uint64_t d = range->key - key;
		range->len += d;
		range->key = key;
	} else if (key >= range->key + range->len) {
		range->len += key - (range->key + range->len - 1);
	}
}

/*
 * log actions
 */
void
vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf)
{
	assert(log->state == VBPT_LOG_STARTED);
	vbpt_range_add(&log->wr_range, key);
}

void
vbpt_log_read(vbpt_log_t *log, uint64_t key)
{
	assert(log->state == VBPT_LOG_STARTED);
	vbpt_range_add(&log->rd_range, key);
}

void
vbpt_log_delete(vbpt_log_t *log, uint64_t key)
{
	assert(log->state == VBPT_LOG_STARTED);
	vbpt_range_add(&log->rm_range, key);
}

/*
 * perform queries on logs
 */

bool
vbpt_log_ws_key_exists(vbpt_log_t *log, uint64_t key, unsigned depth)
{
	for (unsigned i=0; i<depth; i++) {
		if (vbpt_range_contains(&log->wr_range, key))
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
		if (vbpt_range_contains(&log->rd_range, key))
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
		if (vbpt_range_intersects(&log->rd_range, r))
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
		if (vbpt_range_contains(&log->rm_range, key))
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
		if (vbpt_range_intersects(&log->rm_range, r))
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
	fprintf(stderr, ":%s NOT SUPPORTED\n", __FUNCTION__);
	exit(1);
}

void
vbpt_log_replay(vbpt_tree_t *tree, vbpt_log_t *log, unsigned depth)
{
	fprintf(stderr, ":%s NOT SUPPORTED\n", __FUNCTION__);
	exit(1);
}
