#include <stdbool.h>
#include "vbpt_log.h"

#include "misc.h"

vbpt_log_t *vbpt_log_alloc(void)
{
	vbpt_log_t *ret = xmalloc(sizeof(vbpt_log_t));
	ret->state = VBPT_LOG_STARTED;
	pset_init(&ret->rd_set);
}

void vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf)
{
	assert(ret->state == VBPT_LOG_STARTED);
}

void vbpt_log_read(vbpt_log_t *log, uint64_t key)
{
	assert(ret->state == VBPT_LOG_STARTED);
	pset_insert(&log->rd_set, key);
}

void vbpt_log_finalize(vbpt_log_t *log)
{
	assert(ret->state == VBPT_LOG_STARTED);
}

// read set (rs) checks
bool vbpt_log_rs_check_key(vbpt_log_t *log, uint64_t key)
{
}

bool vbpt_log_rs_check_range(vbpt_log_t *log, uint64_t key0, uint64_t len)
{
}

void vbpt_log_dealloc(vbpt_log_t *log)
{
	assert(ret->state == VBPT_LOG_FINALIZED);
	pset_tfree(&ret->rd_set);
	free(ret);
}
