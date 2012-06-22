#ifndef VBPT_LOG_H__
#define VBPT_LOG_H__

#include <stdbool.h>
#include "vbpt.h" /* vbpt_leaf_t */

// interface

vbpt_log_t;

vbpt_log_t *vbpt_log_alloc(void);

void vbpt_log_write(vbpt_log_t *log, uint64_t key, vbpt_leaf_t *leaf);
void vbpt_log_read(vbpt_log_t *log, uint64_t key);

void vbpt_log_finalize(vbpt_log_t *log);

// read set (rs) checks
bool vbpt_log_rs_check_key(vbpt_log_t *log, uint64_t key);
bool vbpt_log_rs_check_range(vbpt_log_t *log, uint64_t key0, uint64_t len);

void vbpt_log_dealloc(vbpt_log_t *log);

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
};
typedef struct vbpt_log vbpt_log_t;

#endif
