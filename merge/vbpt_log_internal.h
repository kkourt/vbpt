#ifndef VBPT_LOG_INTERNAL_H__
#define VBPT_LOG_INTERNAL_H__

// Intennal (implementation) of logs. We keep that in a separate file, so that
// it can by included by ver.h, which needs vbpt_log_t's size.

// implementation details:
//  We are using a hash set for sets. This has two problems:
//   - no way to do efficient range checks
//   - iterating values (e.g., for replay) is O(hash table size)
//
//  Judy1 seems like a good ds for something like this, but we don't use it
//  because it is not trivial to make it persistent. It might make sense to do a
//  Judy1 implementation to measure the performance hit.

#include "phash.h"

typedef struct vbpt_log vbpt_log_t;

enum {
	VBPT_LOG_UNINITIALIZED=0,
	VBPT_LOG_STARTED = 1,
	VBPT_LOG_FINALIZED,
};

/**
 * vbpt_log: log for changes in an object
 */
struct vbpt_log {
	unsigned state;
	pset_t   rd_set;
	pset_t   rm_set;
	phash_t  wr_set;
};

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

// XXX: for ver_release()
void vbpt_log_destroy(vbpt_log_t *log); // destroy a log (pairs with _init)

#endif /* VBPT_LOG_INTERNAL_H__ */
