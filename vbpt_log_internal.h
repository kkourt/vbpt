#ifndef VBPT_LOG_INTERNAL_H__
#define VBPT_LOG_INTERNAL_H__

// Internal (implementation) of logs. We keep that in a separate file, so that
// it can by included by ver.h, which needs vbpt_log_t's size.

// implementation details:
//  We are using a hash set for sets. This has two problems:
//   - no way to do efficient range checks
//   - iterating values (e.g., for replay) is O(hash table size)
//
//  Judy1 seems like a good ds for something like this, but we don't use it
//  because it is not trivial to make it persistent. It might make sense to do a
//  Judy1 implementation to measure the performance hit.


typedef struct vbpt_log vbpt_log_t;

enum {
	VBPT_LOG_UNINITIALIZED = 0,
	VBPT_LOG_STARTED       = 1,
	VBPT_LOG_FINALIZED     = 2,
};

#if defined(VBPT_LOG_PHASH)
/**
 * vbpt_log: log for changes in an object
 */
#include "phash.h"
struct vbpt_log {
	unsigned state;
	pset_t   rd_set;
	pset_t   rm_set;
	phash_t  wr_set;
};
#elif defined(VBPT_LOG_RANGE)
#include "vbpt_range.h"
struct vbpt_log {
	unsigned state;
	vbpt_range_t rd_range;
	vbpt_range_t rm_range;
	vbpt_range_t wr_range;
};
#endif


// XXX: for ver_release()
void vbpt_log_destroy(vbpt_log_t *log); // destroy a log (pairs with _init)

#endif /* VBPT_LOG_INTERNAL_H__ */
