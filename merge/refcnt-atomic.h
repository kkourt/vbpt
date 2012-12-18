#ifndef REFCNT_H_
#define REFCNT_H_

#include <inttypes.h>
#include <assert.h>
#include "misc.h"

#include "processor.h"

// Documentation/kref.txt in linux kernel is an interesting and relevant read

struct refcnt {
	atomic_t cnt;
};
typedef struct refcnt refcnt_t;

// for debugging
static inline uint32_t
refcnt_(refcnt_t *r)
{
	return atomic_read(&r->cnt);
}

static inline void
refcnt_init(refcnt_t *rcnt, uint32_t cnt)
{
	atomic_set(&rcnt->cnt, cnt);
}

static inline uint32_t
refcnt_get(refcnt_t *rcnt)
{
	return atomic_read(&rcnt->cnt);
}

static inline bool
refcnt_try_get(refcnt_t *rcnt, uint32_t *cnt)
{
	*cnt = atomic_read(&rcnt->cnt);
	return true;
}

/*
 * refcnt_{inc,dec}__ just perform the atomic operations
 */

static inline void
refcnt_inc__(refcnt_t *rcnt)
{
	atomic_inc(&rcnt->cnt);
}

static inline uint32_t
refcnt_dec__(refcnt_t *rcnt)
{
	return atomic_dec_return(&rcnt->cnt);
}

static inline void
refcnt_inc(refcnt_t *rcnt)
{
	assert(rcnt->cnt > 0);
	refcnt_inc__(rcnt);
}


static inline int
refcnt_dec(refcnt_t *rcnt, void  (*release)(refcnt_t *))
{
	if (refcnt_dec__(rcnt) == 0) {
		release(rcnt);
		return 1;
	}
	return 0;
}

#endif /* REFCNT_H_ */
