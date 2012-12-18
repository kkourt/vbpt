#ifndef REFCNT_H_
#define REFCNT_H_

#include <inttypes.h>
#include <assert.h>
#include "misc.h"

#include "processor.h"

// Documentation/kref.txt in linux kernel is an interesting and relevant read

struct refcnt {
	uint32_t cnt;
	spinlock_t lock; /* we assume that we don't have atomic ops */
};
typedef struct refcnt refcnt_t;

// for debugging
static inline uint32_t
refcnt_(refcnt_t *r)
{
	return r->cnt;
}

static inline void
refcnt_init(refcnt_t *rcnt, uint32_t cnt)
{
	spinlock_init(&rcnt->lock);
	rcnt->cnt = cnt;
}

static inline uint32_t
refcnt_get(refcnt_t *rcnt)
{
	uint32_t ret;
	spin_lock(&rcnt->lock);
	ret = rcnt->cnt;
	spin_unlock(&rcnt->lock);
	return ret;
}

static inline bool
refcnt_try_get(refcnt_t *rcnt, uint32_t *cnt)
{
	bool ret = false;
	if (spin_try_lock(&rcnt->lock)) {
		*cnt = rcnt->cnt;
		ret = true;
		spin_unlock(&rcnt->lock);
	}
	return ret;
}

/*
 * refcnt_{inc,dec}__ just perform the atomic operations
 */

static inline void
refcnt_inc__(refcnt_t *rcnt)
{
	spin_lock(&rcnt->lock);
	rcnt->cnt++;
	spin_unlock(&rcnt->lock);
}

static inline uint32_t
refcnt_dec__(refcnt_t *rcnt)
{
	uint32_t ret;
	spin_lock(&rcnt->lock);
	ret = --rcnt->cnt;
	spin_unlock(&rcnt->lock);
	return ret;
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
	assert(rcnt->cnt > 0);
	spin_lock(&rcnt->lock);
	if (--rcnt->cnt == 0) {
		release(rcnt);
		// this is ugly, but since the lock lives in refcnt, and refcnt
		// is part of the object that is going to be released, we can't
		// unlock(), because after the release we don't own the memory
		return 1;
	}
	spin_unlock(&rcnt->lock);
	return 0;
}

#endif /* REFCNT_H_ */
