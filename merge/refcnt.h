#ifndef REFCNT_H_
#define REFCNT_H_

#include <inttypes.h>
#include "misc.h"

struct refcnt {
	uint32_t cnt;
	spinlock_t lock; /* we assume that we don't have atomic ops */
};
typedef struct refcnt refcnt_t;

static inline void
refcnt_init(refcnt_t *rcnt, uint32_t cnt)
{
	spinlock_init(&rcnt->lock);
	rcnt->cnt = cnt;
}

static inline void
refcnt_inc(refcnt_t *rcnt)
{
	assert(rcnt->cnt > 0);
	spin_lock(&rcnt->lock);
	rcnt->cnt++;
	spin_unlock(&rcnt->lock);
}

static inline void
refcnt_dec(refcnt_t *rcnt)
{
	assert(rcnt->cnt > 0);
	spin_lock(&rcnt->lock);
	if (--rcnt->cnt == 0) {
		// TODO
	}
	spin_unlock(&rcnt->lock);
}

#endif /* REFCNT_H_ */
