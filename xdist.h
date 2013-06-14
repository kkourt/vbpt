#ifndef XDIST_H
#define XDIST_H

#include <stdlib.h> // rand_r, RAND_MAX
#include <assert.h>

#include "misc.h"   // xmalloc()

// some helpers for random numbers

// distribution description
struct xdist_desc {
	uint64_t r_start;
	uint64_t r_len;
	uint64_t nr;
	unsigned int seed;
};

#define xdist_for_each(xd, n) \
	for (unsigned i__ = 0;                              \
	     i__ < (xd)->nr && ((n = xdist_rand(xd)) || 1); \
             i__++)

static inline uint64_t
xdist_rand(struct xdist_desc *d)
{
	if (d->r_len > RAND_MAX) {
		assert(0 && "FIXME");
	}

	uint64_t r = rand_r(&d->seed);
	return d->r_start + (r % d->r_len);
}

static inline void
xdist_generate_keys(struct xdist_desc *d, uint64_t **data_ptr, uint64_t *data_len)
{
	if (d->r_len > RAND_MAX) {
		assert(0 && "FIXME");
	}

	uint64_t *data = *data_ptr;
	unsigned int seed = d->seed;
	uint64_t dlen = sizeof(uint64_t)*d->nr;
	if (data == NULL) {
		data = xmalloc(dlen);
	} else if (*data_len != dlen) {
		data = xrealloc(data, dlen);
	}
	*data_len = dlen;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t r = (seed == 0) ? i : rand_r(&seed);
		data[i] = d->r_start + (r % d->r_len);
	}

	*data_ptr = data;
}

static inline void
xdist_print(struct xdist_desc *d)
{
	printf("start:%6lu len:%6lu nr:%6lu seed:%u\n",
	       d->r_start, d->r_len, d->nr, d->seed);
}

#endif // XDIST_H
