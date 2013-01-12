#ifndef XDIST_H
#define XDIST_H

// some helpers for random numbers

// distribution description
struct xdist_desc {
	uint64_t r_start;
	uint64_t r_len;
	uint64_t nr;
	unsigned int seed;
};

static inline uint64_t
xdist_rand(struct xdist_desc *d, unsigned *seed)
{
	if (d->r_len > RAND_MAX) {
		assert(false && "FIXME");
	}

	uint64_t r = rand_r(seed);
	return d->r_start + (r % d->r_len);
}

static inline void
xdist_generate_keys(struct xdist_desc *d, uint64_t **data_ptr, uint64_t *data_len)
{
	if (d->r_len > RAND_MAX) {
		assert(false && "FIXME");
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



#endif // XDIST_H
