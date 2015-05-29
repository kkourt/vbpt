/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#ifndef VBPT_RANGE_H
#define VBPT_RANGE_H

#include <assert.h>

/**
 * Key ranges
 */
struct vbpt_range {
	uint64_t key, len;
};
typedef struct vbpt_range vbpt_range_t;

// return true if the two versions are equal
static inline bool
vbpt_range_eq(const vbpt_range_t *r1, const vbpt_range_t *r2)
{
	return r1->key == r2->key && r1->len == r2->len;
}

// return true if r1 <= r2 -- i.e., r1 is fully included in r2
static inline bool
vbpt_range_leq(const vbpt_range_t *r1, const vbpt_range_t *r2)
{
	if (r1->key < r2->key)
		return false;

	// be vewy vewy cawefull of ovewflows
	uint64_t x = r1->key - r2->key;
	if (x > r2->len)
		return false;
	if (r2->len - x < r1->len)
		return false;
	return true;
}

// return true if r1 < r2 -- i.e., r1 is strictly included in r1
static inline bool
vbpt_range_lt(const vbpt_range_t *r1, const vbpt_range_t *r2)
{
	if (vbpt_range_eq(r1, r2))
		return false;
	return vbpt_range_leq(r1, r2);
}

static inline bool
vbpt_range_contains(const vbpt_range_t *r1, uint64_t key)
{
	if (key < r1->key)
		return false;
	if (key >= r1->key + r1->len)
		return false;

	return true;
}

static inline bool
vbpt_range_intersects(const vbpt_range_t *r1, const vbpt_range_t *r2)
{
	const vbpt_range_t *rb, *rs;
	if (r1->key > r2->key) {
		rb = r1;
		rs = r2;
	} else {
		rb = r2;
		rs = r1;
	}

	assert(rb->len > 0);
	return (rs->key + rs->len - 1 < rb->key) ? false : true;
}

#endif /* VBPT_RANGE_H */
