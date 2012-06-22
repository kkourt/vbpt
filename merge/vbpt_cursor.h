#ifndef VBPT_CURSOR_H
#define VBPT_CURSOR_H

#include <inttypes.h>
#include <stdbool.h>

#include "vbpt.h"
#include "ver.h"

/**
 * Cursor interface
 */

typedef struct vbpt_cur   vbpt_cur_t;
typedef struct vbpt_range vbpt_range_t;

vbpt_cur_t *vbpt_cur_alloc(vbpt_tree_t *);
void vbpt_cur_free(vbpt_cur_t *);

ver_t *vbpt_cur_ver(vbpt_cur_t *);

void vbpt_cur_down(vbpt_cur_t *);
void vbpt_cur_downrange(vbpt_cur_t *, vbpt_range_t *);
void vbpt_cur_next(vbpt_cur_t *);

/**
 * Implementation
 */

/**
 * Key ranges
 */
struct vbpt_range {
	uint64_t key, len;
};

/**
 * Cursor:
 *  We keep an explicit range, because the nodes in the path might not contain
 *  enough information about the current range (e.g., in cases where no nodes
 *  exist). Note that @range is the range of the node pointed to.
 */
struct vbpt_cur {
	vbpt_tree_t  *tree;
	vbpt_path_t  path;
	vbpt_range_t range;
	uint64_t     null_max;
};
//typedef struct vbpt_cur vbpt_cur_t;

//typedef struct vbpt_range vbpt_range_t;

// return true if the two versions are equal
static inline bool
vbpt_range_eq(vbpt_range_t *r1, vbpt_range_t *r2)
{
	return r1->k == r2->k && r1->len == r2->len;
}

// return true if r1 <= r2 -- i.e., r1 is fully included in r2
static inline bool
vbpt_range_leq(vbpt_range_t *r1, vbpt_range_t *r2)
{
	if (r1->k < r2->k)
		return false;
	if (r1->k + r1->len > r2->k + r2->len)
		return false;
	return true;
}

// return true if r1 < r2 -- i.e., r1 is strictly included in r1
static inline bool
vbpt_range_lt(vbpt_range_t *r1, vbpt_range_t *r2)
{
	if (vbpt_range_eq(r1, r2))
		return false;
	return vbpt_range_leq(r1, r2);
}


#endif /* VBPT_CURSOR_H */
