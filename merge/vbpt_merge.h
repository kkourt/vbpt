#ifndef VBPT_MERGE_H
#define VBPT_MERGE_H

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

ver_t *vbpt_cur_ver(const vbpt_cur_t *);

void vbpt_cur_down(vbpt_cur_t *);
void vbpt_cur_downrange(vbpt_cur_t *, const vbpt_cur_t *);
int vbpt_cur_next(vbpt_cur_t *);

bool vbpt_cur_end(vbpt_cur_t *);

void vbpt_cur_sync(vbpt_cur_t *cur1, vbpt_cur_t *cur2);
static inline bool vbpt_cur_null(const vbpt_cur_t *c);

bool vbpt_cur_replace(vbpt_cur_t *pc, const vbpt_cur_t *gc);

bool vbpt_cmp(vbpt_tree_t *t1, vbpt_tree_t *t2);

/**
 * Merging
 */

bool vbpt_merge(const vbpt_tree_t *gt, vbpt_tree_t *pt, ver_t **vbase);
bool vbpt_log_merge(vbpt_tree_t *gtree, vbpt_tree_t *ptree);

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
 *  exist). Note that @range is the range of the node that the cursor points to.
 */
struct vbpt_cur {
	vbpt_tree_t  *tree;
	vbpt_path_t  path;
	vbpt_range_t range;
	uint64_t     null_maxkey;
	struct {
		uint8_t deleteme:1;
		uint8_t null:1;
	}            flags;
};

static inline bool
vbpt_cur_null(const vbpt_cur_t *c)
{
	return c->flags.null != 0;
}

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
	if (r1->key + r1->len > r2->key + r2->len)
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


#endif /* VBPT_MERGE_H */
