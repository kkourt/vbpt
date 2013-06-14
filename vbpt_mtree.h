#ifndef VBPT_MTREE_H
#define VBPT_MTREE_H

#include <stdbool.h>

#include "vbpt.h"
#include "vbpt_stats.h"
#include "misc.h"

/*  mutable tree objects on top of immutable versioned trees */

/**
 * Garbage collecting trees
 *
 * tree nodes (vbpt_hdr_t), are referenced by:
 *  - other nodes (inteernal nodes and leafs)
 *  - trees       (vbpt_tree_t's ->root), which exist:
 *   . in transactions
 *   . in an mtree
 */

/**
 * Mutable trees:
 * @tree_current current tree version
 * @mt_lock  serialize access to mtree
 * @gc_lock  serialize gc on version chain
 * @tx_lock  to be used exclusively by transaction code
 */
struct vbpt_mtree {
	vbpt_tree_t *mt_tree;
	spinlock_t   mt_lock;
	spinlock_t   gc_lock;
	spinlock_t   tx_lock;
};
typedef struct vbpt_mtree vbpt_mtree_t;

vbpt_mtree_t *vbpt_mtree_alloc(vbpt_tree_t *tree);
void          vbpt_mtree_dealloc(vbpt_mtree_t *mtree, vbpt_tree_t **tree_ptr);

/* we do a branch (i.e., grab a references for the root and version) under a
 * lock, so that it won't dissapear */
static inline void
vbpt_mtree_branch(vbpt_mtree_t *mtree, vbpt_tree_t *tree)
{
	spin_lock(&mtree->mt_lock);
	vbpt_tree_branch_init(mtree->mt_tree, tree);
	spin_unlock(&mtree->mt_lock);
}


bool vbpt_mtree_try_commit(vbpt_mtree_t *mtree,
                           vbpt_tree_t *tree, ver_t *b_ver,
                           vbpt_tree_t *mt_tree_dst);

bool vbpt_mtree_try_commit2(vbpt_mtree_t *mtree,
                            vbpt_tree_t *tree,
                            ver_t *b_ver,
                            vbpt_tree_t **mt_tree_old_ptr);

bool vbpt_mtree_try_commit3(vbpt_mtree_t *mtree,
                            vbpt_tree_t *tree,
                            ver_t *b_ver,
                            vbpt_tree_t **mt_tree_old_ptr);
#endif /* VBPT_MTREE_H */
