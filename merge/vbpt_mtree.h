#ifndef VBPT_MTREE_H
#define VBPT_MTREE_H

#include <stdbool.h>

#include "vbpt.h"
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
 * @mt_lock      atomically update mtree
 */
struct vbpt_mtree {
	vbpt_tree_t *mt_tree;
	spinlock_t   mt_lock;
	spinlock_t   gc_lock;
};
typedef struct vbpt_mtree vbpt_mtree_t;

/**
 * allocate a new mtree and initialize it with @tree
 *
 * return the mtree
 *
 * @tree's refcount is not increased
 */
static inline vbpt_mtree_t *
vbpt_mtree_alloc(vbpt_tree_t *tree)
{
	vbpt_mtree_t *ret = xmalloc(sizeof(vbpt_mtree_t));
	ret->mt_tree = tree;
	spinlock_init(&ret->mt_lock);
	spinlock_init(&ret->gc_lock);
	ver_pin(ret->mt_tree->ver, NULL);
	return ret;
}

static inline void
vbpt_mtree_dealloc(vbpt_mtree_t *mtree, vbpt_tree_t **tree_ptr)
{
	vbpt_tree_t *tree = mtree->mt_tree;

	free(mtree);

	if (tree_ptr) {
		*tree_ptr = tree;
	} else {
		ver_t *v = tree->ver;
		vbpt_tree_dealloc(tree);
		ver_tree_gc(v);
		ver_unpin(v);
	}
}

/* we do a branch (i.e., grab a references for the root and version) under a
 * lock, so that it won't dissapear */
static inline void
vbpt_mtree_branch(vbpt_mtree_t *mtree, vbpt_tree_t *tree)
{
	spin_lock(&mtree->mt_lock);
	vbpt_tree_branch_init(mtree->mt_tree, tree);
	spin_unlock(&mtree->mt_lock);
}

/**
 * try to commit a new version to @mtree
 *
 * @tree:        the new version of the tree
 * @b_ver:       the version @tree is based on
 * @mt_tree_dst: if not NULL, a copy of old version of the tree is placed here
 *
 * if (un)successful true (false) is returned.
 *
 * @tree's refcount is not increased
 * if @mt_tree_dst is not NULL, caller is responisble for releasing the tree
 * using vbpt_tree_dealloc().
 */
static inline bool
vbpt_mtree_try_commit(vbpt_mtree_t *mtree, vbpt_tree_t *tree,
                      ver_t *b_ver, vbpt_tree_t *mt_tree_dst)
{
	bool committed = false;
	vbpt_tree_t *mt_tree;
	spin_lock(&mtree->mt_lock);
	ver_t *cur_ver = (mt_tree = mtree->mt_tree)->ver;
	//tmsg("trying to commit ver:%zd to cur_ver:%zd\n", tree->ver->v_id, cur_ver->v_id);
	if (ver_eq(cur_ver, b_ver)) {
		mtree->mt_tree = tree;
		committed = true;
	}
	if (mt_tree_dst)
		vbpt_tree_copy(mt_tree_dst, mt_tree);
	spin_unlock(&mtree->mt_lock);

	// pin without holding the lock
	if (committed)
		ver_pin(mtree->mt_tree->ver, mt_tree->ver);


	if (committed) {
		vbpt_tree_dealloc(mt_tree);
	}

	return committed;
}

#endif /* VBPT_MTREE_H */
