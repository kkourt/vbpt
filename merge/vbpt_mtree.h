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
vbpt_mtree_dealloc_tree(vbpt_tree_t *tree)
{
	if (vbpt_tree_log(tree)->state == VBPT_LOG_UNINITIALIZED)
		vbpt_tree_dealloc(tree);
	else
		vbpt_logtree_dealloc(tree);
}

static inline void
vbpt_mtree_destroy(vbpt_mtree_t *mtree, vbpt_tree_t **tree_ptr)
{
	vbpt_tree_t *tree = mtree->mt_tree;

	free(mtree);

	if (tree_ptr) {
		*tree_ptr = tree;
	} else {
		ver_t *v = tree->ver;
		vbpt_mtree_dealloc_tree(tree);
		ver_tree_gc(v);
		ver_unpin(v);
	}
}

/**
 * get the current version of mtree
 */
static inline vbpt_tree_t *
vbpt_mtree_tree(vbpt_mtree_t *mtree)
{
	return mtree->mt_tree; // ASSUMPTION: this is atomic
}

/**
 * try to commit a new version to @mtree
 *
 * @tree:        the new version of the tree
 * @b_ver:       the version @tree is based on
 * @mt_tree_ptr: if not NULL, the old version of the tree is placed here
 *
 * if successful:
 *   - true is returned
 *   - if @mt_tree_ptr is NULL, vbpt_tree_dealloc() is called with the old tree
 * if unsuccessful:
 *   - false is returned
 *
 * @tree's refcount is not increased
 */
static inline bool
vbpt_mtree_try_commit(vbpt_mtree_t *mtree, vbpt_tree_t *tree,
                      ver_t *b_ver, vbpt_tree_t **mt_tree_ptr)
{
	bool committed = false;
	vbpt_tree_t *mt_tree;
	spin_lock(&mtree->mt_lock);
	ver_t *cur_ver = (mt_tree = mtree->mt_tree)->ver;
	if (ver_eq(cur_ver, b_ver)) {
		mtree->mt_tree = tree;
		committed = true;
	}
	spin_unlock(&mtree->mt_lock);

	// pin without holding the lock
	if (committed)
		ver_pin(mtree->mt_tree->ver, mt_tree->ver);


	if (mt_tree_ptr) {
		*mt_tree_ptr = mt_tree;
	} else if (committed) {
		vbpt_mtree_dealloc_tree(mt_tree);
	}

	return committed;
}

#endif /* VBPT_MTREE_H */
