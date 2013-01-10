#include "vbpt.h"
#include "vbpt_mtree.h"

/*  mutable tree objects on top of immutable versioned trees */

/**
 * allocate a new mtree and initialize it with @tree
 *
 * return the mtree
 *
 * @tree's refcount is not increased
 */
vbpt_mtree_t *
vbpt_mtree_alloc(vbpt_tree_t *tree)
{
	vbpt_mtree_t *ret = xmalloc(sizeof(vbpt_mtree_t));
	ret->mt_tree = tree;
	spinlock_init(&ret->mt_lock);
	spinlock_init(&ret->gc_lock);
	spinlock_init(&ret->tx_lock);
	ver_pin(ret->mt_tree->ver, NULL);
	return ret;
}


void
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
		if (v->parent != NULL) {
			// this should not happen, we should be able to collect
			// all of the version chain at this point
			ver_chain_print(v);
			//exit(1);
		}
		ver_unpin(v);
	}
}

/**
 * try to commit a new version to @mtree
 *
 * @tree:        the new version of the tree
 * @b_ver:       the version @tree is based on
 *
 * @mt_tree_dst: if not NULL, a copy of old version of the tree is placed here
 *               if the commit fails.
 *
 *               The tree reference is intended to be used by the caller to
 *               perform a merge. Doing the copy (essentially just taking a
 *               reference) under the lock guarantees that the tree won't
 *               dissapear while the caller performs the merge.
 *
 *               In addition, if @mt_tree_dst is not NULL, ver_rebase_prepare
 *               will be called on its version. Doing it under the lock, assures
 *               that the version won't get collected before the merge adds a
 *               child to it.
 *
 * if (un)successful true (false) is returned.
 *
 * @tree's refcount is not increased
 * if @mt_tree_dst is not NULL, caller is responisble for releasing the tree
 * using vbpt_tree_dealloc().
 */
bool
vbpt_mtree_try_commit(vbpt_mtree_t *mtree, vbpt_tree_t *tree,
                      ver_t *b_ver, vbpt_tree_t *mt_tree_dst)
{
	VBPT_START_TIMER(mtree_try_commit);
	bool committed = false;
	vbpt_tree_t *mt_tree;

	spin_lock(&mtree->mt_lock);
	ver_t *cur_ver = (mt_tree = mtree->mt_tree)->ver;
	//tmsg("trying to commit ver:%zd to cur_ver:%zd\n",
	//      tree->ver->v_id, cur_ver->v_id);
	if (ver_eq(cur_ver, b_ver)) {
		//tmsg("commited ver:%zd to previous:%zd\n",
		//     tree->ver->v_id, cur_ver->v_id);
		mtree->mt_tree = tree;
		committed = true;
	} else if (mt_tree_dst) {
		// failure: copy tree to mt_tree_dst, so that caller can try to
		// merge
		vbpt_tree_copy(mt_tree_dst, mt_tree);
		ver_rebase_prepare(mt_tree->ver);
	}
	spin_unlock(&mtree->mt_lock);

	// pin without holding the lock
	if (committed) {
		ver_pin(tree->ver, mt_tree->ver);

		// run gc for versions before the pinned version
		// if somebody else has the lock, just continue
		if (spin_try_lock(&mtree->gc_lock)) {
			ver_tree_gc(tree->ver);
			spin_unlock(&mtree->gc_lock);
		}

		vbpt_tree_dealloc(mt_tree);
	}
	VBPT_STOP_TIMER(mtree_try_commit);

	return committed;
}

/**
 * try to commit a new version to @mtree
 *
 * @tree:            the new version of the tree
 * @b_ver:           the version @tree is based on
 * @mt_tree_old_ptr: old version of the tree is placed in this poinetr
 *                   (i.e., version before the call)
 *
 * if (un)successful true (false) is returned.
 *
 * caller should take mtree->mt_lock before calling
 * if successful, lock is released
 *
 * @tree's refcount is not increased
 */
bool
vbpt_mtree_try_commit2(vbpt_mtree_t *mtree,
                       vbpt_tree_t *tree,
                       ver_t *b_ver,
                       vbpt_tree_t **mt_tree_old_ptr)
{
	bool committed;
	ver_t *ver_old;

	VBPT_START_TIMER(mtree_try_commit);

	*mt_tree_old_ptr = mtree->mt_tree;
	ver_old          = mtree->mt_tree->ver;

	committed = false;
	if (ver_eq(ver_old, b_ver)) {
		mtree->mt_tree = tree;
		// commit aftermath
		committed = true;
		spin_unlock(&mtree->mt_lock);
		// pin new tree version in place of old
		ver_pin(mtree->mt_tree->ver, ver_old);
		// try to run gc for versions before the pinned version
		// if somebody else has the lock, just continue
		if (spin_try_lock(&mtree->gc_lock)) {
			ver_tree_gc(mtree->mt_tree->ver);
			spin_unlock(&mtree->gc_lock);
		}
	}

	VBPT_STOP_TIMER(mtree_try_commit);
	return committed;
}

/**
 * try to commit a new version to @mtree
 *
 * @tree:            the new version of the tree
 * @b_ver:           the version @tree is based on
 * @mt_tree_old_ptr: old version of the tree is placed in this poinetr
 *                   (i.e., version before the call)
 *
 * if (un)successful true (false) is returned.
 *
 * caller should take mtree->tx_lock before calling
 * if successful, lock is released
 *
 * @tree's refcount is not increased
 */
bool
vbpt_mtree_try_commit3(vbpt_mtree_t *mtree,
                       vbpt_tree_t *tree,
                       ver_t *b_ver,
                       vbpt_tree_t **mt_tree_old_ptr)
{
	bool committed;
	ver_t *ver_old;

	VBPT_START_TIMER(mtree_try_commit);

	spin_lock(&mtree->mt_lock);
	*mt_tree_old_ptr = mtree->mt_tree;
	ver_old          = mtree->mt_tree->ver;
	if (ver_eq(ver_old, b_ver)) {
		mtree->mt_tree = tree;
		committed = true;
	} else {
		committed = false;
		ver_rebase_prepare(ver_old);
	}
	spin_unlock(&mtree->mt_lock);

	if (committed){
		spin_unlock(&mtree->tx_lock);
		// pin new tree version in place of old
		ver_pin(mtree->mt_tree->ver, ver_old);
		// try to run gc for versions before the pinned version
		// if somebody else has the lock, just continue
		if (spin_try_lock(&mtree->gc_lock)) {
			ver_tree_gc(mtree->mt_tree->ver);
			spin_unlock(&mtree->gc_lock);
		}
	}

	VBPT_STOP_TIMER(mtree_try_commit);
	return committed;
}
