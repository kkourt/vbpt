#ifndef VBPT_TX_
#define VBPT_TX_

#include "vbpt.h"
#include "vbpt_log.h"
#include "vbpt_mtree.h"

/**
 * a transaction on a vbpt tree
 *  @txt_depth: distance of @txt_tree from the base version
 *  @txt_bver: base version -- this is redundant since the version is reachable
 *              via txt_tree and txt_depth, but we keep it for convience. Since
 *              it is redundant, we do not keep a reference to it
 *  @txt_tree: current tree
 */
struct vbpt_txtree {
	ver_t       *bver;
	unsigned    depth;
	vbpt_tree_t *tree;
};
typedef struct vbpt_txtree vbpt_txtree_t;

static inline vbpt_txtree_t *
vbpt_txtree_alloc(vbpt_mtree_t *mtree)
{
	VBPT_START_TIMER(txtree_alloc);
	vbpt_txtree_t *ret = xmalloc(sizeof(vbpt_txtree_t));

	vbpt_tree_t *tree = vbpt_tree_alloc(NULL); // allocate a dummy tree
	vbpt_mtree_branch(mtree, tree);
	vbpt_logtree_log_init(tree);

	ret->tree = tree;
	ret->depth = 1;
	ret->bver = tree->ver->parent;
	VBPT_STOP_TIMER(txtree_alloc);
	return ret;
}


static inline void
vbpt_txtree_dealloc(vbpt_txtree_t *txt)
{
	VBPT_START_TIMER(txtree_dealloc);
	vbpt_logtree_dealloc(txt->tree);
	free(txt);
	VBPT_STOP_TIMER(txtree_dealloc);
}

/* transaction result */
typedef enum {
	VBPT_COMMIT_OK     =       0,
	VBPT_COMMIT_MERGED =       1,
	VBPT_COMMIT_MERGE_FAILED = 2,
	VBPT_COMMIT_FAILED =       3,
} vbpt_txt_res_t;

static char __attribute__((unused)) *vbpt_txt_res2str[] = {
	[VBPT_COMMIT_OK]           =  "COMMIT OK",
	[VBPT_COMMIT_MERGED]       =  "COMMIT MERGED",
	[VBPT_COMMIT_MERGE_FAILED] =  "COMMIT MERGE FAILED",
	[VBPT_COMMIT_FAILED]       =  "COMMIT FAILED"
};

static inline void
vbpt_txt_update_stats(vbpt_txt_res_t ret)
{

	switch (ret) {
		case VBPT_COMMIT_FAILED:
		VBPT_INC_COUNTER(commit_fail);
		break;

		case VBPT_COMMIT_MERGE_FAILED:
		VBPT_INC_COUNTER(commit_merge_fail);
		break;

		case VBPT_COMMIT_OK:
		VBPT_INC_COUNTER(commit_ok);
		break;

		case VBPT_COMMIT_MERGED:
		VBPT_INC_COUNTER(commit_merge_ok);
		break;
	}
}

// There are two, unimaginatively named, functions to commit
//
// -vbpt_tx_try_commit (using vbpt_mtree_try_commit):
//     checks to see if it can commit. If yes, that's it. If not, it releases
//     the lock, tries to perform a merge, and if successful tries to commit
//     again. Since it has released the lock, another process might already have
//     commited. It retries for a number of times.
//
// -vbpt_tx_try_commit2 (using vbpt_mtree_try_commit2):
//     takes the lock and checks to is if it can commit. If yes, that's it.
//     If not, it tries to merge (still holding the lock) and if it succeeds
//     it commits its changes.
//

static inline vbpt_txt_res_t
vbpt_txt_try_commit(vbpt_txtree_t *txt,
                    vbpt_mtree_t *mt,
                    unsigned merge_repeats)
{
	vbpt_txt_res_t ret = VBPT_COMMIT_OK;
	vbpt_tree_t *tx_tree = txt->tree; // transaction tree
	ver_t *bver = txt->bver;          // transaction base version

	VBPT_START_TIMER(txt_try_commit);
	unsigned cnt;
	for (cnt = 0; ;cnt++) {
		vbpt_tree_t gtree;
		if (vbpt_mtree_try_commit(mt, tx_tree, bver, &gtree)) {
			vbpt_tree_destroy(&gtree);
			goto success;
		}

		ret = VBPT_COMMIT_MERGED;
		// try to merge (TODO: we know Vj, implement better merge)
		//tmsg("trying to merge %zd to %zd\n",
		//      tx_tree->ver->v_id, gtree.ver->v_id);
		bool ret_merge = vbpt_merge(&gtree, tx_tree, &bver);
		vbpt_tree_destroy(&gtree);
		if (!ret_merge) {
			ret = VBPT_COMMIT_MERGE_FAILED;
			break;
		}

		if (cnt == merge_repeats) {
			ret = VBPT_COMMIT_FAILED;
			break;
		}

		//XXX: There seems to be a bug when looping back: versions with
		// ->rfcnt_children = 2 when no branches should exist in the
		// version tree. Need to investigate.
		//ret = VBPT_COMMIT_FAILED;
		//break;
	}

	/* failure */
	ver_detach(tx_tree->ver);
	vbpt_tree_dealloc(tx_tree);
success:
	VBPT_XCNT_ADD(merge_iters, cnt);
	free(txt);
	vbpt_txt_update_stats(ret);
	VBPT_STOP_TIMER(txt_try_commit);
	return ret;
}

static inline vbpt_txt_res_t
vbpt_txt_try_commit2(vbpt_txtree_t *txt, vbpt_mtree_t *mt)
{
	vbpt_txt_res_t result;
	vbpt_tree_t *old_tree;
	vbpt_tree_t *tx_tree = txt->tree;
	ver_t *bver = txt->bver;

	VBPT_START_TIMER(txt_try_commit);

	spin_lock(&mt->mt_lock);

	// try to commit
	if (vbpt_mtree_try_commit2(mt, tx_tree, bver, &old_tree)) {
		vbpt_tree_dealloc(old_tree);
		result = VBPT_COMMIT_OK;
		goto success;
	}

	// still holding the lock, try to merge
	if (vbpt_merge(old_tree, tx_tree, &bver)) {
		// merge succeeded
		vbpt_tree_t *old_tree2;
		bool ret;
		ret = vbpt_mtree_try_commit2(mt, tx_tree, bver, &old_tree2);

		// we holded the lock before calling, so no change should have
		// happened on mtree
		if (!ret) {
			fprintf(stderr, "This should not happen\n");
			abort();
		}
		assert(old_tree == old_tree2);

		result = VBPT_COMMIT_MERGED;
		goto success;
	}

	// failed to merge, need to clean up
	result = VBPT_COMMIT_MERGE_FAILED;
	spin_unlock(&mt->mt_lock);
	ver_detach(tx_tree->ver);
	vbpt_tree_dealloc(tx_tree);

success:
	free(txt);
	vbpt_txt_update_stats(result);
	VBPT_STOP_TIMER(txt_try_commit);
	return result;
}

#endif /* VBPT_TX_ */

