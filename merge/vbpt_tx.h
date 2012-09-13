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
	vbpt_logtree_dealloc(txt->tree);
	free(txt);
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


static inline vbpt_txt_res_t
vbpt_txt_try_commit(vbpt_txtree_t *txt, vbpt_mtree_t *mt,
                    unsigned merge_repeats)
{
	vbpt_txt_res_t ret = VBPT_COMMIT_OK;
	vbpt_tree_t *tx_tree = txt->tree;
	ver_t *bver = txt->bver;

	VBPT_START_TIMER(txt_try_commit);
	for (unsigned i=0;;) {
		vbpt_tree_t gtree;
		if (vbpt_mtree_try_commit(mt, tx_tree, bver, &gtree)) {
			vbpt_tree_destroy(&gtree);
			goto end;
		}

		ret = VBPT_COMMIT_MERGED;
		// try to merge (TODO: we know Vj, implement better merge)
		//tmsg("trying to merge %zd to %zd\n", tx_tree->ver->v_id, gtree.ver->v_id);
		bool ret_merge = vbpt_merge(&gtree, tx_tree, &bver);
		vbpt_tree_destroy(&gtree);
		if (!ret_merge) {
			ret = VBPT_COMMIT_MERGE_FAILED;
			break;
		}

		if (i++ == merge_repeats) {
			ret = VBPT_COMMIT_FAILED;
			break;
		}
	}

	vbpt_tree_dealloc(tx_tree);
end:
	free(txt);
	VBPT_STOP_TIMER(txt_try_commit);
	return ret;
}

#endif /* VBPT_TX_ */

