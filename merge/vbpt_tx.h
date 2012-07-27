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
	vbpt_txtree_t *ret = xmalloc(sizeof(vbpt_txtree_t));
	vbpt_tree_t *tree_b = vbpt_mtree_tree(mtree);
	ret->tree = vbpt_logtree_branch(tree_b);
	ret->depth = 1;
	ret->bver = tree_b->ver;
	return ret;
}

static inline void
vbpt_txtree_dealloc(vbpt_txtree_t *txt)
{
	vbpt_logtree_dealloc(txt->tree);
	free(txt);
}

static inline bool
vbpt_txtree_try_commit(vbpt_txtree_t *txt, vbpt_mtree_t *mtree)
{
	bool ret = vbpt_mtree_try_commit(mtree, txt->tree, txt->bver, NULL);
	if (ret)
		free(txt);
	return ret;
}

static inline bool
vbpt_txtree_try_commit_merge(vbpt_txtree_t *txt, vbpt_mtree_t *mt,
                             unsigned repeats)
{
	bool ret = true;
	vbpt_tree_t *tx_tree = txt->tree;
	ver_t *bver = txt->bver;
	for (unsigned i=0; i<=repeats; i++) {
		vbpt_tree_t *gtree;
		if (vbpt_mtree_try_commit(mt, tx_tree, bver, &gtree)) {
			vbpt_mtree_dealloc_tree(gtree);
			goto success;
		}
		// try to merge (TODO: we know Vj, implement better merge)
		if (!vbpt_merge(gtree, tx_tree, &bver)) {
			break;
		}
	}
	ret = false; // failure
	vbpt_mtree_dealloc_tree(tx_tree);
success:
	free(txt);
	return ret;
}

#endif /* VBPT_TX_ */

