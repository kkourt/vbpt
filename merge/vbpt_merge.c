
#include "ver.h"
#include "vbpt.h"
#include "vbpt_merge.h"
#include "vbpt_log.h"

#include "misc.h"

#define VBPT_KEY_MAX UINT64_MAX

// forward declaration
static inline void vbpt_cur_maybe_delete(vbpt_cur_t *c);

/**
 * Cursors:
 *  - include a path from the root and point to the node that the path points to
 *  - represent ranges in the keyspace
 *  - move in two directions:
 *   . down             => towards more specific values
 *     (the start of the range remains the same)
 *   . rightside (next) => moving to the next range of values
 *     (the start of the new ranges the end of the old range +1)
 *
 * Cursors aim to provide iterators to perform the "synchronized iteration"
 * required for merging.
 * Most of the code complexity is due to the need to handle NULL values.
 *
 * NULL values arise the following cases:
 *  - the empty keyspace between singular ranges in the last level of the tree
 *  - the empty keyspace before the first value in the tree
 * If ->null_max_key is not zero, cursor is currently in a null range
 */

void
vbpt_cur_print(const vbpt_cur_t *cur)
{
	printf("cursor: range:[%4lu+%4lu] null_max_key:%4lu tree:%p %s\n",
	       cur->range.key, cur->range.len, cur->null_max_key, cur->tree,
	       ver_str(vbpt_cur_ver((vbpt_cur_t *)cur)));
}

/**
 * return the hdr that the cursor points to
 *  if tree->root == NULL, this function will return NULL
 */
vbpt_hdr_t *
vbpt_cur_hdr(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	if (path->height == 0)
		return cur->tree->root ? &cur->tree->root->n_hdr : NULL;
;
	vbpt_node_t *pnode = path->nodes[path->height - 1];
	uint16_t pslot = path->slots[path->height - 1];
	assert(pslot < pnode->items_nr);
	return pnode->kvp[pslot].val;
}

static const vbpt_range_t vbpt_range_full = {.key = 0, .len = VBPT_KEY_MAX};

/**
 * allocate and initialize a cursor
 */
vbpt_cur_t *
vbpt_cur_alloc(vbpt_tree_t *tree)
{
	vbpt_cur_t *ret = xmalloc(sizeof(vbpt_cur_t));
	ret->tree = tree;
	ret->path.height = 0;
	ret->range = vbpt_range_full;
	ret->null_max_key = 0;
	ret->flags.deleteme = 0;
	return ret;
}

/**
 * deallocate a cursor
 */
void
vbpt_cur_free(vbpt_cur_t *cur)
{
	free(cur);
}

/**
 * Return the version of the node that the cursor points to
 *
 *  Note that NULL areas have the version of the last node in the tree. This is
 *  a bit conservative because we can't distinguish if the NULL range was there
 *  before the last node's version or not.
 */
ver_t *
vbpt_cur_ver(const vbpt_cur_t *cur)
{
	const vbpt_path_t *path = &cur->path;
	const vbpt_tree_t *tree = cur->tree;
	if (path->height == 0) {
		return tree->ver;
	} else if (cur->null_max_key == 0) {
		return vbpt_cur_hdr((vbpt_cur_t *)cur)->ver;
	} else {
		vbpt_node_t *pnode = path->nodes[path->height -1];
		return pnode->n_hdr.ver;
	}
}

/**
 * Move cursor down, to the node below
 *
 * Output invariant: cur->range.key: remains unchanged
 */
void
vbpt_cur_down(vbpt_cur_t *cur)
{
	vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);
	vbpt_path_t *path = &cur->path;

	// sanity checks
	assert(!cur->null_max_key && "Can't go down: pointing to NULL");
	if (hdr->type == VBPT_LEAF) {
		assert(cur->range.len == 1); // if this is a leaf, only one key
		assert(false && "Can't go down: pointing to a leaf");
	}
	assert(path->height < VBPT_MAX_LEVEL);


	vbpt_node_t *node = hdr2node(hdr);
	uint64_t node_key0 = node->kvp[0].key;

	// fix path
	path->nodes[path->height] = node;
	path->slots[path->height] = 0;
	path->height++;

	// update the range by reducing the length
	if (vbpt_cur_hdr(cur)->type == VBPT_NODE) {
		assert(node_key0 - cur->range.key);
		cur->range.len = node_key0 - cur->range.key;
	} else if (node_key0 > cur->range.key) {
		// last-level, where we need to inset a NULL area
		cur->range.len = node_key0 - cur->range.key;
		cur->null_max_key = node_key0 - 1;
		path->slots[path->height -1] = 0;
	} else {
		// last-level, singular range pointing to a leaf
		cur->range.key = node_key0;
		cur->range.len = 1;
	}
}

/**
 * move down, trying to reach range @r
 */
void
vbpt_cur_downrange(vbpt_cur_t *cur, const vbpt_cur_t *cur2)
{
	const vbpt_range_t *r = &cur2->range;
	assert(vbpt_range_lt(r, &cur->range));
	do {
		if (cur->null_max_key > 0)
			cur->range = *r;
		else
			vbpt_cur_down(cur);
	} while (!vbpt_range_leq(r, &cur->range));
}

/**
* the cursor is pointing to a leaf, move to the next node
*/
static int
vbpt_cur_next_leaf(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	assert(path->height > 0);
	vbpt_node_t *n = path->nodes[path->height -1];
	uint16_t nslot = path->slots[path->height -1];
	assert(nslot < n->items_nr);
	assert(vbpt_cur_hdr(cur)->type == VBPT_LEAF);

	if (cur->null_max_key != 0) {
		assert(!cur->flags.deleteme);
		// cursor pointing to NULL already:
		uint64_t r_last = cur->range.key + cur->range.len - 1;
		assert(r_last <= cur->null_max_key);
		if (r_last < cur->null_max_key) {
			// if the curent NULL range has not ended, just update
			// the cursor to the remaining NULL range.
			cur->range.key = r_last + 1;
			cur->range.len = cur->null_max_key - r_last;
			return 0;
		} else if (nslot == n->items_nr) {
			// if the current NULL range has ended, but there are no
			// more items in this node, pop and return to the parent
			// node.  XXX: On second thought, I don't think  that's
			// possible
			assert(false);
			goto move_up;
		} else {
			// if the current NULL range has ended, and there are
			// more items to the next key
			cur->null_max_key = 0;
			goto next_key;
		}
	} else if (nslot + 1  == n->items_nr) {
		// no more keys in this node, we need to pop and return to the
		// parent node
		goto move_up;
	} else {
		assert(cur->range.len == 1);
		assert(cur->range.key == n->kvp[nslot].key);
		uint64_t next_key = n->kvp[nslot+1].key;
		vbpt_cur_maybe_delete(cur);
		if (next_key == cur->range.key + 1) {
			// if the current and next key are sequential, we can
			// just move to the next key
			nslot = nslot + 1;
			goto next_key;
		} else {
			// if there is range space between the current and the
			// next key, it is NULL space and we need to account for
			// it. Note that nslot for null values points to the
			// next node
			path->slots[path->height - 1] = nslot + 1;
			cur->null_max_key = next_key - 1;
			cur->range.key += 1;
			cur->range.len = next_key - cur->range.key;
			return 0;
		}
	}
	assert(false && "How did I end up here?");

next_key:
	path->slots[path->height - 1] = nslot;
	cur->range.key = n->kvp[nslot].key;
	cur->range.len = 1;
	return 0;

move_up:
	if (--path->height == 0) {
		cur->range.key = cur->range.key + cur->range.len - 1;
		cur->range.len = VBPT_KEY_MAX - cur->range.key;
		cur->null_max_key = VBPT_KEY_MAX;
		return 0;
	} else return vbpt_cur_next(cur);
}

/**
 * move to next node
 *  returns -1 if it's unable to proceed
 *  (This does not happen currntly, but it might be a good idea to keep the
 *  intrafce so that we can avoid complex cases, by just bailing out)
 */
int
vbpt_cur_next(vbpt_cur_t *cur)
{
	vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);
	if (hdr->type == VBPT_LEAF) {
		return vbpt_cur_next_leaf(cur);
	}

	vbpt_path_t *path = &cur->path;
	while (true) {
		vbpt_node_t *n = path->nodes[path->height -1];
		uint16_t nslot = path->slots[path->height -1];
		assert(nslot < n->items_nr);
		vbpt_cur_maybe_delete(cur);
		if (nslot + 1 < n->items_nr) {
			path->slots[path->height -1] = nslot + 1;
			uint64_t old_high_k = n->kvp[nslot].key;
			cur->range.key = old_high_k + 1;
			cur->range.len = n->kvp[nslot+1].key - old_high_k;
			break;
		}

		if (--path->height == 0) {
			cur->range.key += cur->range.len;
			cur->range.len = VBPT_KEY_MAX - cur->range.key;
			cur->null_max_key = VBPT_KEY_MAX;
			break;
		}
	}

	return 0;
}

/**
 * check if cursor has reached the end of the tree
 */
bool
vbpt_cur_end(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	return (path->height == 0 && cur->null_max_key == VBPT_KEY_MAX);
}

/**
 * synchronize two cursors
 */
void
vbpt_cur_sync(vbpt_cur_t *cur1, vbpt_cur_t *cur2)
{
	assert(!cur1->flags.deleteme && !cur2->flags.deleteme);
	assert(cur1->range.key == cur2->range.key);
	while (!vbpt_range_eq(&cur1->range, &cur2->range)) {
		// order the ranges (s: small, b: big)
		vbpt_cur_t *cs, *cb;
		if (vbpt_range_lt(&cur1->range, &cur2->range)) {
			cs = cur1;
			cb = cur2;
		} else if (vbpt_range_lt(&cur2->range, &cur1->range)) {
			cs = cur2;
			cb = cur1;
		} else {
			vbpt_cur_print(cur1);
			vbpt_cur_print(cur2);
			assert(false && "ranges can't be ordered");
		}
		// try to synchronize big range
		vbpt_cur_downrange(cb, cs);
	}
	assert(vbpt_range_eq(&cur1->range, &cur2->range));
}

/**
 * compare two cursors
 */
bool
vbpt_cur_cmp(vbpt_cur_t *c1, vbpt_cur_t *c2, bool check_leafs)
{

	while (true) {
		while (!vbpt_cur_null(c1) && c1->range.len != 1)
			vbpt_cur_down(c1);
		while (!vbpt_cur_null(c2) && c2->range.len != 1)
			vbpt_cur_down(c2);

		if (!vbpt_range_eq(&c1->range, &c2->range))
			return false;

		if (c1->range.len == 1) {
			assert(c2->range.len == 1);
			if (check_leafs && vbpt_cur_hdr(c1) != vbpt_cur_hdr(c2))
				return false;
		} else {
			assert(vbpt_cur_null(c1));
			assert(vbpt_cur_null(c2));
		}

		vbpt_cur_next(c1);
		vbpt_cur_next(c2);

		if (vbpt_cur_end(c1)) {
			if (!vbpt_cur_end(c2))
				return false;
			if (!vbpt_range_eq(&c1->range, &c2->range))
				return false;
			return true;
		}
	}
}

/**
 * mark the pointed node for deletion
 *  returns false if (for whatever reason) we are not able to delete node
 *  the node will be deleted in the subsequent vbpt_cur_next() call
 *  no cursor call are allowed, when a node is marked for deletion
 *  -- the caller should make sure that next() is called first.
 *
 * (note that the interface allows to mark something for deletion, and do it
 *  afterwards, as long as we are sure that we will be able to delete it)
 */
bool
vbpt_cur_mark_delete(vbpt_cur_t *c)
{
	assert(!vbpt_cur_null(c));
	vbpt_path_t *path = &c->path;
	vbpt_node_t *pnode = path->nodes[path->height -1];

	// if this is the last element, re-balancing is required.
	// Avoid this complex case (at least for now)
	if (pnode->items_nr == 1)
		return false;

	c->flags.deleteme = 1;
	return true;
}

/**
 *  if a node is marked for deletion, delete it
 */
static inline void
vbpt_cur_maybe_delete(vbpt_cur_t *cur)
{
	if (cur->flags.deleteme) {
		vbpt_delete_ptr(cur->tree, &cur->path, NULL);
		cur->flags.deleteme = 0;
	}
}

static uint16_t
vbpt_cur_height(const vbpt_cur_t *cur)
{
	assert(cur->tree->height >= cur->path.height);
	return cur->tree->height - cur->path.height;
}

/**
 * try to replace node pointed by @pc with node pointed by @gc, knowing that @gc
 * does not point to null
 *   if unsuccessful, return false
 *
 * old values that are replaced are decrefed
 */
static bool
vbpt_cur_do_replace(vbpt_cur_t *pc, const vbpt_cur_t *gc)
{
	assert(!vbpt_cur_null(gc));
	uint16_t p_height = vbpt_cur_height(pc);
	uint16_t g_height = vbpt_cur_height(gc);

	if (g_height > p_height) {
		printf("g_height > p_height: bailing out\n");
		return false; // TODO
	}

	uint16_t     p_key = pc->range.key + pc->range.len -1;
	vbpt_node_t *p_pnode = pc->path.nodes[pc->path.height - 1];
	uint16_t     p_pslot = pc->path.slots[pc->path.height - 1];
	vbpt_hdr_t  *p_hdr = NULL;
	if (vbpt_cur_null(pc)) {
		assert(p_pnode->items_nr <= p_pnode->items_total);
		if (p_pnode->items_nr == p_pnode->items_total) {
			printf("pc points to NULL and no room in node\n");
			return false;
		}
		// slot should be OK
	} else {
		p_hdr = vbpt_cur_hdr(pc);
	}


	vbpt_hdr_t  *g_hdr = vbpt_cur_hdr((vbpt_cur_t *)gc);
	vbpt_hdr_t *new_hdr = vbpt_hdr_getref(g_hdr);
	if (p_height > g_height) { // add a sufficiently large chain of nodes
		uint16_t levels = p_height - g_height;
		new_hdr = &(vbpt_node_chain(pc->tree, levels, p_key, new_hdr)->n_hdr);
	}

	vbpt_hdr_t *old_hdr = vbpt_insert_ptr(p_pnode, p_pslot, p_key, new_hdr);
	assert(old_hdr == p_hdr);
	if (p_hdr) {
		vbpt_hdr_putref(p_hdr);
	} else {
		assert(pc->null_max_key != 0);
		pc->null_max_key = 0;
	}

	return true;
}

/**
 * try to replace node pointed by @pc with node pointed by @gc
 *   if unsuccessful, return false
 */
bool
vbpt_cur_replace(vbpt_cur_t *pc, const vbpt_cur_t *gc)
{
	if (vbpt_cur_null(gc)) {
		return (vbpt_cur_null(pc) || vbpt_cur_mark_delete(pc));
	}
	return vbpt_cur_do_replace(pc, gc);
}

/**
 * compare two cursors
 */
bool
vbpt_cmp(vbpt_tree_t *t1, vbpt_tree_t *t2)
{
	vbpt_cur_t *c1 = vbpt_cur_alloc(t1);
	vbpt_cur_t *c2 = vbpt_cur_alloc(t2);
	bool ret = vbpt_cur_cmp(c1, c2, false);
	vbpt_cur_free(c1);
	vbpt_cur_free(c2);
	return ret;
}

/**
 * merge @ptree with @gtree -> result in @ptree
 */
bool
vbpt_log_merge(vbpt_txtree_t *gtree, vbpt_txtree_t *ptree)
{
	if (vbpt_log_conflict(gtree->tx_log, ptree->tx_log)) {
		printf("%s => CONFLICT\n", __FUNCTION__);
		return false;
	}
	vbpt_log_replay(ptree, gtree->tx_log);
	return true;
}


/**
 * helper function for merging when cursors are at the same range
 *  see vbpt_merge() for more details
 *
 * returns:
 *  -1 : if a conflict was detected
 *   0 : if more information is needed -- cursors need to move down
 *   1 : if merge was successful
 *
 *      |-            (jv)       -|
 *      |            /   \      p_dist
 *   g_dist         /     \       |
 *      |          /     (pv)    -|
 *      |-       (gv)
 */
static inline int
do_merge(const vbpt_cur_t *gc, vbpt_cur_t *pc,
        const vbpt_txtree_t *gtree, vbpt_txtree_t *ptree,
        ver_t *gv, ver_t  *pv, uint16_t g_dist, uint16_t p_dist, ver_t *jv)
{
	assert(vbpt_range_eq(&gc->range, &pc->range));
	ver_t *gc_v = vbpt_cur_ver(gc);
	ver_t *pc_v = vbpt_cur_ver(pc);
	//dmsg("range: %4lu,+%3lu\n\tgc_v:%s\n\tpc_v:%s\n",
	//      gc->range.key, gc->range.len, ver_str(gc_v), ver_str(pc_v));
	vbpt_log_t *plog = ptree->tx_log;
	vbpt_range_t *range = &pc->range;

	/*
         * (gc_v)------>|
         *              |
	 *             (jv)
	 *            /   \
	 *           /     \
	 *          /     (pv)
	 *        (gv)
	 */
	if (ver_leq_limit(gc_v, jv, g_dist)) {
		//printf("NO CHANGES in gc_v\n");
		return 1;
	}

	/*
         *              |<-----(pc_v)
         *              |
	 *             (jv)
	 *            /   \
	 *  (gc_v)-->/     \
	 *          /     (pv)
	 *        (gv)
	 */
	if (ver_leq_limit(pc_v, jv, p_dist)) {
		//printf("Only gc_v changed\n");
		// check if private tree read something that is under the
		// current (changed in the global tree) range. If it did,
		// it would read an older value, so we need to abort.
		if (vbpt_log_rs_range_exists(plog, range))
			return -1;
		// we need to effectively replace the node pointed by @pv with
		// the node pointed by @gc
		return vbpt_cur_replace(pc, gc) ? 1: -1;
	}

	/*
         *              |
	 *             (jv)
	 *            /   \<----(pc_v)
	 *  (gc_v)-->/     \
	 *          /     (pv)
	 *        (gv)
	 */
	 //printf("Both changed\n");

	// Handle NULL cases: NULL cases are special because we lack information
	// to precisely check for conflicts. For example, we can't go deeper --
	// all information on the tree is lost. It might be a good idea to make
	// (more) formal arguments about the correctness of the cases below
	if (vbpt_cur_null(pc) && vbpt_cur_null(gc)) {
		//printf("Both are NULL\n");
		// if both cursors point to NULL, there is a conflict if @gv has
		// read an item from the previous state, which may not have been
		// NULL.
		// NOTE: we could also check whether @glog contains a delete to
		// that range. However, for now we just use @plog for the merge,
		// which would be convenient for the distributed case
		return vbpt_log_rs_range_exists(plog, range) ? -1:1;
	} else if (vbpt_cur_null(pc)) {
		//printf("pc is NULL\n");
		// @pc points to NULL, but @gc does not. If @pv did not read or
		// delete anything in that range, we can replace @pc with @gc.
		if (vbpt_log_rs_range_exists(plog, range))
			return -1;
		if (vbpt_log_ds_range_exists(plog, range))
			return -1;
		//printf("trying to replace pc with gc\n");
		return vbpt_cur_replace(pc, gc) ? 1: -1;
	} else if (vbpt_cur_null(gc)) {
		//printf("gc is NULL\n");
		// @gc points  to NULL, but @pc does not. We can't just check
		// against the readset of @plog and keep the NULL: If @gc
		// deleted a key, that @pc still has due to COW (not because it
		// inserted it) we need to delete it.

		// special case: this is a leaf that we now has changed after
		// @vj -- we just keep it similarly to leaf checks
		if (range->len == 1 && !vbpt_log_rs_key_exists(plog, range->key))
			return 1;

		return -1;
	}

	assert(!vbpt_cur_null(gc) && !vbpt_cur_null(pc));
	if (range->len == 1) {
		return vbpt_log_rs_key_exists(plog, range->key) ? -1:1;
	}

	/* we need to go deeper */
	return 0;
}


/**
 * merge @ptree with @gtree -> result in @ptree
 *  @gtree: globally viewable version of the tree
 *  @ptree: transaction-private version of the tree
 *
 * The merging happens  *in-place* in @ptree. If successful, true is returned.
 * If not, false is returned, and @ptree is invalid.
 *
 * Assuming that @gver is the version of @gtree, and @pver is the version of
 * @ptree: If merge is successful, @pver should become @pver's ancestor (which
 * is the whole point of doing the merge: to make it appear as if the changes in
 * @pver happened after @gver). This has some implications (see Note below).
 *
 * The merge finds first common ancestor/join point of the two versions @vj. In
 * the general case the version tree might look like:
 *
 *        (vj)                  (vj)
 *       /    \                  |
 *     ...  (hpver)             ...
 *     /        \     ==>        |
 *  (gver)      ...            (gver)
 *                \              |
 *               (pver)        (hpver)
 *                               |
 *                              ...
 *                               |
 *                            (pver') == (nver)
 *
 * Note:
 * If the versions between vj (common ancestor) and mver have a refcount of 1,
 * we can just move the path (vj) -- (mver) under (tver). If not all refcounts
 * in the path are 1, there is a branched vresion somewhere in the path that is
 * not yet merged and if we move the path the join point might change. OTOH, if
 * we copy the path, we need to change all the versions in the tree nodes (or
 * provide some indirection), so that the versioning comparison will work for
 * future versions.
 * Hence, we choose to restrict merging so that all refcounts of versions from
 * @mver to @vj are 1. Note that this translates to requiring that a transaction
 * commits only after all of its nested transactions have comitted.
 */
bool
vbpt_merge(const vbpt_txtree_t *gtree, vbpt_txtree_t *ptree)
{
	vbpt_tree_t *gt = gtree->tx_tree;
	vbpt_tree_t *pt = ptree->tx_tree;
	//dmsg(""); vbpt_tree_print(gt, true);
	//dmsg(""); vbpt_tree_print(pt, true);

	vbpt_cur_t *gc = vbpt_cur_alloc(gt);
	vbpt_cur_t *pc = vbpt_cur_alloc(pt);

	uint16_t g_dist, p_dist;
	ver_t *hpver;
	ver_t *gver = gt->ver;
	ver_t *pver = pt->ver;
	ver_t *vj = ver_join(gver, pver, &hpver, &g_dist, &p_dist);
	//printf("VERSIONS: gver:%s  pver:%s  vj:%s\n", ver_str(gver), ver_str(pver), ver_str(vj));

	bool merge_ok = true;
	while (!vbpt_cur_end(gc) && !vbpt_cur_end(pc)) {
		vbpt_cur_sync(gc, pc);
		int ret = do_merge(gc, pc, gtree, ptree, gver, pver, g_dist, p_dist, vj);
		if (ret == -1) {
			merge_ok = false;
			goto end;
		} else if (ret == 0) {
			vbpt_cur_down(gc);
			vbpt_cur_down(pc);
		} else if (ret == 1) {
			vbpt_cur_next(gc);
			vbpt_cur_next(pc);
		} else assert(false && "This should never happen");
	}
	/* success: fix version tree */
	assert(!ver_chain_has_branch(pver, hpver));
	ver_setparent(hpver, gver);
end:
	vbpt_cur_free(gc);
	vbpt_cur_free(pc);
	return merge_ok;
}

void
vbpt_sync_test(vbpt_tree_t *t1, vbpt_tree_t *t2)
{
	vbpt_cur_t *c1 = vbpt_cur_alloc(t1);
	vbpt_cur_t *c2 = vbpt_cur_alloc(t2);

	for (size_t i=0; ; i++) {
		vbpt_cur_sync(c1, c2);
		printf("State: [iter:%zd]\n", i);
		printf("c1: "); vbpt_cur_print(c1);
		printf("c2: "); vbpt_cur_print(c2);

		if (vbpt_cur_end(c1) && vbpt_cur_end(c2)) {
			printf("     => both cursors reached the end\n");
			break;
		}
		if (vbpt_cur_ver(c1) == vbpt_cur_ver(c2)) {
			printf("     => same version\n");
			vbpt_cur_next(c1);
			vbpt_cur_next(c2);
		} else if (c1->range.len == 1) {
			assert(c2->range.len == 1);
			printf("     => reached bottom [VAL]\n");
			vbpt_cur_next(c1);
			vbpt_cur_next(c2);
		} else if (vbpt_cur_null(c1) || vbpt_cur_null(c2)) {
			printf("     => reached bottom [NULL]\n");
			vbpt_cur_next(c1);
			vbpt_cur_next(c2);
		} else {
			printf("     => need to go deeper\n");
			vbpt_cur_down(c1);
			vbpt_cur_down(c2);
		}
	}

	printf("End State: \n");
	printf("c1: "); vbpt_cur_print(c1);
	printf("c2: "); vbpt_cur_print(c2);
	vbpt_cur_free(c1);
	vbpt_cur_free(c2);
}

#if defined(VBPT_MERGE_TEST)

#if 0
static void __attribute__((unused))
ver_test(void)
{
	ver_t *v0 = ver_create();
	ver_t *v1 = ver_branch(v0);
	ver_t *v2 = ver_branch(v0);
	ver_t *v2a = ver_branch(v2);
	ver_t *x;

	assert(ver_join(v1, v2, &x) == v0 && x == v2);
	assert(ver_join(v1, v2a, &x) == v0 && x == v2);
}
#endif

int main(int argc, const char *argv[])
{
	vbpt_tree_t *t1 = vbpt_tree_create();
	vbpt_insert(t1, 42,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, t1->ver), NULL);
	vbpt_insert(t1, 100, vbpt_leaf_alloc(VBPT_LEAF_SIZE, t1->ver), NULL);
	assert(vbpt_cmp(t1, t1) == true);

	vbpt_txtree_t *txt1 = vbpt_txtree_alloc(t1);
	vbpt_txtree_insert(txt1, 66,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, txt1->tx_tree->ver), NULL);
	vbpt_txtree_insert(txt1, 99,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, txt1->tx_tree->ver), NULL);
	vbpt_log_finalize(txt1->tx_log);

	vbpt_txtree_t *txt2_a = vbpt_txtree_alloc(t1);
	vbpt_txtree_insert(txt2_a, 11,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, txt2_a->tx_tree->ver), NULL);

	vbpt_txtree_t *txt2_b = vbpt_txtree_alloc(t1);
	vbpt_txtree_insert(txt2_b, 11,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, txt2_b->tx_tree->ver), NULL);

	printf("MERGE:\n");
	if (vbpt_log_merge(txt1, txt2_b)) {
		printf("vbpt_log_merge(): SUCCESS\n");
	} else printf("FAILED\n");


	if (vbpt_merge(txt1, txt2_a)) {
		printf("vbpt_merge():     SUCCESS\n");
	} else printf("FAILED\n");

	vbpt_tree_print(txt2_a->tx_tree, true);
	vbpt_tree_print(txt2_b->tx_tree, true);
	printf("CMP=%u\n", vbpt_cmp(txt2_a->tx_tree, txt2_b->tx_tree));
	return 0;
}
#endif
