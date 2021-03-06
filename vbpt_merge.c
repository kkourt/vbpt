/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#include "ver.h"
#include "vbpt.h"
#include "vbpt_merge.h"
#include "vbpt_log.h"
#include "vbpt_tx.h"

#include "misc.h"
#include "tsc.h"

#define VBPT_KEY_MAX UINT64_MAX

//#define XDEBUG_MERGE

// forward declaration
static inline int vbpt_cur_maybe_delete(vbpt_cur_t *c);

/**
 * IDEAS:
 *  - Insert NULL for deletions
 */

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
 * If ->flags.null is 1, cursor is currently in a null range and its maximum
 * value is ->null_maxkey
 */

static inline char *
vbpt_cur_str(vbpt_cur_t *cur)
{
	#define CURSTR_BUFF_SIZE 128
	#define CURSTR_BUFFS_NR   16
	static int i=0;
	static char buff_arr[CURSTR_BUFFS_NR][CURSTR_BUFF_SIZE];
	char *buff = buff_arr[i++ % CURSTR_BUFFS_NR];
	snprintf(buff, CURSTR_BUFF_SIZE,
	         "cur: range:[%4lu+%4lu] null:%u null_max_key:%6lu v:%s",
	          cur->range.key, cur->range.len,
	          cur->flags.null, cur->null_maxkey,
	          vref_str(vbpt_cur_vref((vbpt_cur_t *)cur)));
	return buff;
	#undef CURSTR_BUFF_SIZE
	#undef CURSTR_BUFFS_NR
}

void
vbpt_cur_print(const vbpt_cur_t *cur)
{
	printf("cursor: range:[%4lu+%4lu] null:%u null_max_key:%4lu tree:%p %s\n",
	       cur->range.key, cur->range.len,
	       cur->flags.null, cur->null_maxkey,
	       cur->tree,
	       vref_str(vbpt_cur_vref((vbpt_cur_t *)cur)));
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

	vbpt_node_t *pnode = path->nodes[path->height - 1];
	uint16_t pslot = path->slots[path->height - 1];
	assert(pslot < pnode->items_nr);
	return pnode->kvp[pslot].val;
}

static const vbpt_range_t vbpt_range_full = {.key = 0, .len = VBPT_KEY_MAX};

void
vbpt_cur_init(vbpt_cur_t *cur, vbpt_tree_t *tree)
{
	cur->tree = tree;
	cur->path.height = 0;
	cur->range = vbpt_range_full;
	cur->null_maxkey = 0;
	cur->flags.deleteme = 0;
	cur->flags.null = 0;
}

/**
 * allocate and initialize a cursor
 */
vbpt_cur_t *
vbpt_cur_alloc(vbpt_tree_t *tree)
{
	vbpt_cur_t *ret = xmalloc(sizeof(vbpt_cur_t));
	vbpt_cur_init(ret, tree);
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
 * Return the vref of the node that the cursor points to
 *
 *  Note that NULL areas have the vref of the last node in the tree. This is a
 *  bit conservative because we can't distinguish if the NULL range was there
 *  before the last node's version or not.
 */
vref_t
vbpt_cur_vref(const vbpt_cur_t *cur)
{
	const vbpt_path_t *path = &cur->path;
	const vbpt_tree_t *tree = cur->tree;
	if (path->height == 0) {
		return vref_get__(tree->ver);
	} else if (!vbpt_cur_null(cur)) {
		return vbpt_cur_hdr((vbpt_cur_t *)cur)->vref;
	} else { // cursor points to NULL, return the version of the last node
		vbpt_node_t *pnode = path->nodes[path->height -1];
		return pnode->n_hdr.vref;
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
	//dmsg("IN  "); vbpt_cur_print(cur);

	// sanity checks
	assert(!vbpt_cur_null(cur) && "Can't go down: pointing to NULL");
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
		assert(node_key0 > cur->range.key);
		cur->range.len = node_key0 - cur->range.key + 1;
	} else if (node_key0 > cur->range.key) {
		// last-level, where we need to insert a NULL area
		cur->range.len = node_key0 - cur->range.key;
		cur->flags.null = 1;
		cur->null_maxkey = node_key0 - 1;
		path->slots[path->height -1] = 0;
	} else {
		// last-level, singular range pointing to a leaf
		cur->range.key = node_key0;
		cur->range.len = 1;
	}

	//dmsg("OUT  "); vbpt_cur_print(cur);
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
		if (vbpt_cur_null(cur))
			cur->range = *r;
		else
			vbpt_cur_down(cur);
	} while (vbpt_range_lt(r, &cur->range));
}

static void
vbpt_cur_verify(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;

	if (path->height == 0) {
		assert(cur->flags.null);
		assert(cur->null_maxkey = VBPT_KEY_MAX);
		return;
	}

	#if !defined(NDEBUG)
	uint16_t nslot = path->slots[path->height -1];
	vbpt_node_t *node = path->nodes[path->height -1];
	assert(nslot < node->items_nr);
	uint64_t node_key = node->kvp[nslot].key;
	if (cur->flags.null) {
		assert(node_key == cur->null_maxkey + 1);
	} else {
		assert(node_key == cur->range.key + cur->range.len - 1);
	}
	#endif
}

__attribute__((unused))
static void
vbpt_cur_next_verify(vbpt_cur_t *ocur, vbpt_cur_t *cur)
{
	if (cur->range.key != ocur->range.key + ocur->range.len) {
		fprintf(stderr,
		        "cur->range.key=%lu new_range_start=%lu\n",
		        cur->range.key, ocur->range.key + ocur->range.len);
		assert(false);
	}
	vbpt_cur_verify(cur);
}

#if !defined(NDEBUG)
# define CUR_NEXT_CHECK_BEGIN(cur) volatile vbpt_cur_t ocur__ =  *cur
# define CUR_NEXT_CHECK_END(cur)   vbpt_cur_next_verify((vbpt_cur_t *)&ocur__, cur)
#else
# define CUR_NEXT_CHECK_BEGIN(cur)
# define CUR_NEXT_CHECK_END(cur)
#endif

/**
 * the cursor is pointing to a leaf and is NULL
 */
static int
vbpt_cur_next_leaf_null(vbpt_cur_t *cur)
{
	CUR_NEXT_CHECK_BEGIN(cur);
	assert(!cur->flags.deleteme);
	uint64_t range_last_key = cur->range.key + cur->range.len - 1;
	assert(range_last_key <= cur->null_maxkey);

	// if the curent NULL range has not ended, just update the cursor to the
	// remaining NULL range.
	if (range_last_key < cur->null_maxkey) {
		cur->range.key = range_last_key + 1;
		cur->range.len = cur->null_maxkey - range_last_key;
		CUR_NEXT_CHECK_END(cur);
		return 0;
	}

	// The NULL range has ended
	#if !defined(NDEBUG)
	vbpt_path_t *path = &cur->path;
	assert(path->height > 0);
	uint16_t nslot = path->slots[path->height -1];
	vbpt_node_t *node = path->nodes[path->height -1];
	assert(nslot < node->items_nr);
	assert(node->kvp[nslot].key == range_last_key + 1);
	#endif
	cur->range.key = range_last_key + 1;
	cur->range.len = 1;
	cur->null_maxkey = 0;
	cur->flags.null = 0;

	CUR_NEXT_CHECK_END(cur);
	return 0;
}

static int
vbpt_cur_next_leaf_ascend(vbpt_cur_t *cur)
{
	CUR_NEXT_CHECK_BEGIN(cur);
	assert(!cur->flags.deleteme);
	vbpt_path_t *path = &cur->path;
	assert(path->height > 0);
	if (--path->height == 0) {
		cur->range.key = cur->range.key + cur->range.len;
		cur->range.len = VBPT_KEY_MAX - cur->range.key;
		cur->flags.null = 1;
		cur->null_maxkey = VBPT_KEY_MAX;
		CUR_NEXT_CHECK_END(cur);
		return 0;
	} else {
		return vbpt_cur_next(cur);
	}
}

/**
* the cursor is pointing to a leaf, move to the next node
*/
static int
vbpt_cur_next_leaf(vbpt_cur_t *cur)
{
	if (vbpt_cur_null(cur))
		return vbpt_cur_next_leaf_null(cur);

	vbpt_path_t *path = &cur->path;
	assert(path->height > 0);
	vbpt_node_t *n = path->nodes[path->height -1];
	uint16_t nslot = path->slots[path->height -1];
	assert(nslot < n->items_nr);
	assert(vbpt_cur_hdr(cur)->type == VBPT_LEAF);
	assert(cur->range.len == 1);
	assert(cur->range.key == n->kvp[nslot].key);

	// no more space in this node, need to move up
	if (nslot + 1 == n->items_nr)
		return vbpt_cur_next_leaf_ascend(cur);

	CUR_NEXT_CHECK_BEGIN(cur);
	uint64_t next_key = n->kvp[nslot+1].key;
	int del = vbpt_cur_maybe_delete(cur);
	uint16_t next_slot = nslot + 1 - del;
	assert(n->kvp[next_slot].key == next_key);
	if (next_key == cur->range.key + 1) {
		// if the current and next key are sequential, we can just move
		// to the next key
		path->slots[path->height - 1] = next_slot;
		cur->range.key = n->kvp[next_slot].key;
		cur->range.len = 1;
		CUR_NEXT_CHECK_END(cur);
	} else {
		// if there is range space between the current and the next key,
		// it is NULL space and we need to account for it. Note that
		// nslot for null values points to the next node
		path->slots[path->height - 1] = next_slot;
		cur->flags.null = 1;
		cur->null_maxkey = next_key - 1;
		cur->range.key += 1;
		cur->range.len = next_key - cur->range.key;
		CUR_NEXT_CHECK_END(cur);
	}

	return 0;
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
	vbpt_path_t *path = &cur->path;
	//dmsg("IN  "); vbpt_cur_print(cur);

	if (hdr->type == VBPT_LEAF) {
		return vbpt_cur_next_leaf(cur);
	}

	CUR_NEXT_CHECK_BEGIN(cur);
	if (path->height == 0 && vbpt_cur_null(cur)) {
		assert(hdr->type == VBPT_NODE);
		assert(cur->null_maxkey == VBPT_KEY_MAX);
		cur->range.key += cur->range.len;
		cur->range.len = VBPT_KEY_MAX - cur->range.key;
		CUR_NEXT_CHECK_END(cur);
		return 0;
	}

	while (true) {
		vbpt_node_t *n = path->nodes[path->height -1];
		uint16_t nslot = path->slots[path->height -1];
		if (nslot + 1 < n->items_nr) {
			assert(nslot < n->items_nr);
			uint64_t next_key   = n->kvp[nslot+1].key;
			uint64_t old_high_k = n->kvp[nslot].key;
			int del             = vbpt_cur_maybe_delete(cur);
			uint16_t next_slot  = nslot + 1 - del;
			assert(n->kvp[next_slot].key == next_key);
			path->slots[path->height -1] = next_slot;
			cur->range.key = old_high_k + 1;
			cur->range.len = next_key - cur->range.key + 1;
			break;
		}

		assert(!cur->flags.deleteme);
		if (--path->height == 0) {
			cur->range.key += cur->range.len;
			cur->range.len = VBPT_KEY_MAX - cur->range.key;
			cur->flags.null = 1;
			cur->null_maxkey = VBPT_KEY_MAX;
			break;
		}
	}

	CUR_NEXT_CHECK_END(cur);
	return 0;
}

/**
 * check if cursor has reached the end of the tree
 */
bool
vbpt_cur_end(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	return (path->height == 0 && cur->null_maxkey == VBPT_KEY_MAX);
}

/**
 * synchronize two cursors
 */
void
vbpt_cur_sync(vbpt_cur_t *cur1, vbpt_cur_t *cur2)
{
	assert(!cur1->flags.deleteme && !cur2->flags.deleteme);
	if (cur1->range.key != cur2->range.key) {
		vbpt_cur_print(cur1);
		vbpt_cur_print(cur2);
		assert(cur1->range.key == cur2->range.key);
	}
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
			cs = cb = NULL; // shut the compiler up
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

		if (!vbpt_range_eq(&c1->range, &c2->range)) {
			//printf("got different ranges:\n");
			//vbpt_cur_print(c1);
			//vbpt_cur_print(c2);
			return false;
		}

		if (c1->range.len == 1) {
			assert(c2->range.len == 1);
			if (check_leafs && vbpt_cur_hdr(c1) != vbpt_cur_hdr(c2)) {
				//printf("leafs are not the same\n");
				return false;
			}
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
vbpt_cur_mark_delete(vbpt_cur_t *c, ver_t *jv, uint16_t p_dist)
{
	assert(!vbpt_cur_null(c));
	vbpt_path_t *path = &c->path;
	vbpt_node_t *pnode = path->nodes[path->height -1];

	// Is COW needed? (parent has to be strictly after jv)
	// TODO: verify this is correct via a test/better rationale
	vref_t pvref = pnode->n_hdr.vref;
	if (vref_ancestor_limit(pvref, jv, p_dist)) {
		return -1;
	}
	assert(refcnt_get(&pnode->n_hdr.h_refcnt) == 1);

	// if this is the last element, re-balancing is required.
	// Avoid this complex case (at least for now)
	if (pnode->items_nr == 1) {
		printf("mark_delete failed\n");
		return false;
	}

	// this is another case we don't handle
	assert(pnode->items_nr > 0);
	if (pnode->items_nr -1 == path->slots[path->height-1]) {
		printf("mark_delete failed\n");
		return false;
	}

	c->flags.deleteme = 1;
	return true;
}

/**
 *  if a node is marked for deletion, delete it
 */
static inline int
vbpt_cur_maybe_delete(vbpt_cur_t *cur)
{
	if (cur->flags.deleteme) {
		vbpt_delete_ptr(cur->tree, &cur->path, NULL);
		cur->flags.deleteme = 0;
		return 1;
	}

	return 0;
}

static uint16_t
vbpt_cur_height(const vbpt_cur_t *cur)
{
	assert(cur->tree->height >= cur->path.height);
	return cur->tree->height - cur->path.height;
}

/*
 * get key for current node
 */
static uint64_t __attribute__((unused))
vbpt_cur_nodekey(const vbpt_cur_t *cur)
{
	assert(cur->path.height > 0); // not sure what we should return here
	uint16_t pidx  = cur->path.height - 1;
	uint16_t pslot = cur->path.slots[pidx];
	return           cur->path.nodes[pidx]->kvp[pslot].key;
}

/**
 * Helper structure for merge
 * (see bellow for more details)
 *
 *      |-            (vj)       -|
 *      |            /   \      p_dist
 *   g_dist         /     \       |
 *      |          /     (pver)  -|
 *      |-       (gver)
 */
struct vbpt_merge {
	ver_t   *vj;
	ver_t   *gver;
	ver_t   *pver;
	ver_t   *hpver;
	uint16_t p_dist;
	uint16_t g_dist;
};

/**
 * try to replace node pointed by @pc with node pointed by @gc, knowing that @gc
 * does not point to null
 *   if unsuccessful, return false
 *
 * old values that are replaced are decrefed
 * @pc could point to NULL. In that case, the item is inserted only if there are
 * available items on the parent node.
 *
 * We need to (potentially) update the path:
 *  - if we add a chain of nodes, we need to add them to the path
 *  - we need to remove flags.null, since now we have a node
 *
 * XXX: Should we check for COW? -- yes
 * XXX: Think about versioning issues -- i.e., two nodes on the tree P and C,
 * where P points to C then ver(P) > ver(C).
 * XXX: Clean this up
 */
static bool
vbpt_cur_do_replace(vbpt_cur_t *pc, const vbpt_cur_t *gc,
                    struct vbpt_merge merge)
{
	assert(!vbpt_cur_null(gc));
	uint16_t p_height = vbpt_cur_height(pc);
	uint16_t g_height = vbpt_cur_height(gc);
	vbpt_hdr_t *g_hdr = vbpt_cur_hdr((vbpt_cur_t *)gc);
	assert(vbpt_cur_hdr(pc) != g_hdr);

	if (g_height > p_height) {
		//printf("g_height > p_height: bailing out\n");
		return false; // TODO
	}

	// find the key for the new node
	uint64_t p_key = pc->range.key + pc->range.len - 1;
	assert(vbpt_cur_nodekey(gc) == p_key);
	assert(vbpt_cur_null(pc) || vbpt_cur_nodekey(pc) == p_key);

	vbpt_node_t *p_pnode;
	uint16_t     p_pslot;
	if (pc->path.height == 0) {
		// we are at the end, we need to add a new item to the root node
		p_pnode = pc->tree->root;
		p_pslot = pc->tree->root->items_nr;
		p_height = 0;
	} else {
		p_pnode = pc->path.nodes[pc->path.height - 1];
		p_pslot = pc->path.slots[pc->path.height - 1];
	}

	// we are going to modify p_pnode, check if COW is needed
	// (parent has to be strictly after vj)
	// TODO: verify this is correct via a test/better rationale
	vref_t p_pvref = p_pnode->n_hdr.vref;
	if (!vref_ancestor_limit(p_pvref, merge.pver, merge.p_dist-1)) {
		return false;
	}
	assert(refcnt_get(&p_pnode->n_hdr.h_refcnt) == 1);

	vbpt_hdr_t *p_hdr = NULL; // this is the item we are going to replace
	if (vbpt_cur_null(pc)) {
		assert(p_pnode->items_nr <= p_pnode->items_total);
		if (p_pnode->items_nr == p_pnode->items_total) {
			//printf("pc points to NULL, and no room in node\n");
			return false;
		}
		// slot should be OK
	} else if (pc->path.height != 0) {
		p_hdr = vbpt_cur_hdr(pc);
	}

	#if 0
	// XXX: I think the tests below are not needed since we are replacing
	// the same range and the key won't change
	//
	// in the following cases, the rightmost value will change, and we would
	// need to update the parent chain. For now we just bail out
	if ( p_hdr && p_pslot == p_pnode->items_nr -1 && pc->path.height != 0)
		return false;
	if (!p_hdr && p_pslot == p_pnode->items_nr    && pc->path.height != 0)
		return false;
	#endif

	assert(g_hdr != p_hdr);
	vbpt_hdr_t *new_hdr = vbpt_hdr_getref(g_hdr);
	if (p_height > g_height) { // add a sufficiently large chain of nodes
		uint16_t levels = p_height - g_height;
		new_hdr = &(vbpt_node_chain(pc->tree, levels, p_key, new_hdr)->n_hdr);
		assert(false && "need to fix the path");
		abort();
	}

	vbpt_hdr_t *old_hdr __attribute__((unused));
	old_hdr = vbpt_insert_ptr(p_pnode, p_pslot, p_key, new_hdr);
	assert(old_hdr == p_hdr);
	if (p_hdr) {
		//VBPT_MERGE_START_TIMER(cur_do_replace_putref);
		vbpt_hdr_putref(p_hdr);
		//VBPT_MERGE_STOP_TIMER(cur_do_replace_putref);
	} else {
		assert(vbpt_cur_null(pc));
		if (pc->path.height != 0) {
			pc->null_maxkey = 0;
			pc->flags.null = 0;
		} else {
			assert(false && "Need to fix the path?");
			abort();
		}
	}

	assert(pc->flags.null == 0);
	assert(vbpt_cur_hdr(pc) == new_hdr);
	return true;
}

/**
 * try to replace node pointed by @pc with node pointed by @gc
 *   if unsuccessful, return false
 */
bool
vbpt_cur_replace(vbpt_cur_t *pc, const vbpt_cur_t *gc,
                 struct vbpt_merge merge)
{
	//VBPT_MERGE_START_TIMER(cur_replace);
	//dmsg("REPLACE: "); vbpt_cur_print(pc);
	//dmsg("WITH:    "); vbpt_cur_print(gc);
	//tmsg("REPLACE %s WITH %s\n", vbpt_cur_str(pc), vbpt_cur_str((vbpt_cur_t *)gc));
	bool ret;
	if (vbpt_cur_null(gc)) {
		ret = (vbpt_cur_null(pc)
		       || vbpt_cur_mark_delete(pc, merge.vj, merge.p_dist));
	} else {
		tsc_t t2; tsc_init(&t2); tsc_start(&t2);
		//VBPT_MERGE_START_TIMER(cur_do_replace);
		ret = vbpt_cur_do_replace(pc, gc, merge);
		//VBPT_MERGE_STOP_TIMER(cur_do_replace);
	}
	//VBPT_MERGE_STOP_TIMER(cur_replace);
	return ret;
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
vbpt_log_merge(vbpt_tree_t *gtree, vbpt_tree_t *ptree)
{
	vbpt_log_t *g_log = vbpt_tree_log(gtree);
	vbpt_log_t *p_log = vbpt_tree_log(ptree);

	uint16_t g_dist, p_dist;
	ver_t *hpver = NULL; // initialize to shut the compiler up
	ver_join(gtree->ver, ptree->ver, &hpver, &g_dist, &p_dist);

	if (vbpt_log_conflict(g_log, g_dist, p_log, p_dist)) {
		printf("%s => CONFLICT\n", __FUNCTION__);
		return false;
	}
	vbpt_log_replay(ptree, g_log, g_dist);
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
 */
static int
do_merge(const vbpt_cur_t *gc, vbpt_cur_t *pc,
        const vbpt_tree_t *gtree, vbpt_tree_t *ptree,
	struct vbpt_merge merge)
{
	VBPT_MERGE_INC_COUNTER(merge_steps);
	assert(vbpt_range_eq(&gc->range, &pc->range));
	vref_t gc_vref = vbpt_cur_vref(gc);
	vref_t pc_vref = vbpt_cur_vref(pc);
	#if defined(XDEBUG_MERGE)
	tmsg("range: %4lu,+%3lu gc_vref:%s \tpc_vref:%s\n",
	      gc->range.key, gc->range.len,
	      vref_str(gc_vref), vref_str(pc_vref));
	#endif
	assert(merge.g_dist > 0);
	assert(merge.p_dist > 0);
	vbpt_log_t *plog = vbpt_tree_log(ptree);
	vbpt_log_t *glog = vbpt_tree_log((vbpt_tree_t *)gtree);
	vbpt_range_t *range = &pc->range;

	/*
	 * (gc_vref)--->|
	 *              |
	 *             (vj)
	 *            /   \
	 *           /     \
	 *          /     (pver)
	 *        (gver)
	 */
	if (!vref_ancestor_limit(gc_vref, merge.gver, merge.g_dist - 1)) {
		#if defined(XDEBUG_MERGE)
		dmsg("NO CHANGES in gc_v\n");
		#endif
		VBPT_MERGE_INC_COUNTER(gc_old);
		return 1;
	}

	/*
	 *                |<-----(pc_vref)
	 *                |
	 *               (vj)
	 *              /   \
	 * (gc_vref)-->/     \
	 *            /     (pver)
	 *          (gver)
	 */
	if (!vref_ancestor_limit(pc_vref, merge.pver, merge.p_dist - 1)) {
		#if defined(XDEBUG_MERGE)
		dmsg("Only gc_v changed\n");
		#endif
		VBPT_MERGE_INC_COUNTER(pc_old);
		// check if private tree read something that is under the
		// current (changed in the global tree) range. If it did,
		// it would read an older value, so we need to abort.
		if (vbpt_log_rs_range_exists(plog, range, merge.p_dist)) {
			return -1;
		}
		//assert(pc_v != gc_v);
		// we need to effectively replace the node pointed by @pv with
		// the node pointed by @gc
		return vbpt_cur_replace(pc, gc, merge) ? 1: -1;
	}

	/*
	 *                 |
	 *                (vj)
	 *               /   \<----(pc_vref)
	 *  (gc_vref)-->/     \
	 *             /     (pver)
	 *           (gver)
	 */
	 #if defined(XDEBUG_MERGE)
	 dmsg("Both changed\n");
	 //dmsg("base: %s\n", ver_str(merge.vj));
	 dmsg("pc:"); vbpt_cur_print(pc);
	 dmsg("gc:"); vbpt_cur_print(gc);
	 dmsg("\n");
	 #endif

	// Handle NULL cases: NULL cases are special because we lack information
	// to precisely check for conflicts. For example, we can't go deeper --
	// all information on the tree is lost. It might be a good idea to make
	// (more) formal arguments about the correctness of the cases below
	if (vbpt_cur_null(pc) && vbpt_cur_null(gc)) {
		VBPT_MERGE_INC_COUNTER(both_null);
		#if defined(XDEBUG_MERGE)
		dmsg("Both are NULL\n");
		#endif
		// if both cursors point to NULL, there is a conflict if @gv has
		// read an item from the previous state, which may not have been
		// NULL.  We could also check whether @glog contains a delete to
		// that range.
		return vbpt_log_rs_range_exists(plog,
		                                range,
		                                merge.p_dist) ? -1 : 1;
	} else if (vbpt_cur_null(pc)) {
		VBPT_MERGE_INC_COUNTER(pc_null);
		#if defined(XDEBUG_MERGE)
		dmsg("pc is NULL\n");
		#endif
		// @pc points to NULL, but @gc does not. If @pv did not read or
		// delete anything in that range, we can replace @pc with @gc.
		if (vbpt_log_rs_range_exists(plog, range, merge.p_dist))
			return -1;
		if (vbpt_log_ds_range_exists(plog, range, merge.p_dist))
			return -1;
		//printf("trying to replace pc with gc\n");
		return vbpt_cur_replace(pc, gc, merge) ? 1: -1;
	} else if (vbpt_cur_null(gc)) {
		VBPT_MERGE_INC_COUNTER(gc_null);
		#if defined(XDEBUG_MERGE)
		dmsg("gc is NULL\n");
		#endif
		// @gc points  to NULL, but @pc does not. We can't just check
		// against the readset of @plog and keep the NULL: If @gc
		// deleted a key, that @pc still has due to COW (not because it
		// inserted it) we would need to delete it.

		// Since, however, @gc points to NULL, if there are no deletions
		// in Log(J->G), then @vj pointed to NULL for gc->range. Hence,
		// if there are no Reads in Log(J->P), there is no conflict.
		if (!vbpt_log_ds_range_exists(glog, range, merge.g_dist) &&
		    !vbpt_log_rs_range_exists(plog, range, merge.p_dist)) {
			return 1;
		}

		// special case: this is a leaf that we now has changed after
		// @vj -- we just keep it similarly to leaf checks
		if (range->len == 1 &&
		    !vbpt_log_rs_key_exists(plog, range->key, merge.p_dist))
			return 1;

		//printf("gc is null, I ran out of options\n");
		return -1;
	}

	assert(!vbpt_cur_null(gc) && !vbpt_cur_null(pc));
	if (range->len == 1) {
		return vbpt_log_rs_key_exists(plog,
		                              range->key,
		                              merge.p_dist) ? -1 : 1;
	}

	/* we need to go deeper */
	return 0;
}

/**
 * merge @ptree with @gtree -> result in @ptree
 *  @gtree: globally viewable version of the tree
 *  @ptree: transaction-private version of the tree
 *
 * Note that this code assumes that ver_rebase_prepare() has been called on
 * gtree->ver
 *
 * The merging happens  *in-place* in @ptree. If successful, true is returned.
 * If not, false is returned, and @ptree is invalid.
 *
 * If successful and @vbase not NULL, @hpver is placed in @vbase
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
 *
 * TODO: check for invalid merges (e.g., when no merge is needed)
 */
bool
vbpt_merge(const vbpt_tree_t *gt, vbpt_tree_t *pt, ver_t  **vbase)
{
	VBPT_MERGE_START_TIMER(vbpt_merge);

	#if defined(XDEBUG_MERGE)
	dmsg("Global  "); vbpt_tree_print_limit((vbpt_tree_t *)gt, true, 1);
	dmsg("Private "); vbpt_tree_print_limit(pt, true, 1);
	#endif

	//VBPT_MERGE_START_TIMER(cur_init);
	vbpt_cur_t gc, pc;
	vbpt_cur_init(&gc, (vbpt_tree_t *)gt);
	vbpt_cur_init(&pc, pt);
	bool merge_ok;
	//VBPT_MERGE_STOP_TIMER(cur_init);

	struct vbpt_merge merge;
	merge.gver = gt->ver;
	merge.pver = pt->ver;
	//VBPT_MERGE_START_TIMER(ver_join);
	merge.vj = ver_join(merge.gver, merge.pver,
	                    &merge.hpver,
	                    &merge.g_dist, &merge.p_dist);
	//VBPT_MERGE_STOP_TIMER(ver_join);
	if (merge.vj == VER_JOIN_FAIL) {
		VBPT_MERGE_INC_COUNTER(join_failed);
		goto fail;
	}
	#if defined(XDEBUG_MERGE)
	dmsg("VERSIONS: gver:%s  pver:%s  vj:%s g_dist:%d p_dist:%d\n",
	      ver_str(merge.gver), ver_str(merge.pver), ver_str(merge.vj),
	      merge.g_dist, merge.p_dist);
	#endif

	//unsigned steps = 0;
	while (!(vbpt_cur_end(&gc) && vbpt_cur_end(&pc))) {
		//VBPT_MERGE_START_TIMER(cur_sync);
		assert(vbpt_path_verify((vbpt_tree_t *)gt, &gc.path));
		assert(vbpt_path_verify(pt, &pc.path));
		vbpt_cur_sync(&gc, &pc);
		//VBPT_MERGE_STOP_TIMER(cur_sync);

		VBPT_MERGE_START_TIMER(do_merge);
		int ret = do_merge(&gc, &pc, gt, pt, merge);
		VBPT_MERGE_STOP_TIMER(do_merge);
		if (ret == -1) {
			goto fail;
		} else if (ret == 0) {
			//VBPT_MERGE_START_TIMER(cur_down);
			vbpt_cur_down(&gc);
			vbpt_cur_down(&pc);
			//VBPT_MERGE_STOP_TIMER(cur_down);
		} else if (ret == 1) {
			//VBPT_MERGE_START_TIMER(cur_next);
			vbpt_cur_next(&gc);
			vbpt_cur_next(&pc);
			//VBPT_MERGE_STOP_TIMER(cur_next);
		} else assert(false && "This should never happen");

		/*
		if (steps++ > 32) {
			merge_ok = false;
			goto fail;
		}
		*/
	}

	/* success: fix version tree */
	merge_ok = true;
	//VBPT_MERGE_START_TIMER(ver_rebase);
	assert(!ver_chain_has_branch(merge.pver, merge.hpver));
	ver_rebase_commit(merge.hpver, merge.gver);
	if (vbase)
		*vbase = merge.gver;
	assert(ver_ancestor(merge.gver, merge.pver));
	assert(ver_ancestor(merge.gver, merge.hpver));
	//VBPT_MERGE_STOP_TIMER(ver_rebase);
end:
	#if defined(XDEBUG_MERGE)
	dmsg("MERGE %s\n", merge_ok ? "SUCCEEDED":"FAILED");
	#endif

	#if 0
	if (merge_ok)
		VBPT_INC_COUNTER(merge_ok);
	else
		VBPT_INC_COUNTER(merge_fail);
	#endif
	VBPT_MERGE_STOP_TIMER(vbpt_merge);
	return merge_ok;
fail:
	merge_ok = false;
	ver_rebase_abort(merge.gver);
	goto end;
}

#if defined(VBPT_SYNC_TEST)
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
#endif
