
#include "ver.h"
#include "vbpt.h"
#include "vbpt_merge.h"
#include "vbpt_log.h"
#include "vbpt_tx.h"

#include "misc.h"
#include "tsc.h"

#define VBPT_KEY_MAX UINT64_MAX

//#define XDEBUG_MERGE
static __thread vbpt_merge_stats_t MergeStats = {0};

// forward declaration
static inline void vbpt_cur_maybe_delete(vbpt_cur_t *c);

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
	       "C: range:[%4lu+%4lu] null:%u null_max_key:%4lu v:%s",
	       cur->range.key, cur->range.len,
	       cur->flags.null, cur->null_maxkey,
	       ver_str(vbpt_cur_ver((vbpt_cur_t *)cur)));
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
	} else if (!vbpt_cur_null(cur)) {
		return vbpt_cur_hdr((vbpt_cur_t *)cur)->ver;
	} else { // cursor points to NULL, return the version of the last node
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
		assert(node_key0 - cur->range.key);
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

	if (vbpt_cur_null(cur)) {
		assert(!cur->flags.deleteme);
		// cursor pointing to NULL already:
		uint64_t r_last = cur->range.key + cur->range.len - 1;
		assert(r_last <= cur->null_maxkey);
		if (r_last < cur->null_maxkey) {
			// if the curent NULL range has not ended, just update
			// the cursor to the remaining NULL range.
			cur->range.key = r_last + 1;
			cur->range.len = cur->null_maxkey - r_last;
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
			cur->null_maxkey = 0;
			cur->flags.null = 0;
			goto next_key;
		}
	} else if (nslot + 1 == n->items_nr) {
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
			cur->flags.null = 1;
			cur->null_maxkey = next_key - 1;
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
		cur->range.key = cur->range.key + cur->range.len;
		cur->range.len = VBPT_KEY_MAX - cur->range.key;
		cur->flags.null = 1;
		cur->null_maxkey = VBPT_KEY_MAX;
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
	vbpt_path_t *path = &cur->path;
	int ret = 0;
	//dmsg("IN  "); vbpt_cur_print(cur);
	#if !defined(NDEBUG)
	vbpt_cur_t oldcur = *cur;
	enum vbpt_type oldtype = hdr->type;
	#endif
	if (hdr->type == VBPT_LEAF) {
		ret = vbpt_cur_next_leaf(cur);
		goto end;
	} else if (path->height == 0 && vbpt_cur_null(cur)) {
		assert(hdr->type == VBPT_NODE);
		assert(cur->null_maxkey == VBPT_KEY_MAX);
		cur->range.key += cur->range.len;
		cur->range.len = VBPT_KEY_MAX - cur->range.key;
		goto end;
	}

	while (true) {
		vbpt_node_t *n = path->nodes[path->height -1];
		uint16_t nslot = path->slots[path->height -1];
		assert(nslot < n->items_nr);
		vbpt_cur_maybe_delete(cur);
		if (nslot + 1 < n->items_nr) {
			path->slots[path->height -1] = nslot + 1;
			uint64_t old_high_k = n->kvp[nslot].key;
			cur->range.key = old_high_k + 1;
			cur->range.len = n->kvp[nslot+1].key - cur->range.key + 1;
			break;
		}

		if (--path->height == 0) {
			cur->range.key += cur->range.len;
			cur->range.len = VBPT_KEY_MAX - cur->range.key;
			cur->flags.null = 1;
			cur->null_maxkey = VBPT_KEY_MAX;
			break;
		}
	}

end:
	//dmsg("OUT "); vbpt_cur_print(cur);
	#if !defined(NDEBUG)
	if (cur->range.key != oldcur.range.key + oldcur.range.len) {
		fprintf(stderr,
		        "cur->range.key=%lu new_range_start=%lu [oldtype=%d]\n",
		        cur->range.key, oldcur.range.key + oldcur.range.len,
		        oldtype);
		assert(false);
	}
	#endif
	return ret;
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
	if (pnode->items_nr == 1) {
		printf("mark_delete failed\n");
		return false;
	}

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
                    ver_t *jv, uint16_t p_dist)
{
	assert(!vbpt_cur_null(gc));
	uint16_t p_height = vbpt_cur_height(pc);
	uint16_t g_height = vbpt_cur_height(gc);
	assert(vbpt_cur_hdr(pc) != vbpt_cur_hdr((vbpt_cur_t *)gc));

	if (g_height > p_height) {
		//printf("g_height > p_height: bailing out\n");
		return false; // TODO
	}

	uint64_t p_key = pc->range.key + pc->range.len - 1;

	#if !defined(NDEBUG)
	assert(gc->path.height > 0);
	vbpt_node_t *p_gnode = gc->path.nodes[gc->path.height - 1];
	uint16_t p_gslot = gc->path.slots[gc->path.height - 1];
	uint64_t g_key = p_gnode->kvp[p_gslot].key;
	if (g_key != p_key) {
		vbpt_tree_print(gc->tree, false);
		vbpt_tree_print(pc->tree, false);
		vbpt_cur_print(gc);
		vbpt_cur_print(pc);
		dmsg("g_key=%lu p_key=%lu\n", g_key, p_key);
		assert(g_key == p_key);
	}
	#endif

	vbpt_node_t *p_pnode;
	uint16_t     p_pslot;
	vbpt_hdr_t  *p_hdr = NULL;
	if (pc->path.height == 0) {
		// we are at the end, we need to add a new item to the root node
		p_pnode = pc->tree->root;
		p_pslot = pc->tree->root->items_nr;
		p_height = 0;
	} else {
		p_pnode = pc->path.nodes[pc->path.height - 1];
		p_pslot = pc->path.slots[pc->path.height - 1];
	}

	// check if COW is needed
	ver_t *p_pver = p_pnode->n_hdr.ver;
	if (!ver_ancestor_strict_limit(jv, p_pver, p_dist)) {
		return -1;
	}
	assert(refcnt_get(&p_pnode->n_hdr.h_refcnt) == 1);

	if (vbpt_cur_null(pc)) {
		assert(p_pnode->items_nr <= p_pnode->items_total);

		if (p_pnode->items_nr == p_pnode->items_total) {
			//printf("pc points to NULL, and no room in node\n");
			return false;
		}

		// slot should be OK
	} else {
		if (pc->path.height != 0)
			p_hdr = vbpt_cur_hdr(pc);
	}

	// in this case, the rightmost value changes, so we need to update the
	// parent chain, which we don't currently do.
	if (p_pslot == p_pnode->items_nr && pc->path.height != 0) {
		return false;
	}

	vbpt_hdr_t  *g_hdr = vbpt_cur_hdr((vbpt_cur_t *)gc);
	assert(g_hdr != p_hdr);
	vbpt_hdr_t *new_hdr = vbpt_hdr_getref(g_hdr);
	if (p_height > g_height) { // add a sufficiently large chain of nodes
		uint16_t levels = p_height - g_height;
		new_hdr = &(vbpt_node_chain(pc->tree, levels, p_key, new_hdr)->n_hdr);
		assert(false && "need to fix the path");
	}

	vbpt_hdr_t *old_hdr __attribute__((unused));
	old_hdr = vbpt_insert_ptr(p_pnode, p_pslot, p_key, new_hdr);
	assert(old_hdr == p_hdr);
	if (p_hdr) {
		tsc_t t2; tsc_init(&t2); tsc_start(&t2);
		vbpt_hdr_putref(p_hdr);
		tsc_pause(&t2); MergeStats.cur_do_replace_putref_ticks += tsc_getticks(&t2);
	} else {
		assert(vbpt_cur_null(pc));
		if (pc->path.height != 0) {
			pc->null_maxkey = 0;
			pc->flags.null = 0;
		} else {
			assert(false && "Need to fix the path?");
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
                 ver_t *jv, uint16_t p_dist)
{
	tsc_t t; tsc_init(&t); tsc_start(&t);
	//dmsg("REPLACE: "); vbpt_cur_print(pc);
	//dmsg("WITH:    "); vbpt_cur_print(gc);
	//tmsg("REPLACE %s WITH %s\n", vbpt_cur_str(pc), vbpt_cur_str((vbpt_cur_t *)gc));
	bool ret;
	if (vbpt_cur_null(gc)) {
		ret = (vbpt_cur_null(pc) || vbpt_cur_mark_delete(pc));
	} else {
		tsc_t t2; tsc_init(&t2); tsc_start(&t2);
		ret = vbpt_cur_do_replace(pc, gc, jv, p_dist);
		tsc_pause(&t2); MergeStats.cur_do_replace_ticks += tsc_getticks(&t2);
		MergeStats.cur_do_replace_count++;
	}

	tsc_pause(&t); MergeStats.cur_replace_ticks += tsc_getticks(&t);
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


void vbpt_merge_stats_get(vbpt_merge_stats_t *stats)
{
	*stats = MergeStats;
}

void vbpt_merge_stats_do_report(char *prefix, vbpt_merge_stats_t *st)
{
	#define pr_stat(x__) \
		printf("%s" # x__ ": %lu\n", prefix, st->x__)

	#define pr_merge_ratio(x__) \
		printf("%s" # x__ ": %lu  " #x__ "/merge: %.2lf\n", \
		       prefix, st->x__, (double)st->x__ / (double)st->merges)

	#define pr_ticks(x__) do { \
		double p__ = (double)st->x__ / (double)st->merge_ticks; \
		if (p__ < 0.1) \
			break; \
		printf("%s" # x__ ": %lu (%.2lf%%)\n", prefix, st->x__, p__); \
	} while (0)

	#define pr_ticks2(ticks__, cnt__) do { \
		unsigned long t__ = st->ticks__;                   \
		double p__ =  t__ / (double)st->merge_ticks;       \
		if (p__ < 0.1)                                     \
			break;                                     \
		unsigned long c__ = st->cnt__;                     \
		unsigned long a__ = t__ / c__;                     \
		printf("%s" # ticks__ ": %lu (%.2lf%%) cnt:%lu (%lu ticks/call)\n",\
		        prefix, t__, p__, c__, a__);               \
	} while (0)

	pr_stat(join_failed);
	pr_stat(gc_old);
	pr_stat(pc_old);
	pr_stat(both_null);
	pr_stat(pc_null);
	pr_stat(gc_null);
	pr_stat(merges);
	pr_merge_ratio(merge_steps);
	printf("%smerge_steps_max:%lu\n", prefix, st->merge_steps_max);
	pr_merge_ratio(merge_ticks);
	printf("%smerge_ticks_max:%lu\n", prefix, st->merge_ticks_max);
	pr_ticks(cur_down_ticks);
	pr_ticks(cur_next_ticks);
	pr_ticks(do_merge_ticks);
	pr_ticks(ver_join_ticks);
	pr_ticks(ver_rebase_ticks);
	pr_ticks(cur_sync_ticks);
	pr_ticks(cur_replace_ticks);
	pr_ticks2(cur_do_replace_ticks, cur_do_replace_count);
	pr_ticks2(cur_do_replace_putref_ticks, cur_do_replace_count);

	#undef pr_stat
	#undef pr_merge_ratio
	#undef pr_ticks
}

void vbpt_merge_stats_report(void)
{
	tmsg("Merge stats\n");
	vbpt_merge_stats_do_report("\t", &MergeStats);
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
static int
do_merge(const vbpt_cur_t *gc, vbpt_cur_t *pc,
        const vbpt_tree_t *gtree, vbpt_tree_t *ptree,
        ver_t *gv, ver_t  *pv, uint16_t g_dist, uint16_t p_dist, ver_t *jv)
{
	MergeStats.merge_steps++;
	assert(vbpt_range_eq(&gc->range, &pc->range));
	ver_t *gc_v = vbpt_cur_ver(gc);
	ver_t *pc_v = vbpt_cur_ver(pc);
	#if defined(XDEBUG_MERGE)
	tmsg("range: %4lu,+%3lu gc_v:%s \tpc_v:%s\n",
	      gc->range.key, gc->range.len, ver_str(gc_v), ver_str(pc_v));
	#endif
	assert(g_dist > 0);
	assert(p_dist > 0);
	vbpt_log_t *plog = vbpt_tree_log(ptree);
	vbpt_log_t *glog = vbpt_tree_log((vbpt_tree_t *)gtree);
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
	if (!ver_ancestor_limit(gc_v, gv, g_dist - 1)) {
		#if defined(XDEBUG_MERGE)
		printf("NO CHANGES in gc_v\n");
		#endif
		MergeStats.gc_old++;
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
	if (!ver_ancestor_limit(pc_v, pv, p_dist - 1)) {
		#if defined(XDEBUG_MERGE)
		printf("Only gc_v changed\n");
		#endif
		MergeStats.pc_old++;
		// check if private tree read something that is under the
		// current (changed in the global tree) range. If it did,
		// it would read an older value, so we need to abort.
		if (vbpt_log_rs_range_exists(plog, range, p_dist)) {
			return -1;
		}
		//assert(pc_v != gc_v);
		// we need to effectively replace the node pointed by @pv with
		// the node pointed by @gc
		return vbpt_cur_replace(pc, gc, jv, p_dist) ? 1: -1;
	}

	/*
         *              |
	 *             (jv)
	 *            /   \<----(pc_v)
	 *  (gc_v)-->/     \
	 *          /     (pv)
	 *        (gv)
	 */
	 #if defined(XDEBUG_MERGE)
	 printf("Both changed\n");
	 #endif
	 //printf("base: %s\n", ver_str(jv));
	 //printf("pc:"); vbpt_cur_print(pc);
	 //printf("gc:"); vbpt_cur_print(gc);
	 //printf("\n");

	// Handle NULL cases: NULL cases are special because we lack information
	// to precisely check for conflicts. For example, we can't go deeper --
	// all information on the tree is lost. It might be a good idea to make
	// (more) formal arguments about the correctness of the cases below
	if (vbpt_cur_null(pc) && vbpt_cur_null(gc)) {
		MergeStats.both_null++;
		#if defined(XDEBUG_MERGE)
		printf("Both are NULL\n");
		#endif
		// if both cursors point to NULL, there is a conflict if @gv has
		// read an item from the previous state, which may not have been
		// NULL.  We could also check whether @glog contains a delete to
		// that range.
		int ret = vbpt_log_rs_range_exists(plog, range, p_dist) ? -1:1;
		return ret;
	} else if (vbpt_cur_null(pc)) {
		MergeStats.pc_null++;
		#if defined(XDEBUG_MERGE)
		printf("pc is NULL\n");
		#endif
		// @pc points to NULL, but @gc does not. If @pv did not read or
		// delete anything in that range, we can replace @pc with @gc.
		if (vbpt_log_rs_range_exists(plog, range, p_dist))
			return -1;
		if (vbpt_log_ds_range_exists(plog, range, p_dist))
			return -1;
		//printf("trying to replace pc with gc\n");
		return vbpt_cur_replace(pc, gc, jv, p_dist) ? 1: -1;
	} else if (vbpt_cur_null(gc)) {
		MergeStats.gc_null++;
		#if defined(XDEBUG_MERGE)
		printf("gc is NULL\n");
		#endif
		// @gc points  to NULL, but @pc does not. We can't just check
		// against the readset of @plog and keep the NULL: If @gc
		// deleted a key, that @pc still has due to COW (not because it
		// inserted it) we would need to delete it.

		// Since, however, @gc points to NULL, if there are no deletions
		// in Log(J->G), then @jv pointed to NULL for gc->range. Hence,
		// if there are no Reads in Log(J->P), there is no conflict.
		if (!vbpt_log_ds_range_exists(glog, range, g_dist) &&
		    !vbpt_log_rs_range_exists(plog, range, p_dist)) {
			return 1;
		}

		// special case: this is a leaf that we now has changed after
		// @vj -- we just keep it similarly to leaf checks
		if (range->len == 1 &&
		    !vbpt_log_rs_key_exists(plog, range->key, p_dist))
			return 1;

		printf("gc is null, I ran out of options\n");
		return -1;
	}

	assert(!vbpt_cur_null(gc) && !vbpt_cur_null(pc));
	if (range->len == 1) {
		return vbpt_log_rs_key_exists(plog, range->key, p_dist) ? -1:1;
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
	unsigned long msteps = MergeStats.merge_steps;
	tsc_t tsc_, tsc2_;
	tsc_init(&tsc_); tsc_start(&tsc_);
	#if defined(XDEBUG_MERGE)
	dmsg("Global  "); vbpt_tree_print_limit((vbpt_tree_t *)gt, true, 1);
	dmsg("Private "); vbpt_tree_print_limit(pt, true, 1);
	#endif

	#define TSC2_START() { tsc_init(&tsc2_); tsc_start(&tsc2_); }
	#define TSC2_END()   ({tsc_pause(&tsc2_); tsc_getticks(&tsc2_);})

	MergeStats.merges++;
	vbpt_cur_t gc, pc;
	vbpt_cur_init(&gc, (vbpt_tree_t *)gt);
	vbpt_cur_init(&pc, pt);
	bool merge_ok = true;

	TSC2_START()
	uint16_t g_dist, p_dist;
	ver_t *hpver = NULL; // initialize to shut the compiler up
	ver_t *gver = gt->ver;
	ver_t *pver = pt->ver;
	ver_t *vj = ver_join(gver, pver, &hpver, &g_dist, &p_dist);
	MergeStats.ver_join_ticks += TSC2_END();
	if (vj == VER_JOIN_FAIL) {
		MergeStats.join_failed++;
		merge_ok = false;
		goto end;
	}
	#if defined(XDEBUG_MERGE)
	printf("VERSIONS: gver:%s  pver:%s  vj:%s g_dist:%d p_dist:%d\n",
	        ver_str(gver), ver_str(pver), ver_str(vj), g_dist, p_dist);
	#endif

	while (!(vbpt_cur_end(&gc) && vbpt_cur_end(&pc))) {
		assert(vbpt_path_verify((vbpt_tree_t *)gt, &gc.path));
		assert(vbpt_path_verify(pt, &pc.path));
		TSC2_START();
		vbpt_cur_sync(&gc, &pc);
		MergeStats.cur_sync_ticks += TSC2_END();
		TSC2_START();
		int ret = do_merge(&gc, &pc, gt, pt, gver, pver, g_dist, p_dist, vj);
		MergeStats.do_merge_ticks += TSC2_END();
		if (ret == -1) {
			merge_ok = false;
			goto end;
		} else if (ret == 0) {
			TSC2_START();
			vbpt_cur_down(&gc);
			vbpt_cur_down(&pc);
			MergeStats.cur_down_ticks += TSC2_END();
		} else if (ret == 1) {
			TSC2_START();
			vbpt_cur_next(&gc);
			vbpt_cur_next(&pc);
			MergeStats.cur_next_ticks += TSC2_END();
		} else assert(false && "This should never happen");
	}
	/* success: fix version tree */
	TSC2_START();
	assert(!ver_chain_has_branch(pver, hpver));
	ver_rebase(hpver, gver);
	if (vbase)
		*vbase = gver;
	assert(ver_ancestor(gver, pver));
	assert(ver_ancestor(gver, hpver));
	MergeStats.ver_rebase_ticks += TSC2_END();
end:
	tsc_pause(&tsc_);
	msteps = MergeStats.merge_steps - msteps;
	if (msteps > MergeStats.merge_steps_max)
		MergeStats.merge_steps_max = msteps;
	unsigned long mticks = tsc_getticks(&tsc_);
	if (mticks > MergeStats.merge_ticks_max)
		MergeStats.merge_ticks_max = mticks;
	MergeStats.merge_ticks += mticks;
	#if defined(XDEBUG_MERGE)
	dmsg("MERGE %s\n", merge_ok ? "SUCCEEDED":"FAILED");
	#endif
	return merge_ok;

	#undef TSC2_START
	#undef TSC2_END
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
