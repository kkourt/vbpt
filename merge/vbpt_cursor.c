
#include "vbpt.h"
#include "vbpt_cursor.h"
#include "ver.h"

#include "misc.h"

#define VBPT_KEY_MAX UINT64_MAX

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
vbpt_cur_ver(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	vbpt_tree_t *tree = cur->tree;
	return path->height == 0 ? tree->ver : vbpt_cur_hdr(cur)->ver;
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

int vbpt_merge(vbpt_tree_t *t1, vbpt_tree_t *t2)
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
		} else if (c1->null_max_key != 0 || c2->null_max_key != 0) {
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
	return 0;
}

#if defined(VBPT_CURSOR_TEST)
int main(int argc, const char *argv[])
{
	vbpt_tree_t *t1 = vbpt_tree_create();
	vbpt_insert(t1, 42,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, t1->ver), NULL);
	vbpt_insert(t1, 100, vbpt_leaf_alloc(VBPT_LEAF_SIZE, t1->ver), NULL);

	vbpt_tree_t *t2 = vbpt_tree_branch(t1);
	vbpt_insert(t2, 66,  vbpt_leaf_alloc(VBPT_LEAF_SIZE, t2->ver), NULL);
	vbpt_delete(t2, 42, NULL);

	vbpt_tree_print(t1, true);
	vbpt_tree_print(t2, true);

	vbpt_merge(t1, t2);
	return 0;
}
#endif
