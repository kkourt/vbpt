
#include "vbpt.h"
#include "vbpt_cursor.h"
#include "ver.h"

#include "misc.h"

/**
 * return hdr pointed by the cursor
 */
vbpt_hdr_t *
vbpt_cur_hdr(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	if (path->height == 0)
		return cur->tree->root ? &cur->tree->root->n_hdr : NULL;

	vbpt_node_t *pnode = path->nodes[path->height - 1];
	uint16_t pslot = path->slots[path->height - 1];
	if (pslot == pnode->items_nr) {
		return NULL;
	}

	assert(pslot < pnode->items_nr);
	return pnode->kvp[pslot].val;
}

static const vbpt_range_t vbpt_range_full = {.key = 0, .len = UINT64_MAX};

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

void
vbpt_cur_free(vbpt_cur_t *cur)
{
	free(cur);
}

ver_t *
vbpt_cur_ver(vbpt_cur_t *cur)
{
	vbpt_path_t *path = &cur->path;
	vbpt_tree_t *tree = cur->tree;
	return path->height == 0 ? tree->ver : vbpt_cur_hdr(cur)->ver;
}

/**
 * move to the node below
 */
void
vbpt_cur_down(vbpt_cur_t *cur)
{
	vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);

	// sanity checks
	assert(hdr && "Can't go down: pointing to NULL");
	if (hdr->type == VBPT_LEAF) {
		assert(cur->range.len == 1); // if this is a leaf, only one key
		assert(false && "Can't go down: pointing to a leaf");
	}
	assert(path->height < VBPT_MAX_LEVEL);
	vbpt_node_t *node = hdr2node(hdr);
	// fix path
	path->nodes[path->height] = node;
	path->slots[path->height] = 0;
	path->height++;
	// set range
	cur->range.len = cur.range.key - node->kvp[0].key
}


static inline uint64_t
max_key(vbpt_range_t *r)
{
	return
}

static inline void
cur_downrange1(vbpt_cur_t *cur, const vbpt_range_t *r)
{
	vbpt_range_t *cur_r = &cur->range;
	vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);
	if (hdr == NULL) {
		if (cur->null_max_key == 0) {
			cur->null_max_key = cur_r->key + cur_r->len;
		} else {
			assert(cur->null_max_key > cur_r->key + cur_r->len);
		}
		cur->range = *r;
	} else {
		vbpt_cur_down(cur);
	}
}

void
vbpt_cur_downrange(vbpt_cur_t *cur, const vbpt_range_t *r)
{
	assert(vbpt_range_lt(r, &cur->range));
	do {
		vbpt_range_t *cur_r = &cur->range;
		vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);
		if (hdr == NULL) {
			uint64_t new_max_key = cur_r->key + cur_r->len;
			if (cur->null_max_key == 0) {
				cur->null_max_key = new_max_key;
			assert(cur->null_max_key >= new_max_key);
			cur->range = *r;
		} else
			vbpt_cur_down(cur);
	} while (!vbpt_range_lt(r, cur->range));
}

/**
 * move to next node
 *  returns -1 if there are no more nodes
 */
int
vbpt_cur_next(vbpt_cur_t *cur)
{
	vbpt_hdr_t *hdr = vbpt_cur_hdr(cur);
	if (hdr == NULL) {
	}
}
