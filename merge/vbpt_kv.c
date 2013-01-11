
/**
 * Simple key-value interface for vbpt
 *
 * The main difference  with the traditional interface is that instead of
 * mapping keys to leafs, vbpt_kv maps keys to values where mutliple values are
 * maintained in a single leaf node. This reduces the COW overhead since inserts
 * on the same leaf do not require COW.
 *
 * Keys and Values are represented with uint64_t
 *
 * Since leafs contain mappings for multiple key ranges, we need to have a way
 * of distinguish between valid/invalid mappings. We do that by defining a
 * default value  (VBPT_KV_DEFVAL) to be returned for invalid mappings.
 *
 * Another solution would be to pack multiple key values in the leaf as:
 * [k0|off0|k1|off1|...free space...|---val1---|--val0--]
 *                                             <--off0--|
 *                                  <--------off1-------|
 *  ... or something similar
 */

// TODO write tests

#include <inttypes.h>

#include "vbpt.h"
#include "vbpt_log.h"
#include "vbpt_kv.h"

static inline long
vals_per_leaf(void)
{
	return VBPT_LEAF_SIZE/sizeof(uint64_t);
}

static vbpt_leaf_t *
cow_leaf_maybe(ver_t *ver, vbpt_leaf_t *l)
{
	vbpt_leaf_t *ret;

	if (l == NULL) {
		// allocate new leaf, and set default value
		ret = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		memset(ret->data, VBPT_KV_DEFVALBYTE, VBPT_LEAF_SIZE);
	} else if (!vref_eqver(l->l_hdr.vref, ver)) {
		// allocate a new leaf, and copy data
		ret = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		memcpy(ret->data, l->data, VBPT_LEAF_SIZE);
	} else {
		ret = l;
	}

	return ret;
}

// insert a value
//
// NB: optimzations that could be performed:
// 1. we could probably avoid traversing the tree two times, by creating an
//    appropriate vbpt function that gets a callback for ->data initialization,
//    and allocates a leaf only when it finds NULL or a different version when
//    traversing.
// 2. use leaf->d_len, so that we don't have to memset() all the data, just
//    the gap between leaf->d_len - 1 and the new position.
void
vbpt_kv_insert(vbpt_tree_t *tree, uint64_t kv_key, uint64_t kv_val)
{
	uint64_t key = kv_key / vals_per_leaf();
	uint64_t idx = kv_key % vals_per_leaf();

	vbpt_leaf_t *leaf = vbpt_get(tree, key);
	vbpt_leaf_t *new  = cow_leaf_maybe(tree->ver, leaf);
	if (new != leaf) {
		vbpt_leaf_t *old;
		vbpt_insert(tree, key, new, &old);
		assert(old == leaf);
		leaf = new;
	}

	// update value
	// (note that if we needed a new leaf, it's already inserted)
	((uint64_t *)leaf->data)[idx] = kv_val;
}


// get a value (returns -1, if value does not exist)
uint64_t
vbpt_kv_get(vbpt_tree_t *tree, uint64_t kv_key)
{
	uint64_t key = kv_key / vals_per_leaf();
	uint64_t idx = kv_key % vals_per_leaf();
	vbpt_leaf_t *leaf = vbpt_get(tree, key);

	return (leaf == NULL) ? VBPT_KV_DEFVAL : ((uint64_t *)leaf->data)[idx];
}

/**
 * same operations, but updating the log this time
 */

void
vbpt_logtree_kv_insert(vbpt_tree_t *tree, uint64_t kv_key, uint64_t kv_val)
{
	uint64_t key = kv_key / vals_per_leaf();
	uint64_t idx = kv_key % vals_per_leaf();

	vbpt_leaf_t *leaf = vbpt_logtree_get(tree, key);
	vbpt_leaf_t *new  = cow_leaf_maybe(tree->ver, leaf);
	if (new != leaf) {
		vbpt_leaf_t *old;
		vbpt_logtree_insert(tree, key, new, &old);
		assert(old == leaf);
		leaf = new;
	}

	// update value
	// (note that if we needed a new leaf, it's already inserted)
	((uint64_t *)leaf->data)[idx] = kv_val;
}


uint64_t
vbpt_logtree_kv_get(vbpt_tree_t *tree, uint64_t kv_key)
{
	uint64_t key = kv_key / vals_per_leaf();
	uint64_t idx = kv_key % vals_per_leaf();
	vbpt_leaf_t *leaf = vbpt_logtree_get(tree, key);

	return (leaf == NULL) ? VBPT_KV_DEFVAL : ((uint64_t *)leaf->data)[idx];
}
