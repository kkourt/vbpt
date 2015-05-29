/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#ifndef VBPT_TEST_H
#define VBPT_TEST_H

#include "vbpt.h"
#include "vbpt_log.h"
#include "vbpt_kv.h"
#include "xdist.h" // needed for _rand() functions

// vbpt helpers aimed mostly at running tests

static inline void
vbpt_tree_insert_bulk(vbpt_tree_t *t, uint64_t *ins, uint64_t ins_len)
{
	ver_t *ver = t->ver;
	for (uint64_t i=0; i<ins_len; i++) {
		uint64_t key = ins[i];
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_insert(t, key, leaf, NULL);
	}
}

static inline void
vbpt_logtree_insert_bulk(vbpt_tree_t *tree, uint64_t *ins, uint64_t ins_len)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<ins_len; i++) {
		uint64_t key = ins[i];
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_logtree_insert(tree, key, leaf, NULL);
	}
}

// rand() functions

static inline void
vbpt_logtree_insert_rand(vbpt_tree_t *tree, struct xdist_desc *d)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_logtree_insert(tree, key, leaf, NULL);
	}
}

static inline void
vbpt_tree_insert_rand(vbpt_tree_t *tree, struct xdist_desc *d)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_insert(tree, key, leaf, NULL);
	}
}

static inline void
vbpt_kv_insert_rand(vbpt_tree_t *tree, struct xdist_desc *d)
{
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		uint64_t val = key;
		vbpt_kv_insert(tree, key, val);
	}
}

static inline void
vbpt_kv_insert_val_rand(vbpt_tree_t *tree, struct xdist_desc *d, uint64_t val)
{
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		vbpt_kv_insert(tree, key, val);
	}
}


static inline void
vbpt_logtree_kv_insert_rand(vbpt_tree_t *t, struct xdist_desc *d)
{
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		uint64_t val = key;
		vbpt_logtree_kv_insert(t, key, val);
	}
}

static inline void
vbpt_logtree_kv_inc_rand(vbpt_tree_t *t, struct xdist_desc *d)
{
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = xdist_rand(d);
		uint64_t val = vbpt_logtree_kv_get(t, key);
		vbpt_logtree_kv_insert(t, key, val + 1);
	}
}

#endif
