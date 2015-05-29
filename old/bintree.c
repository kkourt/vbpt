/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#include <stdio.h>
#include <stdlib.h>

#include <assert.h>

union bintree_node;
struct bintree_inode;
struct bintree_leaf;

struct bintree {
	size_t             elems_nr;
	union bintree_node *root;
};

struct bintree_i { /* internal node */
	union bintree_node *left, *right;
};

struct bintree_l { /* leaf */
	unsigned long val;
};

union btree_node {
	struct bintree_i inode;
	struct bintree_l  leaf;
};
