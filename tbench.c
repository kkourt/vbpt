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
#include <string.h>
#include <pthread.h>

#include "vbpt.h"
#include "vbpt_mtree.h"
#include "vbpt_merge.h"
#include "vbpt_tx.h"
#include "vbpt_mm.h"

#include "mt_lib.h"
#include "tsc.h"
#include "misc.h"

/*
 * tbench.c : tree benchmark
 */

enum tree_op {
	TREEOP_INSERT,
	TREEOP_LOOKUP,
	TREEOP_DELETE,
	TREEOP_OPS_NR__
};


struct targ {
	int tid;                   // thread id
	int nthreads;              // total number of threads
	unsigned int core;         // core this thread is affined to
	pthread_barrier_t *tbar;   // barrier for measuring time

	size_t k_min, k_max;       // key range
	unsigned seed;
	float in_p, dl_p;          // insert / remove percentage

	size_t            ntxs;    // number of transactions
	size_t            tx_nops; // operations per transaction

	tsc_t             ticks;

	// vbpt-specific
	vbpt_mtree_t      *mtree;
	vbpt_stats_t      vbpt_stats;
	vbpt_mm_stats_t   vbpt_mm_stats;
};

/* randf(): return a random float number in [0,1] */
static inline float
randf(unsigned int *seed)
{
	return ((float)rand_r(seed))/((float)RAND_MAX);
}


// randi(): return a random integer in [i_min, i_max] range
static inline int
randi(unsigned int *seed, size_t i_min, size_t i_max)
{
	return (rand_r(seed) % (i_max - i_min + 1)) + i_min;
}

static enum tree_op
randop(struct targ *targ)
{
	enum tree_op ret;

	float  f = randf(&targ->seed);
	if (f < targ->in_p) {
		ret = TREEOP_INSERT;
	} else if (f < targ->in_p + targ->dl_p) {
		ret = TREEOP_DELETE;
	} else {
		ret = TREEOP_LOOKUP;
	}

	return ret;
}

static void
do_randops(vbpt_tree_t *tree, struct targ *targ)
{
	static uint32_t cnt = 0;
	static volatile uint64_t ret = 0;
	uint64_t prefix = ((uint64_t)targ->tid)<<32;
	ver_t *ver = tree->ver;

	for (size_t i=0; i < targ->tx_nops; i++) {
		size_t key = randi(&targ->seed, targ->k_min, targ->k_max);
		enum tree_op top = randop(targ);
		vbpt_leaf_t *leaf;
		switch (top) {
			case TREEOP_INSERT:
			leaf = vbpt_leaf_alloc(0, ver);
			leaf->val = prefix | cnt++;
			vbpt_logtree_insert(tree, key, leaf, NULL);
			break;

			case TREEOP_LOOKUP:
			leaf = vbpt_logtree_get(tree, key);
			if (leaf)
				ret += leaf->val;
			break;

			case TREEOP_DELETE:
			vbpt_logtree_delete(tree, key, &leaf);
			if (leaf) {
				ret += leaf->val;
				vbpt_leaf_putref(leaf);
			}
			break;

			case TREEOP_OPS_NR__:
			abort();
			break;
		}
	}
}

static void *
vbpt_thread(void *arg)
{
	vbpt_txtree_t *txt;
	unsigned seed;
	struct targ *targ   = arg;
	size_t ntxs         = targ->ntxs;
	vbpt_mtree_t *mtree = targ->mtree;

	vbpt_stats_init();
	vbpt_mm_init();
	tsc_init(&targ->ticks);
	pthread_barrier_wait(targ->tbar);

	tsc_start(&targ->ticks);
	for (size_t tx=0; tx < ntxs; tx++) {
		do {
			seed = targ->seed; // take a snapshot of seed
			txt = vbpt_txtree_alloc(mtree); // start transaction

			//printf("t=%d tx=%zu\n", targ->tid, tx);
			do_randops(txt->tree, targ);

			vbpt_logtree_finalize(txt->tree); // finish transaction
			vbpt_txt_res_t ret = vbpt_txt_try_commit(txt, mtree, 2);
			if (ret == VBPT_COMMIT_OK || ret == VBPT_COMMIT_MERGED)
				break;

			targ->seed = seed; // failed to commit: restore seed
		} while (1);
	}
	tsc_pause(&targ->ticks);

	pthread_barrier_wait(targ->tbar);

	vbpt_stats_get(&targ->vbpt_stats);
	vbpt_mm_stats_get(&targ->vbpt_mm_stats);
	pthread_barrier_wait(targ->tbar);
	return NULL;
}


static void __attribute__((unused))
vbpt_thr_print_stats(struct targ *arg)
{
	tsc_report("", &arg->ticks);
	printf("  VBPT Stats:\n");
	vbpt_stats_do_report("  ", &arg->vbpt_stats, tsc_getticks(&arg->ticks));
	//vbpt_mm_stats_report("  ", &arg->vbpt_mm_stats);
	//vbpt_merge_stats_do_report("\t", &arg->vbpt_stats.m);
}

// insert a key to populate the ->root pointer
static void
init_vbpt(vbpt_tree_t *tree)
{
	vbpt_leaf_t *leaf;
	vbpt_stats_init();
	vbpt_mm_init();
	vbpt_logtree_log_init(tree);
	leaf = vbpt_leaf_alloc(0, tree->ver);
	vbpt_logtree_insert(tree, 0, leaf, NULL);
}

static void
do_run(char *prefix,
       pthread_barrier_t *tbar, struct targ *targs, unsigned nthreads)
{
	pthread_t tids[nthreads];

	for (int i=0; i<nthreads; i++)
		pthread_create(tids + i, NULL, vbpt_thread, targs + i);

	pthread_barrier_wait(tbar);         // START
	TSC_MEASURE(ticks, {
		pthread_barrier_wait(tbar); // END
	})
	pthread_barrier_wait(tbar);         // GET STATS

	for (int i=0; i<nthreads; i++) {
		pthread_join(tids[i], NULL);
	}

	tsc_report(prefix, &ticks);
	printf("---------------------------------------------------------\n");
	for (unsigned i=0; i<nthreads; i++) {
		printf("T: %2u [tid:%d] ", i, targs[i].tid);
		vbpt_thr_print_stats(targs+i);
	}
	printf("---------------------------------------------------------\n\n");
}

int
main(int argc, const char *argv[])
{
	size_t nops, tx_nops;  // number of operations (total/per tx)
	float in_p, dl_p;      // read / insert / remove probability

	if (argc < 4) {
		fprintf(stderr,
		        "Usage: %s <nops> <tx_nops> <in_p> [dl_p]\n", argv[0]);
		exit(1);
	}

	nops    = atol(argv[1]);
	tx_nops = atol(argv[2]);
	in_p    = atof(argv[3]);
	dl_p    = (argc > 4) ? atof(argv[4]) : 0.0;

	if (in_p + dl_p > 1.0) {
		fprintf(stderr, "%lf + %lf > 1.0\n", in_p, dl_p);
		abort();
	}

	unsigned int nthreads;
	unsigned int *cpus;
	mt_get_options(&nthreads, &cpus);

	struct targ targs[nthreads];
	pthread_barrier_t tbar;
	pthread_barrier_init(&tbar, NULL, nthreads+1);

	size_t ntxs = ((nops / nthreads) / tx_nops);
	size_t nops_per_thr = ntxs*tx_nops;
	size_t nops_all     = nops_per_thr*nthreads;
	printf("nthr:%u ntxs:%zu tx_nops:%zu [nops/thr:%zu nops_all:%zu]\n",
	       nthreads, ntxs, tx_nops, nops_per_thr, nops_all);

	vbpt_tree_t  *tree0 = vbpt_tree_create();
	init_vbpt(tree0);
	vbpt_mtree_t *mtree = vbpt_mtree_alloc(tree0);

	size_t key = 0, key_step = ((size_t)-1) / nthreads;
	bzero(targs, sizeof(targs));
	for (int i=0; i<nthreads; i++) {
		targs[i].tid      = i;
		targs[i].nthreads = nthreads;
		targs[i].core     = cpus[i];
		targs[i].tbar     = &tbar;

		targs[i].k_min  = key;
		key            += key_step;
		targs[i].k_max  = key - 1;

		targs[i].seed = 0;
		targs[i].in_p = in_p;
		targs[i].dl_p = dl_p;

		targs[i].ntxs    = ntxs;
		targs[i].tx_nops = tx_nops;

		targs[i].mtree  = mtree;
	}


	do_run("empty:     ", &tbar, targs, nthreads);
	do_run("non-empty: ", &tbar, targs, nthreads);

	free(cpus);
	return 0;
}
