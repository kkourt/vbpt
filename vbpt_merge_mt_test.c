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
#include <inttypes.h>
#include <pthread.h>

#include "ver.h"
#include "vbpt.h"
#include "vbpt_mm.h"
#include "vbpt_log.h"
#include "vbpt_merge.h"
#include "vbpt_mtree.h"
#include "vbpt_tx.h"
#include "vbpt_kv.h"
#include "vbpt_test.h"
#include "vbpt_stats.h"

#include "tsc.h"
#include "array_size.h"
#include "mt_lib.h"
#include "xdist.h"
#include "parse_int.h"

// default parameter values
#define DEF_RANGE_LEN 32768
#define DEF_INS0      (DEF_RANGE_LEN / 128)
#define DEF_TX_KEYS   32
#define DEF_TX_RANGE  128
#define DEF_NTXS      (16*1024)

#define DEF_PARAMS {\
    DEF_RANGE_LEN,  \
    DEF_INS0,       \
    DEF_TX_KEYS,    \
    DEF_TX_RANGE,   \
    DEF_NTXS        \
}

#define DO_VERIFY // verify results

// test parameters
struct params {
	size_t    range_len;  // key range is from 0 to range_len - 1
	size_t    ins0;       // number of keys initially inserted
	size_t    tx_keys;    // number of keys per transaction
	size_t    tx_range;   // transaction range
	size_t    ntxs;       // number of transactions;
};

// statistics per thread
struct merge_thr_stats {
	unsigned long      failures;
	unsigned long      merges;
	unsigned long      merge_failures;
	unsigned long      successes;
	unsigned long      commit_attempts;
	vbpt_mm_stats_t    mm_stats;
	unsigned long      tid;
	tsc_t              txtree_alloc;
	tsc_t              insert;
	tsc_t              finalize;
	tsc_t              commit;
	struct vbpt_stats  vbpt_stats;
};

// thread arguments
struct merge_thr_arg {
	vbpt_mtree_t            *mtree;
	struct xdist_desc       *wl;
	pthread_barrier_t       *barrier;
	unsigned                ntxs;
	unsigned                id;
	unsigned                cpu;
	tsc_t                   thread_ticks;
	spinlock_t              *lock;
	struct merge_thr_stats  stats;
	#if defined(DO_VERIFY)
	struct xdist_desc       wl_copy;
	#endif
};

static void
params_print(struct params *ps)
{
	printf("range_len:%zd ins0:%zd tx_keys:%zd tx_range:%zd ntxs:%zd\n",
	        ps->range_len,
	        ps->ins0,
	        ps->tx_keys,
	        ps->tx_range,
	        ps->ntxs);
}

static void
merge_thr_print_stats(struct merge_thr_arg *arg)
{
	uint64_t total = tsc_getticks(&arg->thread_ticks);
	#define pr_ticks(x__) do { \
		tsc_report_perc( "" # x__, \
		                 &s->x__, \
		                 total, \
		                 0); \
	} while (0)

	struct merge_thr_stats *s = &arg->stats;
	tsc_report_perc("total", &arg->thread_ticks, total, 0);
	pr_ticks(txtree_alloc);
	pr_ticks(insert);
	pr_ticks(finalize);
	pr_ticks(commit);

	//pr_ticks(merge_stats.merge_ticks);
	//printf("  Merge Stats:\n");
	//uint64_t merge_ticks = s->merge_stats.merge_ticks;
	//printf("\tmerge ticks: %lu [merge/total:%lf]\n",
	//         merge_ticks, (double)merge_ticks/(double)arg->ticks);
	//vbpt_merge_stats_do_report("\t", &s->merge_stats);
	//printf("\tmm_allocations: %lu\n", s->mm_stats.nodes_allocated);

	printf("  commit attempts: %5lu", s->commit_attempts);
	printf("  successes: %5lu", s->successes);
	printf("  merges: %5lu", s->merges);
	printf("  failures: %5lu", s->failures);
	printf("  merge failures: %5lu\n", s->merge_failures);
}

static void *
merge_test_thr(void *arg_)
{
	struct merge_thr_arg *arg = (struct merge_thr_arg *)arg_;

	vbpt_mtree_t *mtree = arg->mtree;

	bzero(&arg->stats, sizeof(arg->stats));
	vbpt_mm_init();
	vbpt_stats_init();
	setaffinity_oncpu(arg->cpu);
	arg->stats.tid = gettid();

	pthread_barrier_wait(arg->barrier);
	tsc_init(&arg->thread_ticks); tsc_start(&arg->thread_ticks);

	for (unsigned i=0; i<arg->ntxs; i++) {
		//tsc_spinticks(10000);
		unsigned fails = 0;
		struct xdist_desc old_xdist;
		while (1) {
			old_xdist = *arg->wl;
			// start a transaction
			vbpt_txtree_t *txt;
			TSC_UPDATE(&arg->stats.txtree_alloc, {
				txt = vbpt_txtree_alloc(mtree);
			})
			//tmsg("forked %zd from %zd\n",
			//      txt->tree->ver->v_id, txt->bver->v_id);
			TSC_UPDATE(&arg->stats.insert, {
				//vbpt_logtree_insert_rand(txt->tree,
				//                          arg->wl,
				//                          &seed);
				vbpt_logtree_kv_inc_rand(txt->tree, arg->wl);
			})
			TSC_UPDATE(&arg->stats.finalize, {
				vbpt_logtree_finalize(txt->tree);
			})

			arg->stats.commit_attempts++;
			vbpt_txt_res_t ret;
			TSC_UPDATE(&arg->stats.commit, {
				//ret = vbpt_txt_try_commit(txt, mtree, 4);
				ret = vbpt_txt_try_commit2(txt, mtree);
			})
			//tmsg("RET:%s\n", vbpt_txt_res2str[ret]);
			if (ret == VBPT_COMMIT_FAILED) {
				*(arg->wl) = old_xdist;
				fails++;
				arg->stats.failures++;
			} else if (ret == VBPT_COMMIT_MERGE_FAILED) {
				*(arg->wl) = old_xdist;
				fails++;
				arg->stats.merge_failures++;
			} else if  (ret == VBPT_COMMIT_OK) {
				arg->stats.successes++;
				break;
			} else if (ret == VBPT_COMMIT_MERGED) {
				arg->stats.merges++;
				break;
			}

			//if (fails > 32) assert(false && "Something's wrong here");
		}
	}

	tsc_pause(&arg->thread_ticks);
	pthread_barrier_wait(arg->barrier);
	vbpt_mm_stats_get(&arg->stats.mm_stats);
	vbpt_stats_get(&arg->stats.vbpt_stats);
	return NULL;
}

static void
vbpt_mt_merge_test(unsigned nthreads, unsigned *cpus,
                   struct xdist_desc *wls,
                   struct xdist_desc *wl0,
                   uint64_t ntxs)
{
	pthread_barrier_t    barrier;
	spinlock_t           lock;
	struct merge_thr_arg args[nthreads];
	pthread_t            tids[nthreads];

	// print distributions
	printf(" I> ");xdist_print(wl0);
	for (unsigned i=0; i<nthreads; i++) {
		printf("%2d> ", i); xdist_print(wls + i);
	}

	#if defined(DO_VERIFY)
	struct xdist_desc wl0_copy = *wl0;
	size_t verify_size         = wl0_copy.r_len*sizeof(uint64_t);
	uint64_t *verify           = xmalloc(verify_size);
	uint64_t x;
	memset(verify, VBPT_KV_DEFVALBYTE, verify_size);
	xdist_for_each(&wl0_copy, x) {
		verify[x] = 1;
	}
	#endif

	// allocate and initialize inital tree
	vbpt_tree_t *tree = vbpt_tree_create();
	//vbpt_tree_insert_rand(tree, wl0, &seed);
	vbpt_kv_insert_val_rand(tree, wl0, 1);


	vbpt_mtree_t *mtree = vbpt_mtree_alloc(tree);

	if (pthread_barrier_init(&barrier, NULL, nthreads+1) != 0)
		assert(false && "failed to initialize barrier");

	uint64_t total_ops = 0;
	spinlock_init(&lock);
	for (unsigned i=0; i<nthreads; i++) {
		struct merge_thr_arg *arg = args + i;
		arg->mtree   = mtree;
		arg->wl      = wls + i;
		arg->barrier = &barrier;
		arg->lock    = &lock;
		arg->ntxs    = ntxs;
		arg->id      = i;
		arg->cpu     = cpus[i];
		pthread_create(tids+i, NULL, merge_test_thr, arg);
		total_ops += arg->wl->nr;
		#if defined(DO_VERIFY)
		arg->wl_copy = *(arg->wl);
		#endif
	}

	pthread_barrier_wait(&barrier);
	TSC_MEASURE_TICKS(thr_ticks, {
		pthread_barrier_wait(&barrier);
	})

	for (unsigned i=0; i<nthreads; i++) {
		pthread_join(tids[i], NULL);
	}

	#if defined(DO_VERIFY)
	for (unsigned i=0; i < nthreads; i++) {
		for (size_t tx=0; tx < ntxs; tx++) {
			uint64_t x;
			xdist_for_each(&args[i].wl_copy, x) {
				verify[x] += 1;
			}
		}
	}
	for (uint64_t k=0; k < wl0_copy.r_len; k++) {
		uint64_t vbpt_val = vbpt_kv_get(mtree->mt_tree, k);
		uint64_t verf_val = verify[k];
		if (vbpt_val != verf_val) {
			printf(" k=%" PRIu64 ":"
			       " vbpt_val=%" PRIu64
			       " verf_val=%" PRIu64 "\n",
			       k, vbpt_val, verf_val);
			abort();
		}
	}
	#endif

	//tc_malloc_stats();
	vbpt_mtree_dealloc(mtree, NULL);

	printf("nthreads:%u ticks_per_op:%lu total_ticks:%5s, ntxs:%lu\n",
	        nthreads,
	        thr_ticks/(ntxs*total_ops),
	        tsc_u64_hstr(thr_ticks),
	        ntxs);
	tsc_report_ticks("ALL_ticks", thr_ticks);
	for (unsigned i=0; i<nthreads; i++) {
		printf("T: %2u [tid:%lu]\n", i, args[i].stats.tid);
		merge_thr_print_stats(args+i);
		vbpt_stats_do_report(" ", &args[i].stats.vbpt_stats, thr_ticks);
		vbpt_mm_stats_report("  ", &args[i].stats.mm_stats);
	}
}

static void __attribute__((unused))
test_mt_rand(struct params *ps, unsigned nr_threads, unsigned *cpus)
{
	struct xdist_desc dt[nr_threads];
	struct xdist_desc d0 = {
		.r_start = 0,
		.r_len   = ps->range_len,
		.nr      = ps->ins0,
		.seed    = 1
	};

	const unsigned long part_len = ps->range_len / nr_threads;
	assert(part_len > d_len);
	for (unsigned i=0; i<nr_threads; i++) {
		struct xdist_desc *d = dt + i;
		d->nr      = ps->tx_keys;
		d->r_start = part_len*i;
		d->r_len   = ps->tx_range;
		d->seed    = 1;
	}

	vbpt_mt_merge_test(nr_threads, cpus, dt, &d0, ps->ntxs);
}

static void
usage(const char *prog)
{
	struct params params = DEF_PARAMS;
	printf("Usage: %s range,ins0,tx_keys,tx_range,ntxs\n", prog);
	printf("   default:  "); params_print(&params);
	exit(0);
}

int main(int argc, const char *argv[])
{
	unsigned int ncpus, *cpus;
	mt_get_options(&ncpus, &cpus);

	printf("Using %u threads [cpus: ", ncpus);
	for (unsigned int i=0; i<ncpus; i++)
		printf("%d ", cpus[i]);
	printf("]\n");

	if (argc < 2) {
		usage(argv[0]);
		exit(1);
	}

	int tuple[5] = DEF_PARAMS;
	parse_int_tuple(argv[1], tuple, ARRAY_SIZE(tuple));
	struct params params = {
		.range_len = tuple[0],
		.ins0      = tuple[1],
		.tx_keys   = tuple[2],
		.tx_range  = tuple[3],
		.ntxs      = tuple[4]
	};
	printf("PS> "); params_print(&params);

	test_mt_rand(&params, ncpus, cpus);

	printf("DONE\n");
	return 0;
}
