#include <stdio.h>
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

#include "tsc.h"
#include "array_size.h"
#include "mt_lib.h"
#include "xdist.h"

struct merge_thr_stats {
	unsigned long      failures;
	unsigned long      merges;
	unsigned long      merge_failures;
	unsigned long      successes;
	unsigned long      commit_attempts;
	vbpt_mm_stats_t    mm_stats;
	unsigned long      tid;
	uint64_t           txtree_alloc_ticks;
	uint64_t           insert_ticks;
	uint64_t           finalize_ticks;
	uint64_t           commit_ticks;
	struct vbpt_stats  vbpt_stats;
};

struct merge_thr_arg {
	vbpt_mtree_t            *mtree;
	struct xdist_desc       *wl;
	pthread_barrier_t       *barrier;
	unsigned                ntxs;
	unsigned                id;
	unsigned                cpu;
	uint64_t                ticks;
	spinlock_t              *lock;
	struct merge_thr_stats  stats;
};

static void
merge_thr_print_stats(struct merge_thr_arg *arg)
{
	#define pr_ticks(x__) do { \
		double p__ = (double)s->x__ / (double)arg->ticks; \
		char  *s__ = tsc_ul_hstr(s->x__); \
		if (p__ < 0.1) \
			break; \
		printf("\t" # x__ ": %s (%.1lf%%)\n", s__, p__*100); \
	} while (0)

	struct merge_thr_stats *s = &arg->stats;
	printf("  ticks: %7.1lfm", (double)arg->ticks/(1000.0*1000.0));
	printf("  commit attempts: %5lu", s->commit_attempts);
	printf("  successes: %5lu", s->successes);
	printf("  merges: %5lu", s->merges);
	printf("  failures: %5lu", s->failures);
	printf("  merge failures: %5lu\n", s->merge_failures);
	//pr_ticks(merge_stats.merge_ticks);
	pr_ticks(txtree_alloc_ticks);
	pr_ticks(insert_ticks);
	pr_ticks(finalize_ticks);
	pr_ticks(commit_ticks);
	//printf("  Merge Stats:\n");
	//uint64_t merge_ticks = s->merge_stats.merge_ticks;
	//printf("\tmerge ticks: %lu [merge/total:%lf]\n",
	//         merge_ticks, (double)merge_ticks/(double)arg->ticks);
	//vbpt_merge_stats_do_report("\t", &s->merge_stats);
	//printf("\tmm_allocations: %lu\n", s->mm_stats.nodes_allocated);
	vbpt_mm_stats_report("  ", &s->mm_stats);
}

static void *
merge_test_thr(void *arg_)
{
	struct merge_thr_arg *arg = (struct merge_thr_arg *)arg_;

	vbpt_mtree_t *mtree = arg->mtree;
	unsigned seed = arg->wl->seed;
	arg->stats = (struct merge_thr_stats){0};
	vbpt_mm_init();
	vbpt_stats_init();
	setaffinity_oncpu(arg->cpu);
	arg->stats.tid = gettid();

	pthread_barrier_wait(arg->barrier);
	tsc_t tsc; tsc_init(&tsc); tsc_start(&tsc);
	for (unsigned i=0; i<arg->ntxs; i++) {
		//tsc_spinticks(10000);
		unsigned fails = 0;
		while (1) {
			unsigned old_seed = seed;
			// start a transaction
			vbpt_txtree_t *txt;
			TSC_ADD_TICKS(arg->stats.txtree_alloc_ticks, {
				txt = vbpt_txtree_alloc(mtree);
			})
			//tmsg("forked %zd from %zd\n", txt->tree->ver->v_id, txt->bver->v_id);
			TSC_ADD_TICKS(arg->stats.insert_ticks, {
				//vbpt_logtree_insert_rand(txt->tree, arg->wl, &seed);
				vbpt_logtree_kv_insert_rand(txt->tree, arg->wl, &seed);
			})
			TSC_ADD_TICKS(arg->stats.finalize_ticks, {
				vbpt_logtree_finalize(txt->tree);
			})

			arg->stats.commit_attempts++;
			vbpt_txt_res_t ret;
			TSC_ADD_TICKS(arg->stats.commit_ticks, {
				//ret = vbpt_txt_try_commit(txt, mtree, 4);
				ret = vbpt_txt_try_commit2(txt, mtree);
			})
			//tmsg("RET:%s\n", vbpt_txt_res2str[ret]);
			if (ret == VBPT_COMMIT_FAILED) {
				seed = old_seed;
				fails++;
				arg->stats.failures++;
			} else if (ret == VBPT_COMMIT_MERGE_FAILED) {
				seed = old_seed;
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
	tsc_pause(&tsc);
	arg->ticks = tsc_getticks(&tsc);
	pthread_barrier_wait(arg->barrier);
	vbpt_mm_stats_get(&arg->stats.mm_stats);
	vbpt_stats_get(&arg->stats.vbpt_stats);
	return NULL;
}

static void
vbpt_mt_merge_test(vbpt_tree_t *tree,
                   unsigned nthreads, unsigned *cpus,
                   struct xdist_desc *wls)
{
	pthread_barrier_t    barrier;
	spinlock_t           lock;
	struct merge_thr_arg args[nthreads];
	pthread_t            tids[nthreads];

	vbpt_mtree_t *mtree = vbpt_mtree_alloc(tree);

	if (pthread_barrier_init(&barrier, NULL, nthreads+1) != 0)
		assert(false && "failed to initialize barrier");

	const uint64_t ntxs = 16*1024;
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
	}

	pthread_barrier_wait(&barrier);
	TSC_MEASURE_TICKS(thr_ticks, {
		pthread_barrier_wait(&barrier);
	})

	for (unsigned i=0; i<nthreads; i++) {
		pthread_join(tids[i], NULL);
	}

	//tc_malloc_stats();
	vbpt_mtree_dealloc(mtree, NULL);

	printf("nthreads:%u, ticks/op:%-8s total_ticks:%5s, ntxs:%lu\n",
	        nthreads,
	        tsc_ul_hstr(thr_ticks/(ntxs*total_ops)),
	        tsc_ul_hstr(thr_ticks),
	        ntxs);
	for (unsigned i=0; i<nthreads; i++) {
		printf("T: %2u [tid:%lu] ", i, args[i].stats.tid);
		merge_thr_print_stats(args+i);
		vbpt_stats_do_report(" ", &args[i].stats.vbpt_stats, thr_ticks);
	}
}

static void __attribute__((unused))
do_test_mt_rand(struct xdist_desc *d0,
                unsigned nthreads, unsigned *cpus,
                struct xdist_desc *ds)
{

	printf(" I> start:%6lu len:%6lu nr:%6lu seed:%u\n",
	       d0->r_start, d0->r_len, d0->nr, d0->seed);
	for (unsigned i=0; i<nthreads; i++) {
		struct xdist_desc *d = ds + i;
		printf("%2d> start:%6lu len:%6lu nr:%6lu seed:%u\n",
		       i, d->r_start, d->r_len, d->nr, d->seed);
	}


	vbpt_tree_t *tree = vbpt_tree_create();
	unsigned seed = d0->seed;
	//vbpt_tree_insert_rand(tree, d0, &seed);
	vbpt_kv_insert_rand(tree, d0, &seed);

	vbpt_mt_merge_test(tree, nthreads, cpus, ds);
}

static void __attribute__((unused))
test_mt_rand(unsigned nr_threads, unsigned *cpus)
{
	/**
	 * create disjoint sets for each thread:
	 *   d0:      initial data
	 *   d:       thread-specific data
	 *   d0_len:  key range (max key)
	 *   d0_nr:   number of initial keys
	 *   d_nr:    number of keys for transaction
	 *   d_len:   key range for each thread
	 */
	const unsigned long d0_len = 32768;
	const unsigned long d0_nr  = (d0_len/128); // /128
	const unsigned long d_nr   = 32;
	const unsigned long d_len  = 128;

	struct xdist_desc d0 = {
		.r_start = 0,
		.r_len   = d0_len,
		.nr      = d0_nr,
		.seed    = 1
	};
	struct xdist_desc dt[nr_threads];

	const unsigned long part_len = d0_len / nr_threads;
	assert(part_len > d_len);
	for (unsigned i=0; i<nr_threads; i++) {
		struct xdist_desc *d = dt + i;
		d->r_start = part_len*i;
		d->r_len   = d_len;
		d->nr      = d_nr;
		d->seed = 1;
	}

	do_test_mt_rand(&d0, nr_threads, cpus, dt);
}

int main(int argc, const char *argv[])
{
	#if 0
	//do_test_mt_rand(&d0, 1, ds);
	//do_test_mt_rand(&d0, 2, ds);
	#endif

	unsigned int ncpus, *cpus;
	mt_get_options(&ncpus, &cpus);
	#if 0
	printf("Using %u cpus: ", ncpus);
	for (unsigned int i=0; i<ncpus; i++)
		printf("%d ", cpus[i]);
	printf("\n");
	#endif
	test_mt_rand(ncpus, cpus);

	return 0;
}
