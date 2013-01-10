#include <stdio.h>
#include <pthread.h>

#include "ver.h"
#include "vbpt.h"
#include "vbpt_mm.h"
#include "vbpt_log.h"
#include "vbpt_merge.h"
#include "vbpt_mtree.h"
#include "vbpt_tx.h"

#include "tsc.h"
#include "array_size.h"
#include "mt_lib.h"

#if 0
static void __attribute__((unused))
ver_test(void)
{
	ver_t *v0 = ver_create();
	ver_t *v1 = ver_branch(v0);
	ver_t *v2 = ver_branch(v0);
	ver_t *v2a = ver_branch(v2);
	ver_t *x;

	assert(ver_join(v1, v2, &x) == v0 && x == v2);
	assert(ver_join(v1, v2a, &x) == v0 && x == v2);
}
#endif

static void
print_arr(uint64_t *arr, uint64_t arr_len)
{
	for (uint64_t i=0; i < arr_len; i++)
		printf("%lu ", arr[i]);
	printf("\n");
}

static void
vbpt_logtree_insert_bulk(vbpt_tree_t *tree, uint64_t *ins, uint64_t ins_len)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<ins_len; i++) {
		uint64_t key = ins[i];
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_logtree_insert(tree, key, leaf, NULL);
	}
}

static void
vbpt_tree_insert_bulk(vbpt_tree_t *t, uint64_t *ins, uint64_t ins_len)
{
	ver_t *ver = t->ver;
	for (uint64_t i=0; i<ins_len; i++) {
		uint64_t key = ins[i];
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_insert(t, key, leaf, NULL);
	}
}



static bool
vbpt_merge_test(vbpt_tree_t *t,
               uint64_t *ins1, uint64_t ins1_len,
               uint64_t *ins2, uint64_t ins2_len)
{
	vbpt_tree_t *logt1 = vbpt_logtree_branch(t);
	vbpt_logtree_insert_bulk(logt1, ins1, ins1_len);

	vbpt_tree_t *logt2_a = vbpt_logtree_branch(t);
	TSC_MEASURE_TICKS(t_ins2_a, {
		vbpt_logtree_insert_bulk(logt2_a, ins2, ins2_len);
	});

	vbpt_tree_t *logt2_b = vbpt_logtree_branch(t);
	TSC_MEASURE_TICKS(t_ins2_b, {
		vbpt_logtree_insert_bulk(logt2_b, ins2, ins2_len);
	})

	#if 0
	dmsg("PARENT: "); vbpt_tree_print(t, true);
	dmsg("T1:     "); vbpt_tree_print(logt1, true);
	dmsg("T2:     "); vbpt_tree_print(logt2_a, true);
	#endif

	unsigned log_ret;
	TSC_MEASURE_TICKS(t_merge_log, {
		log_ret = vbpt_log_merge(logt1, logt2_a);
	})

	unsigned mer_ret;
	TSC_MEASURE_TICKS(t_merge_vbpt, {
		mer_ret = vbpt_merge(logt1, logt2_b, NULL);
	})

	bool success = false;
	int err = 0;

	const char *xerr;
	switch (log_ret + (mer_ret<<1)) {
		case 0:
		//printf("Both merges failed\n");
		break;

		case 1:
		//printf("Only log_merge succeeded\n");
		break;

		case 2:
		xerr = "merge succeeded, but log_merge failed";
		err = 1;
		break;

		case 3:
		//printf("Both merges succeeded\n");
		if (!vbpt_cmp(logt2_a, logt2_b)) {
			xerr = "resulting trees are not the same\n";
			err = 1;
		}
		success = true;
		break;

		default:
		assert(false);
	}

	if (0 || err) {
		printf("INITIAL  : "); vbpt_tree_print(t, true);
		printf("\n");
		printf("INS1     : "); print_arr(ins1, ins1_len);
		printf("INS2     : "); print_arr(ins2, ins2_len);
		printf("\n");
		printf("LOG MERGE: "); vbpt_tree_print(logt2_a, true);
		printf("BPT MERGE: "); vbpt_tree_print(logt2_b, true);
	}

	if (err) {
		printf("FAIL: %s", xerr);
		assert(false);
	}

	#define print_ticks(x, base)\
		printf("%-13s %5lu (%0.3lf)\n", \
		       "" # x ":", x, (double)base/(double)x);
	if (success) {
		printf("----\n");
		print_ticks(t_ins2_a, t_ins2_a);
		print_ticks(t_ins2_b, t_ins2_a);
		print_ticks(t_merge_log, t_ins2_a);
		print_ticks(t_merge_vbpt, t_ins2_a);
		printf("----\n");
	}
	#undef print_ticks

	return success;
}


static bool __attribute__((unused))
test1(void)
{
	uint64_t keys0[] = {42, 100};
	uint64_t keys1[] = {66, 99, 200};
	uint64_t keys2[] = {11};

	vbpt_tree_t *t = vbpt_tree_create();
	vbpt_tree_insert_bulk(t, keys0, ARRAY_SIZE(keys0));

	return vbpt_merge_test(t, keys1, ARRAY_SIZE(keys1), keys2, ARRAY_SIZE(keys2));
}

static bool __attribute__((unused))
test2(void)
{
	uint64_t keys0[] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120,
	130, 140, 150, 160, 170, 180, 190, 200};
	uint64_t keys1[] = {0, 1, 2};
	uint64_t keys2[] = {71, 73};

	vbpt_tree_t *t = vbpt_tree_create();
	vbpt_tree_insert_bulk(t, keys0, ARRAY_SIZE(keys0));

	return vbpt_merge_test(t, keys1, ARRAY_SIZE(keys1), keys2, ARRAY_SIZE(keys2));
}

// distribution description
struct dist_desc {
	uint64_t r_start;
	uint64_t r_len;
	uint64_t nr;
	unsigned int seed;
};

static inline  uint64_t
dist_rand(struct dist_desc *d, unsigned *seed)
{
	if (d->r_len > RAND_MAX) {
		assert(false && "FIXME");
	}

	uint64_t r = rand_r(seed);
	return d->r_start + (r % d->r_len);
}

static void
generate_keys(struct dist_desc *d, uint64_t **data_ptr, uint64_t *data_len)
{
	if (d->r_len > RAND_MAX) {
		assert(false && "FIXME");
	}

	uint64_t *data = *data_ptr;
	unsigned int seed = d->seed;
	uint64_t dlen = sizeof(uint64_t)*d->nr;
	if (data == NULL) {
		data = xmalloc(dlen);
	} else if (*data_len != dlen) {
		data = xrealloc(data, dlen);
	}
	*data_len = dlen;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t r = (seed == 0) ? i : rand_r(&seed);
		data[i] = d->r_start + (r % d->r_len);
	}

	*data_ptr = data;
}

static bool __attribute__((unused))
test_merge_rand(struct dist_desc *d0, struct dist_desc *d1, struct dist_desc *d2)
{
	uint64_t *data0=NULL, dlen0, *data1=NULL, dlen1, *data2=NULL, dlen2;
	generate_keys(d0, &data0, &dlen0);
	generate_keys(d1, &data1, &dlen1);
	generate_keys(d2, &data2, &dlen2);

	vbpt_tree_t *t = vbpt_tree_create();
	vbpt_tree_insert_bulk(t, data0, d0->nr);

	return vbpt_merge_test(t, data1, d1->nr, data2, d2->nr);
}

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
	struct dist_desc        *wl;
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

static void
vbpt_logtree_insert_rand(vbpt_tree_t *tree, struct dist_desc *d, unsigned *seed)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = dist_rand(d, seed);
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_logtree_insert(tree, key, leaf, NULL);
	}
}

static void
vbpt_tree_insert_rand(vbpt_tree_t *tree, struct dist_desc *d, unsigned *seed)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t key = dist_rand(d, seed);
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_insert(tree, key, leaf, NULL);
	}
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
				vbpt_logtree_insert_rand(txt->tree, arg->wl, &seed);
			})
			TSC_ADD_TICKS(arg->stats.finalize_ticks, {
				vbpt_logtree_finalize(txt->tree);
			})

			arg->stats.commit_attempts++;
			vbpt_txt_res_t ret;
			TSC_ADD_TICKS(arg->stats.commit_ticks, {
				//ret = vbpt_txt_try_commit(txt, mtree, 2);
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
                   struct dist_desc *wls)
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
do_test_mt_rand(struct dist_desc *d0,
                unsigned nthreads, unsigned *cpus,
                struct dist_desc *ds)
{

	printf(" I> start:%6lu len:%6lu nr:%6lu seed:%u\n",
	       d0->r_start, d0->r_len, d0->nr, d0->seed);
	for (unsigned i=0; i<nthreads; i++) {
		struct dist_desc *d = ds + i;
		printf("%2d> start:%6lu len:%6lu nr:%6lu seed:%u\n",
		       i, d->r_start, d->r_len, d->nr, d->seed);
	}


	vbpt_tree_t *tree = vbpt_tree_create();
	unsigned seed = d0->seed;
	vbpt_tree_insert_rand(tree, d0, &seed);

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

	struct dist_desc d0 = {
		.r_start = 0,
		.r_len   = d0_len,
		.nr      = d0_nr,
		.seed    = 1
	};
	struct dist_desc dt[nr_threads];

	const unsigned long part_len = d0_len / nr_threads;
	assert(part_len > d_len);
	for (unsigned i=0; i<nr_threads; i++) {
		struct dist_desc *d = dt + i;
		d->r_start = part_len*i;
		d->r_len   = d_len;
		d->nr      = d_nr;
		d->seed = 1;
	}

	do_test_mt_rand(&d0, nr_threads, cpus, dt);
}

static void __attribute__((used))
do_serial_test(void)
{
	struct dist_desc d0 = { .r_start =   0,    .r_len =16384, .nr = 1024, .seed = 0};
	struct dist_desc d1 = { .r_start =   0,    .r_len =  128, .nr =   16, .seed = 0};
	struct dist_desc d2 = { .r_start =   4096, .r_len =  128, .nr =   16, .seed = 0};
	unsigned count=0, successes=0;

	void my_test(unsigned i, unsigned k, unsigned j) {
		d0.seed = i;
		d1.seed = j;
		d2.seed = k;
		printf("Testing %u %u %u\n", i, j, k);
		successes += test_merge_rand(&d0, &d1, &d2);
		count++;
	}
	my_test(1, 0, 0);

	// XXX: This test works well for VBPT_NODE_SIZE=128
	// VBPT_NODE_SIZE=128: ------> Count: 16384 Successes: 14513
	// VBPT_NODE_SIZE=512: ------> Count: 16384 Successes: 2489
	// need to investigate more
	const int xsize = 128;
	for (unsigned i=0; i<xsize; i++)
		for (unsigned j=0; j<xsize; j++)
			for (unsigned k=0; j<xsize; j++)
				my_test(i, j, k);
	printf("------> Count: %u Successes: %u\n", count, successes);
}

int main(int argc, const char *argv[])
{
	//test1();
	//test2();

	//do_serial_test();
	#if 0
	struct dist_desc d0 = { .r_start= 0, .r_len =16384, .nr = 4096, .seed = 1};
	struct dist_desc ds[] = {
		{ .r_start =     0,  .r_len = 128,  .nr =16, .seed = 1},
		{ .r_start =  1024,  .r_len = 128,  .nr =16, .seed = 1},
		{ .r_start =  1024,  .r_len = 128,  .nr =16, .seed = 1},
		 { .r_start = 16384,  .r_len = 128, .nr =16, .seed = 1}
	};
	//test_merge_rand(&d0, ds+0, ds+1);
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
