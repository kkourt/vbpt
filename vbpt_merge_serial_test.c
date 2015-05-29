/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */


#include "vbpt.h"
#include "vbpt_test.h"

#include "xdist.h"
#include "array_size.h"

static void
print_arr(uint64_t *arr, uint64_t arr_len)
{
	for (uint64_t i=0; i < arr_len; i++)
		printf("%lu ", arr[i]);
	printf("\n");
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

	if (err) {
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
test_merge_rand(struct xdist_desc *d0, struct xdist_desc *d1, struct xdist_desc *d2)
{
	uint64_t *data0=NULL, dlen0, *data1=NULL, dlen1, *data2=NULL, dlen2;
	xdist_generate_keys(d0, &data0, &dlen0);
	xdist_generate_keys(d1, &data1, &dlen1);
	xdist_generate_keys(d2, &data2, &dlen2);

	vbpt_tree_t *t = vbpt_tree_create();
	vbpt_tree_insert_bulk(t, data0, d0->nr);

	return vbpt_merge_test(t, data1, d1->nr, data2, d2->nr);
}



static void __attribute__((used))
do_serial_test(void)
{
	struct xdist_desc d0 = { .r_start =      0, .r_len =16384, .nr = 1024, .seed = 0};
	struct xdist_desc d1 = { .r_start =      0, .r_len =  128, .nr =   16, .seed = 0};
	struct xdist_desc d2 = { .r_start =   4096, .r_len =  128, .nr =   16, .seed = 0};
	unsigned count=0, successes=0;

	void do_test(unsigned i, unsigned k, unsigned j) {
		d0.seed = i;
		d1.seed = j;
		d2.seed = k;
		printf("Testing %u %u %u\n", i, j, k);
		successes += test_merge_rand(&d0, &d1, &d2);
		count++;
	}
	do_test(1, 0, 0);

	// XXX: This test works well for VBPT_NODE_SIZE=128
	// VBPT_NODE_SIZE=128: ------> Count: 16384 Successes: 14513
	// VBPT_NODE_SIZE=512: ------> Count: 16384 Successes: 2489
	// need to investigate more
	const int xsize = 128;
	for (unsigned i=0; i<xsize; i++)
		for (unsigned j=0; j<xsize; j++)
			for (unsigned k=0; j<xsize; j++)
				do_test(i, j, k);
	printf("------> Count: %u Successes: %u\n", count, successes);
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

int main(int argc, const char *argv[])
{
	#if 0
	test1();
	test2();
	struct dist_desc d0 = { .r_start= 0, .r_len =16384, .nr = 4096, .seed = 1};
	struct dist_desc ds[] = {
		{ .r_start =     0,  .r_len = 128,  .nr =16, .seed = 1},
		{ .r_start =  1024,  .r_len = 128,  .nr =16, .seed = 1},
		{ .r_start =  1024,  .r_len = 128,  .nr =16, .seed = 1},
		 { .r_start = 16384,  .r_len = 128, .nr =16, .seed = 1}
	};
	//test_merge_rand(&d0, ds+0, ds+1);
	#endif

	do_serial_test();
	return 0;
}
