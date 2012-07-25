
#include "ver.h"
#include "vbpt.h"
#include "vbpt_log.h"
#include "vbpt_merge.h"
#include "array_size.h"

#include "tsc.h"

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
vbpt_txtree_insert_bulk(vbpt_tree_t *tree, uint64_t *ins, uint64_t ins_len)
{
	ver_t *ver = tree->ver;
	for (uint64_t i=0; i<ins_len; i++) {
		uint64_t key = ins[i];
		vbpt_leaf_t *leaf = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
		vbpt_txtree_insert(tree, key, leaf, NULL);
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
	tsc_t tsc;

	vbpt_tree_t *txt1 = vbpt_txtree_branch(t);
	vbpt_txtree_insert_bulk(txt1, ins1, ins1_len);

	vbpt_tree_t *txt2_a = vbpt_txtree_branch(t);

	tsc_init(&tsc); tsc_start(&tsc);
	vbpt_txtree_insert_bulk(txt2_a, ins2, ins2_len);
	tsc_pause(&tsc); uint64_t t_ins2_a = tsc_getticks(&tsc);

	vbpt_tree_t *txt2_b = vbpt_txtree_branch(t);

	tsc_init(&tsc); tsc_start(&tsc);
	vbpt_txtree_insert_bulk(txt2_b, ins2, ins2_len);
	tsc_pause(&tsc); uint64_t t_ins2_b = tsc_getticks(&tsc);

	#if 0
	dmsg("PARENT: "); vbpt_tree_print(t, true);
	dmsg("T1:     "); vbpt_tree_print(txt1, true);
	dmsg("T2:     "); vbpt_tree_print(txt2_a, true);
	#endif

	tsc_init(&tsc); tsc_start(&tsc);
	unsigned log_ret = vbpt_log_merge(txt1, txt2_a);
	tsc_pause(&tsc); uint64_t t_merge_log = tsc_getticks(&tsc);

	tsc_init(&tsc); tsc_start(&tsc);
	unsigned mer_ret = vbpt_merge(txt1, txt2_b);
	tsc_pause(&tsc); uint64_t t_merge_vbpt = tsc_getticks(&tsc);

	bool success = false;
	int err = 0;
	switch (log_ret + (mer_ret<<1)) {
		case 0:
		//printf("Both merges failed\n");
		break;

		case 1:
		//printf("Only log_merge succeeded\n");
		break;

		case 2:
		//printf("ERROR: merge succeeded, but log_merge failed\n");
		err = 1;
		break;

		case 3:
		//printf("Both merges succeeded\n");
		if (!vbpt_cmp(txt2_a, txt2_b)) {
			printf("======> Resulting trees are not the same\n");
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
		printf("LOG MERGE: "); vbpt_tree_print(txt2_a, true);
		printf("BPT MERGE: "); vbpt_tree_print(txt2_b, true);
	}

	if (err)
		assert(false);

	if (success) {
		printf("----\n");
		printf("t_ins2_a:     %5lu (%0.3lf)\n", t_ins2_a,     ((double)t_ins2_a / (double)t_ins2_a)    );
		printf("t_ins2_b:     %5lu (%0.3lf)\n", t_ins2_b,     ((double)t_ins2_a / (double)t_ins2_b)    );
		printf("t_merge_log:  %5lu (%0.3lf)\n", t_merge_log,  ((double)t_ins2_a / (double)t_merge_log) );
		printf("t_merge_vbpt: %5lu (%0.3lf)\n", t_merge_vbpt, ((double)t_ins2_a / (double)t_merge_vbpt));
		printf("----\n");
	}

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
	uint64_t *data;
	uint64_t data_len;
};

static void
generate_keys(struct dist_desc *d)
{
	if (d->r_len > RAND_MAX) {
		assert(false && "FIXME");
	}

	unsigned int seed = d->seed;
	uint64_t data_len = sizeof(uint64_t)*d->nr;
	if (d->data == NULL) {
		d->data = xmalloc(data_len);
	} else if (d->data_len != data_len) {
		d->data = xrealloc(d->data, data_len);
	}
	d->data_len = data_len;
	for (uint64_t i=0; i<d->nr; i++) {
		uint64_t r = (seed == 0) ? i : rand_r(&seed);
		d->data[i] = d->r_start + (r % d->r_len);
	}
}

static bool
test_merge_rand(struct dist_desc *d0, struct dist_desc *d1, struct dist_desc *d2)
{
	generate_keys(d0);
	generate_keys(d1);
	generate_keys(d2);

	vbpt_tree_t *t = vbpt_tree_create();
	vbpt_tree_insert_bulk(t, d0->data, d0->nr);

	return vbpt_merge_test(t, d1->data, d1->nr, d2->data, d2->nr);
}

int main(int argc, const char *argv[])
{
	//test1();
	//test2();
	#if 1
	struct dist_desc d0 = { .r_start =   0, .r_len =16384, .nr = 1024,  .seed = 0, .data = NULL};
	struct dist_desc d1 = { .r_start =   0, .r_len = 128, .nr =   16, .seed = 0, .data = NULL};
	struct dist_desc d2 = { .r_start =   4096, .r_len = 128, .nr =  16, .seed = 0, .data = NULL};

	unsigned count=0, successes=0;
	for (unsigned i=0; i<128; i++)
		for (unsigned j=0; j<128; j++)
			for (unsigned k=0; j<128; j++) {
				d0.seed = i;
				d1.seed = j;
				d2.seed = k;
				//printf("Testing %u %u %u\n", i, j, k);
				successes += test_merge_rand(&d0, &d1, &d2);
				count++;
			}
	printf("------> Count: %u Successes: %u\n", count, successes);
	#endif

	return 0;
}
