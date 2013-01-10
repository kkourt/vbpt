#include "vbpt.h"
#include "vbpt_stats.h"

#if 0
void vbpt_merge_stats_do_report(char *prefix, vbpt_merge_stats_t *st)
{
	#define pr_stat(x__) \
		printf("%s" # x__ ": %lu\n", prefix, st->x__)

	#define pr_merge_ratio(x__) \
		printf("%s" # x__ ": %lu  " #x__ "/merge: %.2lf\n", \
		       prefix, st->x__, (double)st->x__ / (double)st->merges)

	#define pr_ticks(x__) do { \
		double p__ = (double)st->x__ / (double)st->merge_ticks; \
		if (p__ < 0.1) \
			break; \
		printf("%s" # x__ ": %lu (%.1lf%%)\n", prefix, st->x__, p__*100); \
	} while (0)

	#define pr_ticks2(ticks__, cnt__) do { \
		unsigned long t__ = st->ticks__;                   \
		double p__ =  t__ / (double)st->merge_ticks;       \
		if (p__ < 0.1)                                     \
			break;                                     \
		unsigned long c__ = st->cnt__;                     \
		unsigned long a__ = c__ ? t__ / c__ : 0;           \
		printf("%s" # ticks__ ": %lu (%.2lf%%) cnt:%lu (%lu ticks/call)\n",\
		        prefix, t__, p__, c__, a__);               \
	} while (0)

	pr_stat(merges);
	pr_merge_ratio(merge_steps);
	printf("%smerge_steps_max:%lu\n", prefix, st->merge_steps_max);
	printf("%smerge_ticks_max:%lu\n", prefix, st->merge_ticks_max);

	#undef pr_stat
	#undef pr_merge_ratio
	#undef pr_ticks
	#undef pr_ticks2
}
#endif


void vbpt_stats_do_report(char *prefix, vbpt_stats_t *st, uint64_t total_ticks)
{
	#define pr_ticks(x__) do { \
		uint64_t t__ = tsc_getticks(&st->x__);  \
		uint64_t c__ = st->x__.cnt;             \
		double p__ = t__ / (double)total_ticks; \
		if (p__ <-0.1 || c__ == 0) \
			break; \
		printf("%s %24s: " \
		       "%8.1lfM (%6.1lf%%) "  \
		       "cnt:%9lu (avg:%7.2lfK min:%7.2lfK max:%7.2lfK)\n", \
		        prefix, "" #x__, \
		        t__/(1000*1000.0), p__*100, \
		        c__, \
		        tsc_getticks_avg(&st->x__) / 1000.0,  \
		        tsc_getticks_min(&st->x__) / 1000.0,  \
		        tsc_getticks_max(&st->x__) / 1000.0); \
	} while (0)

	#define pr_cnt(x__) \
		printf("%s" "%24s" ": %lu\n", prefix, "" #x__, st->x__)

	#define pr_xcnt(x__) do { \
		if (st->x__.cnt > 0) \
			xcnt_report("" #x__, &st->x__); \
	} while (0)

	#if defined(VBPT_STATS)
	pr_ticks(vbpt_app);
	pr_ticks(txt_try_commit);
	pr_ticks(mtree_try_commit);
	pr_ticks(txtree_alloc);
	pr_ticks(ver_tree_gc);
	pr_ticks(file_pread);
	pr_ticks(file_pwrite);
	pr_ticks(vbpt_cache_get_node);
	pr_ticks(vbpt_search);
	pr_ticks(txtree_dealloc);
	pr_ticks(logtree_insert);
	pr_ticks(logtree_get);
	pr_ticks(cow_leaf_write);
	pr_ticks(m.vbpt_merge);
	pr_ticks(m.cur_do_replace);
	pr_ticks(m.cur_do_replace_putref);
	pr_ticks(m.cur_down);
	pr_ticks(m.cur_next);
	pr_ticks(m.do_merge);
	pr_ticks(m.ver_join);
	pr_ticks(m.ver_rebase);
	pr_ticks(m.cur_sync);
	pr_ticks(m.cur_replace);
	pr_ticks(m.cur_init);

	//pr_cnt(commit_ok);
	//pr_cnt(commit_fail);
	//pr_cnt(commit_merge_ok);
	//pr_cnt(commit_merge_fail);
	//pr_cnt(merge_ok);
	//pr_cnt(merge_fail);
	//pr_cnt(m.gc_old);
	//pr_cnt(m.pc_old);
	//pr_cnt(m.both_null);
	//pr_cnt(m.pc_null);
	//pr_cnt(m.gc_null);
	//pr_cnt(m.join_failed);

	pr_xcnt(ver_tree_gc_iters);
	pr_xcnt(merge_iters);
	#endif

	#undef pr_ticks
	#undef pr_cnt
}

void vbpt_stats_report(uint64_t total_ticks)
{
	tmsg("VBPT stats\n");
	vbpt_stats_do_report("  ", &VbptStats, total_ticks);
}
