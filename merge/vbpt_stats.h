#ifndef VBPT_STATS_H_
#define VBPT_STATS_H_

#define VBPT_STATS // enable stats

#include <strings.h> //bzero

#include "tsc.h"
#include "xcnt.h"

typedef struct vbpt_stats vbpt_stats_t;

static inline void vbpt_stats_init(void);
static inline void vbpt_stats_get(vbpt_stats_t *stats);
void               vbpt_stats_report(uint64_t total_ticks);
void               vbpt_stats_do_report(char *prefix,
                                        vbpt_stats_t *st, uint64_t total_ticks);

extern __thread vbpt_stats_t VbptStats;

// should be declared in a single module
#define DECLARE_VBPT_STATS() __thread vbpt_stats_t VbptStats

struct vbpt_merge_stats {
	#if defined(VBPT_STATS)
	uint64_t gc_old;
	uint64_t pc_old;
	uint64_t both_null;
	uint64_t pc_null;
	uint64_t gc_null;
	uint64_t merge_steps;
	uint64_t merge_steps_max;
	uint64_t merges;
	uint64_t join_failed;
	tsc_t    vbpt_merge;
	tsc_t    cur_down;
	tsc_t    cur_next;
	tsc_t    do_merge;
	tsc_t    ver_join;
	tsc_t    ver_rebase;
	tsc_t    cur_sync;
	tsc_t    cur_replace;
	tsc_t    cur_do_replace;
	tsc_t    cur_do_replace_putref;
	tsc_t    cur_init;
	#endif
};

struct vbpt_stats {
	#if defined(VBPT_STATS)
	tsc_t                    vbpt_search;
	tsc_t                    txt_try_commit;
	tsc_t                    mtree_try_commit;
	tsc_t                    logtree_insert;
	tsc_t                    logtree_get;
	tsc_t                    txtree_alloc;
	tsc_t                    txtree_dealloc;
	tsc_t                    file_pread;
	tsc_t                    file_pwrite;
	tsc_t                    cow_leaf_write;
	tsc_t                    vbpt_node_alloc;
	tsc_t                    vbpt_cache_get_node;
	tsc_t                    vbpt_app;
	tsc_t                    ver_tree_gc;
	uint64_t                 commit_ok;
	uint64_t                 commit_fail;
	uint64_t                 commit_merge_ok;
	uint64_t                 commit_merge_fail;
	uint64_t                 merge_ok;
	uint64_t                 merge_fail;
	struct vbpt_merge_stats  m;
	xcnt_t                   ver_tree_gc_iters;
	xcnt_t                   merge_iters;
	#endif
};

static inline void
vbpt_stats_init(void)
{
	bzero(&VbptStats, sizeof(VbptStats));
}

static inline void
vbpt_stats_get(vbpt_stats_t *stats)
{
	*stats = VbptStats;
}

#if defined(VBPT_STATS)
#define VBPT_START_TIMER(_x)  \
	do {                              \
		tsc_start(&VbptStats._x); \
	} while (0)

#define VBPT_XCNT_ADD(_x, val) \
	do { \
		xcnt_add(&VbptStats._x, val); \
	} while (0)

#define VBPT_STOP_TIMER(_x)  \
	do {                               \
		tsc_pause(&VbptStats._x);  \
	} while (0)

#define VBPT_INC_COUNTER(_x)  ((VbptStats._x)++)

#define VBPT_MERGE_START_TIMER(_x)                    \
	do {                                          \
		tsc_start(&VbptStats.m._x); \
	} while (0)

#define VBPT_MERGE_STOP_TIMER(_x)                      \
	do {                                           \
		tsc_pause(&VbptStats.m._x);  \
	} while (0)

#define VBPT_MERGE_INC_COUNTER(_x)     ((VbptStats.m._x)++)
#define VBPT_MERGE_ADD_COUNTER(_x, v)  ((VbptStats.m._x) += v)
#else // !VBPT_STATS
#define VBPT_START_TIMER(_x)  do { ; } while (0)
#define VBPT_STOP_TIMER(_x)   do { ; } while (0)
#define VBPT_INC_COUNTER(_x)  do { ; } while (0)
#define VBPT_MERGE_START_TIMER(_x) do {;} while (0)
#define VBPT_MERGE_STOP_TIMER(_x)  do {;} while (0)
#define VBPT_MERGE_INC_COUNTER(_x) do {;} while (0)
#define VBPT_MERGE_ADD_COUNTER(_x, v)  do {;} while (0)
#endif // VBPT_STATS


#endif // VBPT_STATS_H_
