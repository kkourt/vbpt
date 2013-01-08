#ifndef XCNT_H__
#define XCNT_H__

/* counters */

#include <inttypes.h>

#define XCNT_MINMAX

struct xcnt {
	uint64_t total; // total value of counter
	uint64_t cnt;   // number of additions
	#if defined(XCNT_MINMAX)
	uint64_t min;
	uint64_t max;
	#endif
};
typedef struct xcnt xcnt_t;

static inline void
xcnt_init(xcnt_t *xcnt)
{
	xcnt->total = 0;
	xcnt->cnt = 0;
	#if defined(XCNT_MINMAX)
	xcnt->min = UINT64_MAX;
	xcnt->max = 0;
	#endif
}

static inline void
xcnt_add(xcnt_t *xcnt, uint64_t val)
{
	xcnt->total += val;
	xcnt->cnt++;
	#if defined(XCNT_MINMAX)
	if (val > xcnt->max)
		xcnt->max = val;
	// XXX: there is code that initializes xcnt by zeroing everything
	// (i.e., without calling xcnt_init())
	if (val < xcnt->min || xcnt->min == 0)
		xcnt->min = val;
	#endif
}

static inline void
xcnt_inc(xcnt_t *xcnt)
{
	xcnt_add(xcnt, 1);
}


static inline double
xcnt_avg(xcnt_t *xcnt)
{
	return (double)xcnt->total/(double)xcnt->cnt;
}


static uint64_t
xcnt_max(xcnt_t *xcnt)
{
	#if defined(XCNT_MINMAX)
	return xcnt->max;
	#else
	return xcnt->total/xcnt->cnt;
	#endif
}

static uint64_t
xcnt_min(xcnt_t *xcnt)
{
	#if defined(XCNT_MINMAX)
	return xcnt->min;
	#else
	return xcnt->total/xcnt->cnt;
	#endif
}


static inline char *
xcnt_u64_hstr(uint64_t ul)
{
	#define UL_HSTR_NR 16
	static __thread int i=0;
	static __thread char buffs[UL_HSTR_NR][16];
	char *b = buffs[i++ % UL_HSTR_NR];
	#undef UL_HSTR_NR

	char *m;
	double t;
	if (ul < 1000) {
		m = " ";
		t = (double)ul;
	} else if (ul < 1000*1000) {
		m = "K";
		t = (double)ul/(1000.0);
	} else if (ul < 1000*1000*1000) {
		m = "M";
		t = (double)ul/(1000.0*1000.0);
	} else {
		m = "G";
		t = (double)ul/(1000.0*1000.0*1000.0);
	}

	snprintf(b, 16, "%5.1lf%s", t, m);
	return b;
}

static inline void
xcnt_report(const char *prefix, xcnt_t *xcnt)
{
	printf("%-20s XCNT total:%7s [%13"PRIu64"]"
	                   " cnt:%7s [%13"PRIu64"]"
	                   " min:%7s [%13"PRIu64"]"
	                   " max:%7s [%13"PRIu64"]"
	                   " avg:%10.2lf\n",
	         prefix,
	         xcnt_u64_hstr(xcnt->total), xcnt->total,
	         xcnt_u64_hstr(xcnt->cnt),   xcnt->cnt,
	         xcnt_u64_hstr(xcnt_min(xcnt)), xcnt_min(xcnt),
	         xcnt_u64_hstr(xcnt_max(xcnt)), xcnt_max(xcnt),
	         xcnt_avg(xcnt));
}

#endif
