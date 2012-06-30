#ifndef __PHASH_H__
#define __PHASH_H__

#include <stdbool.h>

/* python hash for C
 *  originally by gtsouk@cslab.ece.ntua.gr
 *  -- kkourt@cslab.ece.ntua.gr
 */

typedef unsigned long ul_t;

struct phash {
    ul_t *kvs; // first go keys, and then vals (if exist)
    ul_t size_shift;
    ul_t minsize_shift;
    ul_t used;
    ul_t dummies;
    ul_t defval;
#ifdef PHASH_STATS
    ul_t inserts;
    ul_t deletes;
    ul_t lookups;
    ul_t bounces;
#endif
};
typedef struct phash phash_t;

static inline void
phash_cp(phash_t *dst, const phash_t *src)
{
    dst->kvs           = src->kvs;
    dst->size_shift    = src->size_shift;
    dst->minsize_shift = src->minsize_shift;
    dst->used          = src->used;
    dst->dummies       = src->dummies;
    dst->defval        = src->defval;
#ifdef PHASH_STATS
    dst->inserts       = src->inserts;
    dst->deletes       = src->deletes;
    dst->lookups       = src->lookups;
    dst->bounces       = src->bounces;
#endif
}

static inline ul_t
phash_elements(phash_t *phash)
{
    return phash->used;
}

static inline ul_t
phash_size(phash_t *phash)
{
    return 1UL<<(phash->size_shift);
}

static inline ul_t *
phash_vals(phash_t *phash)
{
    return phash->kvs + phash_size(phash);
}

#ifdef PHASH_STATS
#define ZEROSTAT(stat) (stat) = 0
#define INCSTAT(stat) (stat) ++
#define DECSTAT(stat) (stat) --
#define REPSTAT(stat)  fprintf(stderr, "" # stat  " = %lu \n" , stat)
#else
#define ZEROSTAT(stat)
#define INCSTAT(stat)
#define DECSTAT(stat)
#define REPSTAT(stat)  do { } while (0)
#endif

#define REPSTATS(x) do {     \
    REPSTAT(x->inserts); \
    REPSTAT(x->deletes); \
    REPSTAT(x->lookups); \
    REPSTAT(x->bounces); \
} while (0)



/**
 * hash functions (dict)
 */

phash_t *phash_new(ul_t minsize_shift);
void phash_free(phash_t *phash); // pairs with _new()

void phash_init(phash_t *phash, ul_t minsize_shift);
void phash_tfree(phash_t *phash); // pairs with _ntfre()

void phash_insert(phash_t *phash, ul_t key, ul_t val);
int phash_update(phash_t *phash, ul_t key, ul_t val);
void phash_freql_update(phash_t *phash, ul_t key, ul_t val);
int phash_delete(struct phash *phash, ul_t key);
int phash_lookup(phash_t *phash, ul_t key, ul_t *val);

struct phash_iter {
    ul_t   loc;  /* location on the array */
    ul_t   cnt;  /* returned items */
};
typedef struct phash_iter phash_iter_t;

/* The iterators are read-only */
void phash_iter_init(phash_t *phash, phash_iter_t *pi);
int  phash_iterate(phash_t *phash, phash_iter_t *pi, ul_t *key, ul_t *val);

void phash_print(phash_t *phash);

/**
 * set funtions
 */

typedef phash_t pset_t;

static inline ul_t
pset_elements(pset_t *pset)
{
    return pset->used;
}

static inline ul_t
pset_size(pset_t *pset)
{
    return 1UL<<(pset->size_shift);
}

pset_t *pset_new(ul_t minsize_shift); // returns an initialized pset
void pset_free(pset_t *pset); // goes with _new()

void pset_init(pset_t *pset, ul_t minsize_shift);
void pset_tfree(pset_t *pset); // goes with _init()

void pset_insert(pset_t *pset, ul_t key);
int pset_delete(pset_t *pset, ul_t key);
bool pset_lookup(pset_t *pset, ul_t key);
int pset_iterate(pset_t *pset, phash_iter_t *pi, ul_t *key);
void pset_print(pset_t *pset);

typedef phash_iter_t pset_iter_t;

static inline void
pset_iter_init(pset_t *pset, pset_iter_t *pi)
{
    phash_iter_init(pset, pi);
}

#endif

// vim:expandtab:tabstop=8:shiftwidth=4:softtabstop=4
