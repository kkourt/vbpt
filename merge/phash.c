#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <stdbool.h>

/* python hash for C
 *  originally by gtsouk@cslab.ece.ntua.gr
 *  -- kkourt@cslab.ece.ntua.gr
 */


#include "phash.h"

#define UNUSED (~(ul_t)0)      /* this entry was never used */
#define DUMMY  ((~(ul_t)0)-1)  /* this entry was used, but now its empty */

#define VAL_OVERLOAD
//#define KEY_OVERLOAD
//#define NO_OVERLOAD /* use separate bitarray -- not implemented */

#define PERTURB_SHIFT 5

static ul_t *
kvs_new(ul_t nr_items, bool vals)
{
    ul_t i;
    size_t keys_size = nr_items*sizeof(ul_t);
    size_t alloc_size = vals ? keys_size<<2 : keys_size;
    ul_t *kvs = malloc(alloc_size);
    if (!kvs) {
        perror("malloc");
        exit(1);
    }

    if (!vals) {
        for (i=0; i < nr_items; i++)
            kvs[i] = UNUSED;
        return kvs;
    }

    for (i=0; i < nr_items; i++){
        #if defined(VAL_OVERLOAD)
        assert(vals);
        kvs[nr_items + i] = UNUSED;
        #elif  defined(KEY_OVERLOAD)
        kvs[i] = UNUSED;
        #endif
    }

    return kvs;
}

static inline void
set_dummy_key(phash_t *phash, ul_t idx)
{
    phash->kvs[idx] = DUMMY;
}

static inline void
set_dummy_val(phash_t *phash, ul_t idx)
{
    ul_t *vals = phash_vals(phash);
    vals[idx] = DUMMY;
}

static bool
item_dummy(phash_t *phash, ul_t idx, bool vals)
{
    bool ret;
    if (!vals) {
        ret = (phash->kvs[idx] == DUMMY);
    } else {
        #if defined(VAL_OVERLOAD)
        assert(vals);
        ul_t *vals = phash_vals(phash);
        ret = (vals[idx] == DUMMY);
        #elif defined(KEY_OVERLOAD)
        ret = (phash->kvs[idx] == DUMMY);
        #endif
    }
    return ret;

}

static void set_dummy_item(phash_t *phash, ul_t idx, bool vals)
{
    if (!vals) {
        set_dummy_key(phash, idx);
        return;
    }

    #ifdef VAL_OVERLOAD
    assert(vals);
    set_dummy_val(phash, idx);
    return;
    #elif defined(KEY_OVERLOAD)
    set_dummy_key(phash, idx);
    return;
    #endif
}
static inline void
set_unused_key(phash_t *phash, ul_t idx)
{
    phash->kvs[idx] = UNUSED;
}

static inline void
set_unused_val(phash_t *phash, ul_t idx)
{
    ul_t *vals = phash_vals(phash);
    vals[idx] = UNUSED;
}

static inline bool
val_unused(phash_t *phash, ul_t idx)
{
    ul_t *vals = phash_vals(phash);
    return vals[idx] == UNUSED;
}

static bool
item_unused(phash_t *phash, ul_t idx, bool vals)
{
    if (!vals) {
        return phash->kvs[idx] == UNUSED;
    }

    #if defined(VAL_OVERLOAD)
    assert(vals);
    return val_unused(phash, idx);
    #elif defined(KEY_OVERLOAD)
    return phash->kvs[idx] == UNUSED;
    #endif

}

static inline unsigned item_valid(phash_t *phash, ul_t idx, bool vals)
{
    return !(item_dummy(phash, idx, vals) || item_unused(phash, idx, vals));
}

static void __attribute__((unused))
assert_key(ul_t key)
{
    assert((key != UNUSED) && (key != DUMMY));
}

static void
assert_val(ul_t val)
{
    assert((val != UNUSED) && (val != DUMMY));
}

static inline void
assert_kv(ul_t k, ul_t v)
{
    #if defined(KEY_OVERLOAD)
    assert_key(k);
    #elif defined(VAL_OVERLOAD)
    assert_val(v);
    #endif
}

void
phash_init__(phash_t *phash, ul_t minsize_shift, bool vals)
{
    phash->kvs = kvs_new(1UL<<minsize_shift, vals);
    phash->dummies = phash->used = 0;
    phash->size_shift = phash->minsize_shift = minsize_shift;

    ZEROSTAT(phash->inserts);
    ZEROSTAT(phash->deletes);
    ZEROSTAT(phash->lookups);
    ZEROSTAT(phash->bounces);
}


phash_t *
phash_new__(ul_t minsize_shift, bool vals) {
    struct phash *phash;
    phash = malloc(sizeof(struct phash));
    if (!phash) {
        perror("malloc");
        exit(1);
    }
    phash_init__(phash, minsize_shift, vals);
    return phash;
}


void
phash_resize__(struct phash *phash, ul_t new_size_shift, bool vals)
{
    ul_t new_size = (ul_t)1UL<<new_size_shift;

    phash->kvs = kvs_new(new_size, vals);
    phash->dummies = phash->used = 0;
    phash->size_shift = new_size_shift;
}

static ul_t
grow_size_shift(phash_t *phash)
{
    ul_t old_size_shift = phash->size_shift;
    ul_t new_size_shift;
    ul_t u;

    u = phash->used;
    if (u/2 + u >= ((ul_t)1 << old_size_shift)) {
        new_size_shift = old_size_shift + 1;
    } else {
        new_size_shift = old_size_shift;
    }

    return new_size_shift;
}

static ul_t
shrink_size_shift(phash_t *phash)
{
    ul_t old_size_shift = phash->size_shift;
    ul_t new_size_shift;
    new_size_shift = old_size_shift - 1;
    if (new_size_shift < phash->minsize_shift) {
        new_size_shift = phash->minsize_shift;
    }
    return new_size_shift;
}

static bool
grow_check(phash_t *phash)
{
    ul_t size_shift = phash->size_shift;
    ul_t u = phash->used + phash->dummies;
    ul_t size = (ul_t)1UL<<size_shift;
    return ((u/2 + u) >= size) ? true : false;
}

int
phash_delete__(phash_t *phash, ul_t key, bool vals)
{
    ul_t perturb = key;
    ul_t mask = phash_size(phash)-1;
    ul_t idx = key & mask;
    for (;;) {
        if ( item_unused(phash, idx, vals) ){
            assert(0);
            return 0;
        }

        if ( !item_dummy(phash, idx, vals) && phash->kvs[idx] == key){
            INCSTAT(phash->deletes);
            set_dummy_item(phash, idx, vals);
            phash->dummies++;
            //fprintf(stderr, "rm: used: %lu\n", phash->used);
            phash->used--;
            return 1;
        }

        INCSTAT(phash->bounces);
        idx = ((idx<<2) + idx + 1 + perturb) & mask;
        perturb >>= PERTURB_SHIFT;
    }
}

/**
 * Phash functions
 */

phash_t *
phash_new(ul_t minsize_shift)
{
    return phash_new__(minsize_shift, true);
}

void
phash_init(phash_t *phash, ul_t minsize_shift)
{
    phash_init__(phash, minsize_shift, true);
}

void
phash_tfree(phash_t *phash)
{
    REPSTATS(phash);
    free(phash->kvs);
}

void phash_free(struct phash *phash)
{

    REPSTATS(phash);
    free(phash->kvs);
    free(phash);
}

void
phash_resize(phash_t *phash, ul_t new_size_shift)
{
    phash_t  old;
    phash_cp(&old, phash);

    phash_resize__(phash, new_size_shift, true);
    for (ul_t i = 0; i < phash_size(&old); i++) {
        if (item_valid(&old, i, true)){
            //fprintf(stderr, "rs: inserting (%lu,%lu)\n", item->k, item->v);
            phash_insert(phash, old.kvs[i], *(phash_vals(&old) + i));
        }
    }

    free(old.kvs);
}


void
phash_grow(struct phash *phash)
{
    ul_t new_size_shift = grow_size_shift(phash);
    phash_resize(phash, new_size_shift);
}

void
phash_shrink(struct phash *phash)
{
    ul_t new_size_shift = shrink_size_shift(phash);
    phash_resize(phash, new_size_shift);
}

static inline void
phash_grow_check(phash_t *phash)
{
    if (grow_check(phash))
        phash_grow(phash);
}

#define PHASH_UPDATE(phash, key, val, vals_flag)      \
{                                                     \
    ul_t size = 1UL<<(phash->size_shift);             \
    ul_t perturb = key;                               \
    ul_t mask = size-1;                               \
    ul_t idx = key & mask;                            \
                                                      \
    INCSTAT(phash->inserts);                          \
    for (;;) {                                        \
        if ( !item_valid(phash, idx, vals_flag) ){    \
             PHUPD_SET__(phash, idx, key, val);       \
             break;                                   \
        }                                             \
        if (phash->kvs[idx] == key){                  \
            PHUPD_UPDATE__(phash, idx, key, val);     \
            break;                                    \
        }                                             \
                                                      \
        again: __attribute__((unused))                \
        INCSTAT(phash->bounces);                      \
        idx = ((idx<<2) + idx + 1 + perturb) & mask;  \
        perturb >>= PERTURB_SHIFT;                    \
    }                                                 \
}

static inline void
set_val(phash_t *p, ul_t idx, ul_t key, ul_t val)
{
    p->kvs[idx] = key;
    ul_t *vals = phash_vals(p);
    vals[idx] = val;
}

void static inline phash_upd_set(phash_t *p, ul_t idx, ul_t key, ul_t val)
{
    if (item_dummy(p, idx, true))
        p->dummies--;
    p->used++;
    p->kvs[idx] = key;
    ul_t *vals = phash_vals(p);
    vals[idx] = val;
}

static inline void
inc_val(phash_t *p, ul_t idx, ul_t val)
{
    ul_t *vals = phash_vals(p);
    vals[idx] += val;
}

void phash_insert(struct phash *phash, ul_t key, ul_t val)
{

    //fprintf(stderr, "insert: (%lu,%lu)\n", key, val);
    assert_kv(key, val);
    phash_grow_check(phash);
    #define PHUPD_UPDATE__(_p, _i, _k, _v) set_val(_p, _i, _k, _v)
    #define PHUPD_SET__(_p, _i, _k, _v)    phash_upd_set(_p, _i, _k, _v)
    PHASH_UPDATE(phash, key, val, true)
    #undef PHUPD_UPDATE__
    #undef PHUPD_SET__
}


void phash_freql_update(struct phash *phash, ul_t key, ul_t val)
{
    assert_kv(key, val);
    assert_val(val);
    phash_grow_check(phash);
    #define PHUPD_UPDATE__(_p, _i, _k, _v) inc_val(_p, _i, _v)
    #define PHUPD_SET__(_p, _i, _k, _v)    phash_upd_set(_p, _i, _k, _v)
    PHASH_UPDATE(phash, key, val, true)
    #undef PHUPD_UPDATE__
    #undef PHUPD_SET__
}

/*
 * note that his function does not modify the internal structure of the hash
 * and thus its safe to use it for updating values during a phash_iterate()
 */
int phash_update(struct phash *phash, ul_t key, ul_t val) {

    //fprintf(stderr, "update: (%lu,%lu)\n", key, val);
    assert_kv(key, val);
    #define PHUPD_UPDATE__(_p, _i, _k, _v) set_val(_p, _i, _k, _v)
    #define PHUPD_SET__(_p, _i, _k, _v)    goto again
    PHASH_UPDATE(phash, key, val, true)
    #undef PHUPD_UPDATE__
    #undef PHUPD_SET__

    return 1;
}

int phash_delete(struct phash *phash, ul_t key)
{
    #if defined(KEY_OVERLOAD)
    assert_key(key);
    #endif
    ul_t size_shift = phash->size_shift;
    ul_t size = (ul_t)1<<size_shift;
    ul_t u = phash->used;
    if (4*u < size)
        phash_shrink(phash);
    return phash_delete__(phash, key, true);
}

int phash_lookup__(phash_t *phash, ul_t key, ul_t *idx_ret, bool vals)
{
    #if defined(KEY_OVERLOAD)
    assert_key(key);
    #endif

    ul_t size_shift = phash->size_shift;
    ul_t size = (ul_t)1<<size_shift;
    ul_t perturb = key;
    ul_t mask = size-1;
    ul_t idx = key & mask;

    INCSTAT(phash->lookups);
    for (;;) {
        if ( item_unused(phash, idx, vals) )
            return 0;

        if ( !item_dummy(phash, idx, vals) && phash->kvs[idx] == key){
            *idx_ret = idx;
            return 1;
        }

        INCSTAT(phash->bounces);
        idx = ((idx<<2) + idx + 1 + perturb) & mask;
        perturb >>= PERTURB_SHIFT;
    }
}

int phash_lookup(struct phash *phash, ul_t key, ul_t *val)
{
    ul_t idx;
    int ret = phash_lookup__(phash, key, &idx, true);
    if (ret) {
        ul_t *values = phash_vals(phash);
        *val = values[idx];
    }
    return ret;
}

void
phash_iter_init(phash_t *phash, phash_iter_t *pi)
{
    pi->cnt = pi->loc = 0;
}

int
phash_iterate__(phash_t *phash, bool vals,
                phash_iter_t *pi, ul_t *key_ret,  ul_t *idx_ret)
{
    ul_t idx = pi->loc;
    ul_t size = (ul_t)1<<phash->size_shift;
    INCSTAT(phash->lookups);
    for (;;){
        if (phash->used == pi->cnt || idx >= size)
            return 0;

        if (item_valid(phash, idx, vals)){
            *key_ret = phash->kvs[idx];
            *idx_ret = idx++;
            pi->loc = idx;
            pi->cnt++;
            return 1;
        }

        idx++;
    }
}

int phash_iterate(phash_t *phash, phash_iter_t *pi, ul_t *key, ul_t *val)
{
    ul_t idx;
    int ret = phash_iterate__(phash, true, pi, key, &idx);
    if (ret) {
        ul_t *vals = phash_vals(phash);
        *val = vals[idx];
    }
    return ret;
}

void phash_print(phash_t *phash)
{
    ul_t key, val;
    phash_iter_t pi;
    int ret;

    phash_iter_init(phash, &pi);
    printf("PHASH(%p):\n", phash);
    for (;;){
        ret = phash_iterate(phash, &pi, &key, &val);
        if (!ret){
            break;
        }
        printf(" 0x%017lx : 0x%017lx\n", key, val);
    }
    printf("\n");
}

#ifdef PHASH_MAIN
#define BUFLEN 1024
void help()
{
    printf("Help:\n"
           "  insert : I <key> <val> \n"
           "  update : U <key> <val> (->v += val if exists) \n"
           "  get    : G <key>       \n"
           "  delete : D <key>       \n"
           "  size   : S             \n"
           "  print  : P             \n");
}

int main(int argc, char **argv)
{
    struct phash *ph;
    char *s, buf[BUFLEN];
    ul_t key, val;
    int ret;

    ph = phash_new(2);

    for (;;){
        s = fgets(buf, BUFLEN-1, stdin);
        if (s == NULL){
            break;
        }

        switch (*s) {
            case 'I':
            ret = sscanf(s+1, "%lu %lu", &key, &val);
            if (ret == 2){
                phash_insert(ph, key, val);
            }
            break;

            case 'U':
            ret = sscanf(s+1, "%lu %lu", &key, &val);
            if (ret == 2){
                phash_freql_update(ph, key, val);
            }
            break;

            case 'G':
            ret = sscanf(s+1, "%lu", &key);
            if (ret == 1){
                ret = phash_lookup(ph, key, &val);
                if (ret){
                    printf("%lu\n", val);
                } else {
                    printf("<None>\n");
                }
            }
            break;

            case 'D':
            ret = sscanf(s+1, "%lu", &key);
            if (ret == 1){
                phash_delete(ph, key);
            }
            break;

            case 'S':
            printf("%lu\n", phash_elements(ph));
            break;

            case 'P':
            phash_print(ph);
            break;

            case '#':
            break;

            default:
            help();
            break;

        }
        fflush(stdout);
    }

    phash_free(ph);
    return 0;
}
#endif


/**
 * Pset functions
 */
pset_t *
pset_new(ul_t minsize_shift)
{
    return phash_new__(minsize_shift, false);
}

void
pset_init(pset_t *pset, ul_t minsize_shift)
{
    phash_init__(pset, minsize_shift, false);
}

void
pset_free(pset_t *pset)
{
    phash_free(pset);
}

void
pset_tfree(pset_t *pset)
{
    REPSTATS(pset);
    free(pset->kvs);
}

void
pset_resize(pset_t *pset, ul_t new_size_shift)
{
    pset_t  old;
    phash_cp(&old, pset);

    phash_resize__(pset, new_size_shift, false);
    for (ul_t i = 0; i < pset_size(&old); i++) {
        if (item_valid(&old, i, false)){
            //fprintf(stderr, "rs: inserting (%lu,%lu)\n", item->k, item->v);
            pset_insert(pset, old.kvs[i]);
        }
    }
    free(old.kvs);
}

void
pset_grow(pset_t *pset)
{
    ul_t new_size_shift = grow_size_shift(pset);
    pset_resize(pset, new_size_shift);
}

static inline void
pset_grow_check(pset_t *pset)
{
    if (grow_check(pset))
        pset_grow(pset);
}

void static inline pset_upd_set(phash_t *p, ul_t idx, ul_t key)
{
    if (item_dummy(p, idx, false))
        p->dummies--;
    p->used++;
    p->kvs[idx] = key;
}

void pset_insert(pset_t *pset, ul_t key)
{
    assert_key(key);
    pset_grow_check(pset);
    #define PHUPD_UPDATE__(_p, _i, _k, _v) do { } while (0)
    #define PHUPD_SET__(_p, _i, _k, _v) pset_upd_set(_p, _i, _k)
    PHASH_UPDATE(pset, key, 0xdeadbabe, false)
    #undef PHUPD_UPDATE__
    #undef PHUPD_SET__
}

void
pset_shrink(pset_t *pset)
{
    ul_t new_size_shift = shrink_size_shift(pset);
    pset_resize(pset, new_size_shift);
}

int pset_delete(pset_t *pset, ul_t key)
{
    if (pset->used == 0)
        return false;

    assert_key(key);
    ul_t size_shift = pset->size_shift;
    ul_t size = (ul_t)1<<size_shift;
    ul_t u = pset->used;
    if (4*u < size)
        pset_shrink(pset);
    return phash_delete__(pset, key, false);
}

bool pset_lookup(pset_t *pset, ul_t key)
{
    ul_t idx;
    return !!phash_lookup__(pset, key, &idx, false);
}

int pset_iterate(pset_t *pset, phash_iter_t *pi, ul_t *key)
{
    ul_t idx;
    int ret = phash_iterate__(pset, false, pi, key, &idx);
    return ret;
}

void pset_print(pset_t *pset)
{
    ul_t key;
    int ret;
    phash_iter_t pi;

    phash_iter_init(pset, &pi);
    printf("PSET(%p):\n", pset);
    for (;;){
        ret = pset_iterate(pset, &pi, &key);
        if (!ret){
            break;
        }
        printf(" 0x%017lx\n", key);
    }
    printf("\n");
}

#if defined(PSET_MAIN)
#define BUFLEN 1024
void help()
{
    printf("Help:\n"
           "  insert : I <key> <val> \n"
           "  get    : G <key>       \n"
           "  delete : D <key>       \n"
           "  size   : S             \n"
           "  print  : P             \n");
}

int main(int argc, char **argv)
{
    pset_t *ps;
    char *s, buf[BUFLEN];
    ul_t key;
    int ret;

    ps = pset_new(2);

    for (;;){
        s = fgets(buf, BUFLEN-1, stdin);
        if (s == NULL){
            break;
        }

        switch (*s) {
            case 'I':
            ret = sscanf(s+1, "%lu", &key);
            if (ret == 1){
                pset_insert(ps, key);
            }
            break;

            case 'G':
            ret = sscanf(s+1, "%lu", &key);
            if (ret == 1){
                ret = pset_lookup(ps, key);
                printf("%lu -> %s\n", key, ret ? "true" : "false");
            }
            break;

            case 'D':
            ret = sscanf(s+1, "%lu", &key);
            if (ret == 1){
                pset_delete(ps, key);
            }
            break;

            case 'S':
            printf("%lu\n", pset_elements(ps));
            break;

            case 'P':
            pset_print(ps);
            break;

            case '#':
            break;

            default:
            help();
            break;

        }
        fflush(stdout);
    }

    phash_free(ps);
    return 0;
}
#endif

// vim:expandtab:tabstop=8:shiftwidth=4:softtabstop=4
