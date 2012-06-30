#ifndef VER_H__
#define VER_H__

#include <stdbool.h>

#include "refcnt.h"
#include "container_of.h"
#include "misc.h"

struct ver {
	struct ver *parent;
	refcnt_t   v_refcnt;
	#ifndef NDEBUG
	size_t v_id;
	#endif
};
typedef struct ver ver_t;


static void
ver_init(ver_t *ver)
{
	#ifndef NDEBUG
	static size_t id = 0;
	spinlock_t *lock_ptr = NULL;
	spinlock_t lock;
	#endif
	refcnt_init(&ver->v_refcnt, 1);
	#ifndef NDEBUG
	if (lock_ptr == NULL) { // XXX: race
		spinlock_init(&lock);
		lock_ptr = &lock;
	}
	spin_lock(lock_ptr);
	ver->v_id = id++;
	spin_unlock(lock_ptr);
	#endif
}

static inline char *
ver_str(ver_t *ver)
{
	static char buff[128];
	#ifndef NDEBUG
	snprintf(buff, sizeof(buff), " (ver:%3zd ) ", ver->v_id);
	#else
	snprintf(buff, sizeof(buff), " (ver:%p ) ", ver);
	#endif
	return buff;
}

static inline ver_t *
refcnt2ver(refcnt_t *rcnt)
{
	return container_of(rcnt, ver_t, v_refcnt);
}

/* get a reference of ver -- i.e., increase reference count */
static inline ver_t *
ver_getref(ver_t *ver)
{
	refcnt_inc(&ver->v_refcnt);
	return ver;
}

static void ver_release(refcnt_t *);

/**
 * release reference -- decrease reference count
 */
static inline void
ver_putref(ver_t *ver)
{
	refcnt_dec(&ver->v_refcnt, ver_release);
}


/**
 * release a version
 *  decrements reference count of parent
 */
static void
ver_release(refcnt_t *refcnt)
{
	ver_t *ver = refcnt2ver(refcnt);
	if (ver->parent)
		ver_putref(ver->parent);
	free(ver);
}

/* create a version from a versioned object */
static inline ver_t *
ver_create(void)
{
	ver_t *ret = xmalloc(sizeof(ver_t));
	ret->parent = NULL;
	ver_init(ret);
	return ret;
}

/* branch (i.e., fork) a version */
static inline ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *ret = xmalloc(sizeof(ver_t));
	ver_init(ret);
	/* increase the reference count of the parent */
	ret->parent = ver_getref(parent);
	return ret;
}


static inline bool
ver_eq(ver_t *ver1, ver_t *ver2)
{
	return ver1 == ver2;
}

/**
 * check if ver1 <= ver2 -- i.e., if ver1 is ancestor of ver2
 */
static inline bool
ver_leq(ver_t *ver1, ver_t *ver2)
{
	for (ver_t *v = ver2; v != NULL; v = v->parent)
		if (v == ver1)
			return true;
	return false;
}


#define VER_JOIN_FAIL ((ver_t *)(~((uintptr_t)0)))
#define VER_JOIN_LIMIT 3

/**
 * see ver_join()
 *
 * We can avoid the O(n^2) thing by keeping a hash table for all the versions on
 * the global path, so that we can iterate the pver path and check membership.
 */
static ver_t *
ver_join_slow(ver_t *gver, ver_t *pver, ver_t **prev_pver)
{
	ver_t *gv = gver;
	for (unsigned gv_i = 0; gv_i < VER_JOIN_LIMIT; gv_i++) {
		ver_t *pv = pver;
		for (unsigned pv_i=0 ; pv_i < VER_JOIN_LIMIT; pv_i++) {
			if (pv->parent == gv->parent) {
				if (prev_pver)
					*prev_pver = pv;
				assert(pv->parent != NULL);
				return pv->parent;
			}

			if ((pv = pv->parent) == NULL)
				break;
		}
		if ((gv = gv->parent) == NULL)
			break;
	}
	return VER_JOIN_FAIL;
}

/**
 * find the join point (largest common ancestor) of two versions
 *
 * The main purpose of this is to find the common ancestor of two versions.
 * Given our model, however, it gets a bit more complicated than that.
 * We assume that the join is performed between two versions:
 *  - @gver: the current version of the object (read-only/globally viewable)
 *  - @pver: a diverged version, private to the transaction
 *
 * The actual join operation (find the common ancestor) is symmetric, but we
 * neeed to distinguish between the two versions because after we (possibly)
 * merge the two versions, we need to modify the version tree (see merge
 * operation), which requires more information than the common ancestor.
 * Specifically, we need to move @pver under @gver, so we need last node in the
 * path from @pver to the common ancestor (@prev_v). This node is returned in
 * @prev_v, if @prev_v is not NULL.
 *
 *        (join_v)    <--- return value
 *       /        \
 *  (prev_v)      ...
 *     |           |
 *    ...        (gver)
 *     |
 *   (pver)
 */
static inline ver_t *
ver_join(ver_t *gver, ver_t *pver, ver_t **prev_v)
{
	/* this is the most common case, do it first */
	if (gver->parent == pver->parent) {
		assert(pver->parent != NULL);
		if (prev_v)
			*prev_v = pver;
		return pver->parent;
	}
	return ver_join_slow(gver, pver, prev_v);

}

#endif
