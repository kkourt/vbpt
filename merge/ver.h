#ifndef VER_H__
#define VER_H__

#include <stdbool.h>
#include <inttypes.h>

#include "refcnt.h"
#include "container_of.h"
#include "misc.h"

// versions use two refernce counts:
//  - v_refcounts: all references to a version:
//     - from children versions
//     - from objects pointing to a versions
//  - children: references only from other versions
//
// The latter helps validating merges, not sure if is needed for more than
// debugging -- i.e., we might be able to make sure that this does not happen by
// correctly handling transaction nesting. For now, it is defined only for DEBUG
// setups

struct ver {
	struct ver *parent;
	refcnt_t   v_refcnt;
	#ifndef NDEBUG
	refcnt_t   children;
	size_t     v_id;
	#endif
};
typedef struct ver ver_t;


static void
ver_init(ver_t *ver)
{
	#ifndef NDEBUG
	/* XXX note that for this to work properly this function can't be in a
	 * header file -- move it to ver.c */
	static size_t id = 0;
	spinlock_t *lock_ptr = NULL;
	spinlock_t lock;
	#endif
	refcnt_init(&ver->v_refcnt, 1);
	#ifndef NDEBUG
	refcnt_init(&ver->children, 0);
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
	#define VERSTR_BUFF_SIZE 128
	#define VERSTR_BUFFS_NR   16
	static int i=0;
	static char buff_arr[VERSTR_BUFFS_NR][VERSTR_BUFF_SIZE];
	char *buff = buff_arr[i++ % VERSTR_BUFFS_NR];
	#ifndef NDEBUG
	snprintf(buff, VERSTR_BUFF_SIZE, " [%p: ver:%3zd ch:%3u cnt:%3u] ",
		 ver,
	         ver->v_id,
		 refcnt_get(&ver->children),
		 refcnt_get(&ver->v_refcnt));
	#else
	snprintf(buff, VERSTR_BUFF_SIZE, " (ver:%p ) ", ver);
	#endif
	return buff;
}

static inline void
ver_path_print(ver_t *v, FILE *fp)
{
	fprintf(fp, "ver path: ");
	do {
		fprintf(fp, "%s ->", ver_str(v));
	} while ( (v = v->parent) != NULL);
	fprintf(fp, "NULL\n");
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
	#ifndef NDDEBUG
	assert(refcnt_get(&ver->children) == 0);
	#endif
	if (ver->parent) {
		#ifndef NDEBUG
		refcnt_dec__(&ver->parent->children);
		#endif
		ver_putref(ver->parent);
	}
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

static inline void
ver_setparent__(ver_t *v, ver_t *parent)
{
	#ifndef NDEBUG
	refcnt_inc__(&parent->children);
	#endif
	v->parent = ver_getref(parent);
}

/* branch (i.e., fork) a version */
static inline ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *ret = xmalloc(sizeof(ver_t));
	ver_init(ret);
	/* increase the reference count of the parent */
	ver_setparent__(ret, parent);
	return ret;
}

/*
 * versions form a partial order, based on the version tree that arises from
 * ->parent: v1 < v2 iff v1 is an ancestor of v2
 */

static inline bool
ver_eq(ver_t *ver1, ver_t *ver2)
{
	return ver1 == ver2;
}

/**
 * check if ver1 <= ver2 -- i.e., if ver1 is ancestor of ver2 or ver1 == ver2
 *  moves upwards (to parents) from @ver2, until it encounters @ver1 or NULL
 */
static inline bool
ver_leq(ver_t *ver1, ver_t *ver2)
{
	for (ver_t *v = ver2; v != NULL; v = v->parent)
		if (v == ver1)
			return true;
	return false;
}

/**
 * check if ver1 <= ver2 -- i.e., if ver1 is ancestor of ver2 or ver1 == ver2
 *  moves upwards (to parents) from @ver2, until it does @max_distance @steps or
 *  encounters @ver1 or NULL.
 */
static inline bool
ver_leq_limit(ver_t *v1, ver_t *v2, uint16_t max_distance)
{
	ver_t *v = v2;
	for (uint16_t i=0; v != NULL && i < max_distance; v = v->parent, i++) {
		if (v == v1)
			return true;
	}
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
ver_join_slow(ver_t *gver, ver_t *pver, ver_t **prev_pver,
              uint16_t *gdist, uint16_t *pdist)
{
	ver_t *gv = gver;
	for (unsigned gv_i = 0; gv_i < VER_JOIN_LIMIT; gv_i++) {
		ver_t *pv = pver;
		for (unsigned pv_i=0 ; pv_i < VER_JOIN_LIMIT; pv_i++) {
			if (pv->parent == gv->parent) {
				if (prev_pver)
					*prev_pver = pv;
				assert(pv->parent != NULL);
				*gdist = gv_i + 1;
				*pdist = pv_i + 1;
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
 *
 * Furthermore, another useful property for the merge algorithm is to now the
 * distance of each version from the join point. This allows to have more
 * efficient checks on whether a version found in the tree is before or after
 * the join point. The distance of the join point from @gver (@pver) is returned
 * in @gdist (@pdist).
 */
static inline ver_t *
ver_join(ver_t *gver, ver_t *pver, ver_t **prev_v, uint16_t *gdist, uint16_t *pdist)
{
	/* this is the most common case, do it first */
	if (gver->parent == pver->parent) {
		assert(pver->parent != NULL);
		if (prev_v)
			*prev_v = pver;
		*gdist = *pdist = 1;
		return pver->parent;
	}
	return ver_join_slow(gver, pver, prev_v, gdist, pdist);

}

/**
 *  check if a version chain at @start and ending at @end has branches.
 *  returns false if all refcounts between @start and @end are 1
 *  aimed for debugging
 */
static inline bool
ver_chain_has_branch(ver_t *tail, ver_t *head)
{
#ifndef NDEBUG
	ver_t *v = tail;
	while (true) {
		if (refcnt_get(&v->children) > 1)
			return true;
		if (v == head)
			break;
		assert(v != NULL);
		v = v->parent;
	}
#endif
	return false;
}

/**
 * set a parent to a version.
 *  If previous parent is not NULL, refcount will be decreased
 *  Will get a new referece of @new_parent
 */
static inline void
ver_setparent(ver_t *ver, ver_t *new_parent)
{
	if (ver->parent)
		ver_putref(ver->parent);
	ver_setparent__(ver, new_parent);
}


#endif
