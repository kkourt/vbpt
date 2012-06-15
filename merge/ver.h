#ifndef VER_H__
#define VER_H__

#include <stdbool.h>

#include "refcnt.h"
#include "container_of.h"
#include "misc.h"

struct ver {
	struct ver *parent;
	refcnt_t   v_refcnt;
};
typedef struct ver ver_t;

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
	refcnt_init(&ret->v_refcnt, 1);
	return ret;
}

/* branch (i.e., fork) a version */
static inline ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *v = xmalloc(sizeof(ver_t));
	/* increase the reference count of the parent */
	v->parent = ver_getref(parent);
	return v;
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

#endif
