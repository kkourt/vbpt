#ifndef VER_H__
#define VER_H__

#include "refcnt.h"
#include "misc.h"

struct ver {
	struct ver *parent;
	refcnt_t   refcnt;
};
typedef struct ver ver_t;

/* get a reference of ver -- i.e., increase reference count */
ver_t *
ver_getref(ver_t *ver)
{
	refcnt_inc(&ver->refcnt);
	return ver;
}


/* create a version from a versioned object */
ver_t *
ver_create(void)
{
	ver_t *ret = xmalloc(sizeof(ver_t));
	ret->parent = NULL;
	return ret;
}

/* branch (i.e., fork) a version */
ver_t *
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
