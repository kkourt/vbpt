#ifndef VER_H__
#define VER_H__

#include <stdbool.h>
#include <inttypes.h>

#include "refcnt.h"
#include "container_of.h"
#include "misc.h"

// Versions form a tree (partial order) as defined by the ->parent pointer.

// For implementation convenience we add the log to the version, so we need to
// include the appropriate header
#include "vbpt_log_internal.h"

/**
 * Garbage collecting versions
 *
 * versions are referenced by:
 *  - other versions (ver_t's ->parent)
 *  - trees          (vbpt_tree_t's ->ver)
 *  - tree nodes     (vbpt_hdr_t's ->ver)
 *
 *     ...
 *      |
 *      o
 *      |
 *      o Vj
 *      |\
 *      . .
 *      .  .
 *      .   .
 *      |    \
 *   Vg o     o Vp
 *
 * Since children point to parents, we can't use typical reference counting --
 * reference counts will not reach zero. Instead, we need to collect versions
 * which form a chain that reaches the end (NULL) and all refcounts are 1.
 *
 * To implement the merge operation we need to query the partial order of
 * versions. As an optimization, we only check until the Vj (i.e., we only
 * traverse the Vg->Vj paths and Vp->Vj paths), and if we don't find the version
 * we are looking for, we assume that it is before Vj (see ver_join() and
 * ver_leq_limit()).
 *
 * One (i.e., me in several different occasions) might think that this enable us
 * to not count references from nodes.  However, this is not the case. The
 * problem is that if a node points to a version (vx) that is free()d, it is
 * possible that a new version (vy) will be allocated on the same address, which
 * means that nodes with version vx, will be mistakely assumed to be nodes with
 * version vy.  One solution would be to "version" the versions with a unique
 * identifier, and add it to the node references, so that we can quickly check
 * if this a version that was reallocated.
 *
 * Instead, we use two reference counts: one for keeping a version to the
 * version tree (rfcnt_children), and one for reclaiming the version
 * (rfcnt_hdrs).  We use rfcnt_children to remove a version from the tree, and
 * rfcnt_hdrs to reclaim the version (if rfcnt_children == 0). Reming eagerly
 * the version from the tree avoids having to search long version chains.
 * structures that maintain blessed versions (e.g., mtree) should grab a
 * children reference.
 *
 * To avoid keeping old versions around for too long, we could change the
 * versions of the nodes to point to newer versions. This would probably need to
 * maintain two reference counts: one for nodes, and one for parents/trees.
 * Additionally, this would be easier if we set ->ver == NULL for nodes that
 * have the same version as their parent.
 */

struct ver {
	struct ver *parent;
	refcnt_t   rfcnt_children;
	refcnt_t   rfcnt_hdrs;
	#ifndef NDEBUG
	size_t     v_id;
	#endif
	vbpt_log_t v_log;
};
typedef struct ver ver_t;

#define VER_PARENT_UNLINKED ((ver_t *)0xdeaddad)

void ver_debug_init(ver_t *ver);

static inline void
ver_init(ver_t *ver)
{
	refcnt_init(&ver->rfcnt_hdrs, 1);
	refcnt_init(&ver->rfcnt_children, 0);
	#ifndef NDEBUG
	ver_debug_init(ver);
	#endif
	// XXX: ugly but useful
	ver->v_log.state = VBPT_LOG_UNINITIALIZED;
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
	snprintf(buff, VERSTR_BUFF_SIZE, " [%p: ver:%3zd rfcnt_children:%3u rfcnt_hdrs:%3u] ",
		 ver,
	         ver->v_id,
		 refcnt_get(&ver->rfcnt_children),
		 refcnt_get(&ver->rfcnt_hdrs));
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
refcnt_nodes2ver(refcnt_t *rcnt)
{
	return container_of(rcnt, ver_t, rfcnt_hdrs);
}

/* get a reference of @ver for a node */
static inline ver_t *
ver_getref_hdr(ver_t *ver)
{
	refcnt_inc(&ver->rfcnt_hdrs);
	return ver;
}

static void ver_release(refcnt_t *);

/**
 * release a node reference
 */
static inline void
ver_putref_hdr(ver_t *ver)
{
	refcnt_dec(&ver->rfcnt_hdrs, ver_release);
}


/**
 * release a version
 */
static void
ver_release(refcnt_t *refcnt)
{
	ver_t *ver = refcnt_nodes2ver(refcnt);
	assert(refcnt_get(&ver->rfcnt_children) == 0 &&
	       "I don't think this is supposed to happen");
	assert(ver->parent == VER_PARENT_UNLINKED &&
	       "I don't think this is supposed to happen");
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

/**
 * garbage collect the tree from @ver's parent and above
 *  Find the longer chain that ends at NULL and all nodes have a rfcnt_children
 *  of 1. This chain can be detached from the version tree.
 */
static void
ver_tree_gc(ver_t *ver)
{
	ver_t *ver_p = ver->parent;
	while (true) {
		// we reached the end, garbage collect
		if (ver_p == NULL) {
			break;
		} else if (refcnt_get(&ver_p->rfcnt_children) > 1) {
			ver = ver_p;
		}
		ver_p = ver_p->parent;
	}

	#ifndef NDEBUG
	ver_t *v = ver->parent;
	while (v != NULL) {
		ver_t *tmp = v->parent;
		v->parent = VER_PARENT_UNLINKED;
		v = tmp;
	}
	#endif
	// this is the new top
	ver->parent = NULL;
}

/**
 * getting a reference for setting a blessed pointer increases the
 * rfcnt_children, so we need a separate wrapper
 */
static inline ver_t *
ver_getref_blessed(ver_t *ver)
{
	assert(refcnt_get(&ver->rfcnt_children) == 0);
	refcnt_inc__(&ver->rfcnt_children);
	return ver;
}

/**
 * return a reference (@old_bl) that was acquired with ver_getref_blessed() and
 * perform garbage collection after the new version @new_bl.
 */
static inline void
ver_putref_blessed(ver_t *new_bl, ver_t *old_bl)
{
	refcnt_dec__(&old_bl->rfcnt_children);
	ver_tree_gc(new_bl);
}


/**
 * set parent without checking for previous parent
 */
static inline void
ver_setparent__(ver_t *v, ver_t *parent)
{
	refcnt_inc__(&parent->rfcnt_children); // do not check if it's zero
	v->parent = parent;
}

/**
 * set a parent to a version.
 *  If previous parent is not NULL, refcount will be decreased
 *  Will get a new referece of @new_parent
 *
 *  This should be used to rebase a version
 */
static inline void
ver_setparent(ver_t *ver, ver_t *new_parent)
{
	if (ver->parent) {
		refcnt_dec__(&(ver->parent->rfcnt_children));
		ver_tree_gc(ver);
	}
	ver_setparent__(ver, new_parent);
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

ver_t *
ver_join_slow(ver_t *gver, ver_t *pver, ver_t **prev_pver,
              uint16_t *gdist, uint16_t *pdist);

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
	ver_t *v = tail;
	while (true) {
		if (refcnt_get(&v->rfcnt_children) > 1)
			return true;
		if (v == head)
			break;
		assert(v != NULL);
		v = v->parent;
	}
	return false;
}


static inline ver_t *
ver_parent(ver_t *ver)
{
	return ver->parent;
}

/*
 * Log helpers
 */
static inline ver_t *
vbpt_log2ver(vbpt_log_t *log)
{
	return container_of(log, ver_t, v_log);
}

/**
 * return the parent log
 *  Assumption: this is a log embedded in a version
 */
static inline vbpt_log_t *
vbpt_log_parent(vbpt_log_t *log)
{
	ver_t *ver = vbpt_log2ver(log);
	ver_t *ver_p = ver_parent(ver);
	return (ver_p != NULL) ? &ver_p->v_log : NULL;
}

#endif
