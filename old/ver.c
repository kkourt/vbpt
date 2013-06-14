#define _GNU_SOURCE /* for pthread_spinlock */

/* pthread spinlock wrappers */
#define spinlock_t       pthread_spinlock_t
#define spinlock_init(x) pthread_spin_init(x, 0)
#define spin_lock(x)     pthread_spin_lock(x)
#define spin_unlock(x)   pthread_spin_unlock(x)

#include <pthread.h>

#include "ccan/list/list.h"

/**
 * TODO:
 *  - complete a run a simple example with counters
 *  - figure out how to run this example in non-cache-coherent memory /
 *    distributed memory
 *  - handle the more complicated case of the tree, where objects consist
 *    of versioned parts
 *  - write a pluscal algorithm for merging
 *  - write a simple non-balanced binary-tree
 */

/**
 * Note about nesting (transactions):
 * We try to support nested transactions conceptually, but the implementation is
 * not optimized for deep nesting, and in some cases if the nesting is too deep,
 * we might choose to  fail to keep the algorithms simpler
 * (e.g., see VER_JOIN_LIMIT) */

/**
 * We assume that versions are short-lived -- i.e. they exist only in the
 * context of a transaction. This means that:
 *  - they can be stored in RAM, using pointers/etc (?)
 * Versions need to be garbage collected:
 *  - we keep a reference count for each version
 *  - when a fork happens, the reference count of the forked version is
 *    increased
 *  - the reference count of p is decreased when a a child of p merges back
 *  - versions older than the "blessed" one with zero reference count can
 *    (should) be collected
 *
 * Referece counting:
 *  linux/Documentation/kref.txt is a very good read
 *  An important point is that if when you try to get a reference, and the
 *  counter _can_ go to zero you need locking.
 *
 * Version references:
 *  - A mutable object holds a reference to the corresponding version
 *  - A transaction holds references to its private versions
 *  - children versions hold references to the parent version via ->parent
 *  - objects should also hold reference to versions (e.g., each node of the
 *  tree holds a reference to its version)
 *
 * if M is the current mutable version, there is a path from M to the root
 * of the tree (->parent == NULL): M > M1 > M2 > M3 >... > Mx > NULL
 * We can release all versions Mi <= Mj: Mj = max{Mk <= Mj: getref(Mk) == 1}
 *
 */

/**
 * Object GC: In the general case objects consist of nodes (e.g., a tree) and
 *  each of those nodes holds a reference to its version. Each of these nodes
 *  have also reference counts, since they may be referenced from other nodes
 *  (e.g., a node might be referenced by multiple roots). When either of these
 *  two happends:
 *   - an object in the mutable state is replaced
 *   - an object is discarded
 *  the object is passed to the object-specific GC function. This function is
 *  responsible for garbage-collecting the object. In a typical tree structure,
 *  the GC would decrease the reference count of all nodes pointed by its
 *  root. When a node reaches zero, it also releases its version reference.
 */


struct ver {
	struct ver *parent;   // ->parrent == NULL if root of the version tree
	atomic_t   ref_count; // reference count for this version
	void       *vobj      // the (versioned) object for this version
};

static inline ver_t *
ver_parent(ver_t *v)
{
	return v->parent;
}

#define VER_FAILURE (~((uintptr_t)0))
#define VER_JOIN_LIMIT 3

/**
 * The main purpose of this is to find the common ancestor of two versions.
 * Given our object model, however, it gets more complicated than that.
 * We assume that the join is performed between two versions:
 *  - the current version of the mutable object (mver)
 *  - a diverged version, private to the transaction (tver)
 * The actual join operation (find the common ancestor) is symmetric, but we
 * neeed to distinguish between the two versions because after we (possibly)
 * merge the two versions, we need to modify the version tree, which requires
 * more information than the common ancestor.
 */
ver_t *
ver_join(ver_t *v1_orig, ver_t *v2_orig)
{
	/* this is the most common case, do it first */
	if (v1_orig->parent == v2_orig->parent)
		return v1_orig->parent

	int v1_i = 0, v2_i = 0;
	ver_t *v1 = v1_orig;
	ver_t *ret = VER_FAILURE;
	for (;;) {
		/* check current v1 with all v2 */
		ver_t *v2 = v2_orig;
		do {
		}
		if (v1->parent == NULL || v1_i >= VER_JOIN_LIMIT)
			break;
		v1_i++;
	}

	return ret;
}

/**
 * merge mver to tver
 *  @tver: transaction-private version
 *  @mver: mobj's version (no changes are allowed in mver)
 *
 * The merging happens  *in-place* in tver. If successful 0 is returned. If not
 * VER_FAILURE is returned tver is invalid.
 *
 * If successful, new @mver should be @nver's ancestor (which is the whole point
 * of doing the merge: to make it appear as if the changes in tver happened
 * after mver). This has some implications (see below).
 *
 * The merge finds first common ancestor/join point of the two versions vj. In
 * the general case the version tree might look like:
 *
 *        (vj)             (vj)
 *       /    \             |
 *    ...     ...    ==>   ...
 *    /         \           |
 * (tver)    (mver)       (tver)
 *                          |
 *                         ...
 *                          |
 *                        (mver') == (nver)
 *
 * If the versions between vj (common ancestor) and mver have a refcount of 1,
 * we can just move the path (vj) -- (mver) under (tver). If not all refcounts
 * in the path are 1, there is a branched vresion somewhere in the path that is
 * not yet merged and if we move the path the join point might change. OTOH, if
 * we copy the path, we need to change all the versions in the tree nodes (or
 * provide some indirection), so that the versioning comparison will work for
 * future versions.
 */
int
ver_merge(ver_t *tver, const ver_t *mver);
{
	ver_t *vj = ver_join(); //
	obj_merge(tver, mver, vj);
}


/**
 * Mutable objects -- pointing to a "blessed" version
 */
struct mobj {
	ver_t  *ver;
	spinlock_t lock;
}
typedef struct mobj mobj_t;

/**
 * mobj_verget(): get a reference for the current version of the mutable object
 *   since this might be the only reference for this particular version, we need
 *   to take a lock to ensure that the version won't be decrefed() under our
 *   nose. Particularly locking should ensure that no decref() to zero happens
 *   between atomic_read() and atomic_inc(). We need to make a better reasoning
 *   about this by considering all who might hold a reference to a version.
 *
 * @retries: number of retries to get the pointer
 */
static inline const ver_t *
mobj_verget(mobj_t *mobj, int retries)
{
	ver_t *ret = VER_FAILURE;
	do {
		spin_lock(&mobj->lock);
		ver_t *v = mobj->ver;
		if (atomic_read(&v->ref_count) > 0) {
			atomic_inc(&v->ref_count);
			ret = v;
		}
		spin_unlock(&mobj->lock);
	} while ( ret != VER_FAILURE && --retries > 0);
	return ret;
}

/**
 * In our model, each transaction (which is an execution context) works on its
 * own private version of the objects. We need a way to manage versions of
 * objects between the parent and the child execution context (all transactions
 * of level zero, have the global mutable state as their parent execution
 * context)
 *
 * We distinguish two ways to do that:
 *
 * a) lazily: versions are forked from the parent execution context lazily, when
 * the context needs to access the object. This requires synchronization on the
 * parent objects, when doing the forking, so that the new context can get a
 * (valid) reference to the parent version.
 *
 * b) eagerly: versions are forked from the parent, when the new execution
 * context is created => the new execution context acquires references to all the
 * current versions, which become immutable. The parent execution needs to
 * fork-of their new mutable versions. This approach can be helped by language
 * support for what objects are modified in a transaction.
 *
 * The idea is to be lazy in zero-level transactions (i.e., for managing the
 * global state), and eager in nested transactions.
 */

/**
 * Nested transactions:
 * For a transaction to successfully commit and update the global state, all of
 * its children must have committed, and all objects must have been successfully
 * merged. When a transaction is spawned from another transaction, transaction
 * objects (or at least those that are accessed by the child transaction) need
 * to be checkpointed -- i.e., save and make immutable the current version.
 *
 * Here's a motivating example:
 *   A transaction T1 starts
 *   T1 accesses an object O from global state -> forking version V1
 *   V1 is private to T1, T1 is free to modify it
 *   T2 is created inside T1:
 *     - V1 becomes immutable, two versions are forked:
 *          - V2 for T2
 *          - V3 for T1
 *  T1 needs T2 to commit, before attempting to commit to the final state and
 *  merge V2 and V3.
 *
 * Note: it might be the case that a reference count in the transactions is not
 * needed, since we can just wait for the reference count of the objects. This
 * assumes that every nested transaction will at least fork an object from the
 * parent transaction. Note that there not much sense in doing a nested
 * transaction without using some of the parent's transaction objects.  The
 * forking needs to be done eagerly anyway since the inner transaction objects
 * should become immutable.
 *
 * It is not clear what happens if a nested transaction accesses an object not
 * accessed by the parent transaction. If the parent transaction didn't access
 * the object after creating the nested transaction, we can just add it to its
 * set. If it did, we can try and merge the two versions.
 */

/**
 * In the general case, A (global) state can be viewed as a set of mutable
 * objects. Each transaction operates on private versions, forked from the
 * versions of the global state.
 *
 * We want to investigate if the global state needs to change atomically with
 * respect to transactions.
 *
 * For example, consider the following scenario:
 *  state = <A0,B0>     # (state consists of 2 objects: A,B)
 *  T1 forks A1 from A0
 *  T1 forks B1 from B0
 *  T2 forks B2 from B0
 *  T1 commits: state = <A1,B1> (trivial update)
 *  T2 forks A2 from A1
 *  T2 tries to commit state <A2,B2>:
 *    - A2 forked from A1 => OK
 *    - B2 forked from B0 => needs rebase
 *
 * In this case, the change is not updated atomically, since T2 obeserves
 * <A1,B0>. However, I don't think this is an issue, since we end up rebasing
 * the changes of T2 so that it would appear (if possible) to happen _after_
 * T1. Maybe we can make a more formal argument based on some kind of
 * monotonicity property.
 *
 * If, however, we want to ensure transaction atomicity, here are some thoughts:
 * If we consider the state consisting of multiple objects, then one way would
 * be to protect the state with locks (e.g., a single lock or one-lock per
 * object), and before starting the transaction, the locks for _all_ the objecs
 * involved in the transaction are taken, their current versions recorded, and
 * the locks released. State would change using the same locking mechanism
 * (e.g., via 2PC),
 * This is problematic for two reasons: i) initializing a transaction requires
 * locking and may cause contention ii) it might not be known beforehand what
 * the objects the transaction will access are. In this case, versions for all
 * objects would be required.
 * An alternative approach would be to create a hierarchy of objects. For
 * example, a (versioned) tree (let's call it A) could hold a mapping between
 * address and location for each object. The location would point to a
 * _specific_ version (immutable) for this object. If we need to update an
 * object (say O), we create another version of O, and a new version of A where
 * O's address points to O's new version. Note that all references to objects
 * must be in terms of addresses in A. In that case, the only mutable state
 * neded is the "blessed" version of A.  This approach is very suitable for a
 * typical file-system design, where the A tree is the tree that maps inodes to
 * files or directories.
 */

/**
 * Transactional objects
 *
 * ->ver is mutable for the owning transaction
 */
struct vtxobj {
	ver_t  *ver;           /* current version */
	vtx_t  *tx;            /* owning transaction */
	mobj_t *mo;            /* corresponding mutable object (blessed version) */
	struct list_node tx_l; /* transaction list node member */
};
typedef struct vtxobj vtxobj_t;

vtxobj_t *
__vtxobj_alloc(void)
{
	vtxobj_t *vtxo = malloc(sizeof(vtxobj_t));
	if (!vtxo) {
		assert(0);
	}
	return vtxo;
}

vtxobj_t *
vtxobj_alloc(mobj_t *mobj, ver_t *ver, vtx_t *tx)
{
	vtxojb_t vtxo = __vtobj_alloc();
	vtxo->mo = mobj;
	vtxo->ver = ver;
	vtxo->tx = tx;
	if (tx != NULL) // TODO: keep sorted
		list_add(&tx->obj_l, &new->tx_l);
}

void
vtx_access(vtxobj_t **vtxobj_ptr, tx_t *tx)
{
	vtxobj_t *vtxobj = *txobj_ptr, *new;
	if (txobj->tx != NULL && txobj->tx == tx)
		return;
	/* allocate and initialize object */
	vtxobj_alloc(vtxobj->mo, ver_branch(txobj->ver), tx);
	/* return the new object */
	*txobj_ptr = new;
}

/**
 * Transaction handler
 * Transactions are temporary execution contexts.
 *  - They include a set of versioned objects
 *  - These objects are accessed via versions private to the transaction
 */
struct vtx {
	vtx_t  *parent_tx;      /* parent transaction (for nesting support) */
	struct list_head obj_l; /* list of objects owned by transaction */
};
typedef struct vtx vtx_t;

vtx_t *
vtx_begin(vtx_t *parent_tx)
{
	if (parent_tx != NULL) {
		//TODO: nested transactions
	}
	// alocate and initialize transaction
}

#define VTX_FAIL      (-1)
#define VTX_SUCCESS   (0)
int
vtx_try_commit(vtx_t *tx)
{
	if (tx->parent_tx) // TODO: nested transactions
		assert(false);

	int err = VTS_FAIL;
	vtxobj_t *txo;
	list_for_each(&tx->obj_l, txo, tx_l) { // XXX: assumption: list is ordered
		if (atomic_read(&txo->ver) > 1)
			assert(false); // we need to wait for nested transactions

		const ver_t *mver = mobj_verget(tx->mo);
		if (mv == VER_FAILURE)
			goto end;

		if (ver_parent(txo->ver) == mver)
			continue;

		// try to merge...
		// XXX: How the above property would affect versioned trees
		//      (e.g., GC)?
		ver_t *merged_v;
		merged_v = ver_merge(txo->ver, mver);
		if (merged_v == VER_FAILURE)
			goto end;

		// this is private, so we don't need a lock or anything.
		//  -> what happens if a nested transaction has a reference?
		ver_put(txo->ver);
	}

	/* Note, that here we need to change the state atomically, since an
	 * object might have changed under our nose, and we can't merge it now.
	 * So, we take the locks in-order, and check versions. If we manage to
	 * take all the locks, we update the state.*/
	err = VTS_SUCCESS;
	list_for_each(&tx->obj_l, txo, tx_l) { // XXX: assumption: list is ordered
		spin_lock(txo->mobj->lock);
		if (ver_parent(txo->ver) != txo->mo->ver) {
			err = VTS_FAIL;
			break;
		}
	}

	/* no error: update mutable objects */
	if (err == 0) {
		list_for_each(&tx->obj_l, txo, tx_l) {
			txo->mo->ver = txo->ver;
			txo->mo->vobj = txo->vobj;
			/* TODO: decrease/increase reference counts */
		}
	}

	/* release locks starting from txo and going backwards */
	//TODO

end:
	/* TODO: cleanup if fail */
	return err;
}

int
vtx_end(vtx_t *tx, unsigned int retries)
{
	int err;
	do {
		err = vtx_try_commit(vtx_t *tx);
	} while (err != 0 && retries-- > 0);
	return err;
}


/* TODO: add a SLAB:
 *  setting the reference count to 1
 */
ver_t *
ver_alloc__(void)
{
	 ver_t *v = malloc(sizeof(ver_t));
	 if (v == NULL) {
		assert(0);
	 }
	atomic_set(&v->ref_count, 1);
	return v;
}

/* TODO: SLAB */
void
ver_free(vbfs_ver_t *v)
{
	free(v);
}

/* create a mutable object, from a versioned object
 *  this allocates a new root (i.e., ->parent == NULL) version that has a
 *  refcount of 1 */
static mobj_t *
mobj_create(void *vobj)
{
	int err;
	mobj_t *mobj = malloc(sizeof(mobj_t));
	if (mobj == NULL) {
		assert(0);
	}
	mobj->ver = ver_create(vobj);
	err = spinlock_init(&mobj->lock);
	return mobj;
}

/* create a version from a versioned object */
ver_t *
ver_create(void *vobj)
{
	ver_t *ret = ver_alloc__();
	ret->parent = NULL;
	ret->vobj = vobj;
	return ret;
}

ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *v = ver_alloc__();
	/* increase the reference count of the parent */
	v->parent = ver_getref(parent);
	/* TODO copy object */
	// v->vobj = copy(parent->obj)
	return v;
}

/**
 * Test:
 *   This is a simple test for the transaction/object functionality:
 *     - objects are counters (integer)
 *     - merging is a simple reduction
 */
int main(int argc, const char *argv[])
{
	int O_nr = 4;       /* number of objects */
	int threads_nr = 4; /* number of threads */
	int ops_nr = 4096;  /* total number of operations -- in this case additions */

	/* objects:
	 *  O[i] : user object i
	 * mO[i] : mutable object i */
	struct O {
		int cnt;
	} __attribute__ ((aligned(128))) O[O_nr];
	mobj_t *mO[O_nr];

	/* operations (additions) */
	struct op {
		int Oi; /* object index */
		int n;  /* number to add */
	} Ops[ops_nr];

	/* initialize operations */
	srand(666);
	for (int i=0; i<ops_nr; i++) {
		Ops[i].Oi = rand() % O_nr;
		Ops[i].n  = rand();
	}

	/* initialize objects (Os) and create mutable objects (mOs) */
	for (int i=0; i<O_nr; i++) {
		O[i].cnt =  0;
		mO[i] = mobj_create(&O[i]);
	}

	/* threads  */
	struct work {
		struct op *ops;
		int ops_nr;
	};

	void *thread_fn(void *arg) {
		struct work *w = arg;

		vtx_t *vtx = vtx_begin(NULL); // start a transaction
		/* eagerly allocate transaction objects */
		vtxobj_t *vtxO[O_nr];
		for (int i=0; i<O_nr; i++) {
			ver_t *ver = ver_branch(mO[i]->ver);
			vtxO[i] = vtxobj_alloc(mO[i], ver, vtx);
		}

		/* apply the operations to the objects */
		for (int i=0; i<w->ops_nr; i++) {
			int Oi      = w->ops[i].Oi;
			int n       = w->ops[i].On;
			struct O *o = vtxO[Oi]->ver->vobj
			o->cnt += n;
		}

		vtx_end(vtx, 1); //end transaction
	}

	return 0;
}
