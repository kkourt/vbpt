#include "ver.h"
#include "refcnt.h"
#include "vbpt.h"
#include "vbpt_mm.h"

/*
 * initialize a header
 *  Version's refcount will be increased
 */
static inline void
vbpt_hdr_init(vbpt_hdr_t *hdr, ver_t *ver, enum vbpt_type type)
{
	hdr->ver = ver_getref(ver);
	refcnt_init(&hdr->h_refcnt, 1);
	hdr->type = type;
}


/**
 * we maintain two list of nodes:
 */
static __thread struct {
	vbpt_node_t          *mm_nodes;
	size_t                mm_nodes_nr;
	vbpt_leaf_t          *mm_leafs;
	size_t                mm_leafs_nr;
	struct vbpt_mm_stats  mm_stats;
} __attribute__((aligned(128))) vbptCache = {0};

void
vbpt_mm_stats_get(vbpt_mm_stats_t *stats)
{
	*stats = vbptCache.mm_stats;
}

static void
vbpt_cache_prealloc(void)
{
	const uint64_t prealloc_nodes = 32*1024;
	const uint64_t prealloc_leafs = 32*1024;
	for (uint64_t i=0; i<prealloc_nodes; i++) {
		vbpt_node_t *node = xmalloc(VBPT_NODE_SIZE);
		vbptCache.mm_stats.nodes_preallocated++;
		node->mm_next = vbptCache.mm_nodes;
		vbptCache.mm_nodes = node;
		vbptCache.mm_nodes_nr++;
	}
	for (uint64_t i=0; i<prealloc_leafs; i++) {
		vbpt_leaf_t *leaf = xmalloc(sizeof(*leaf));
		leaf->data = xmalloc(VBPT_LEAF_SIZE);
		vbptCache.mm_stats.leafs_preallocated++;
		leaf->mm_next = vbptCache.mm_leafs;
		vbptCache.mm_leafs = leaf;
		vbptCache.mm_leafs_nr++;
	}
}

static vbpt_node_t *
vbpt_cache_get_node(size_t node_size)
{
	assert(node_size == VBPT_NODE_SIZE); // only one size for now
	vbpt_node_t *node;
	if (vbptCache.mm_nodes_nr == 0) {
		node = xmalloc(node_size);
		vbptCache.mm_stats.nodes_allocated++;
		return node;
	}
	// pop a node
	node = vbptCache.mm_nodes;
	vbptCache.mm_nodes = node->mm_next;
	vbptCache.mm_nodes_nr--;
	// release children references
	// NB: vbpt_hdr_putref() might end up calling vbpt_node_dealloc(),
	//     which adds more nodes to the queue. We are using per-thread queues,
	//     so it should be OK.
	vbpt_kvp_t *kvp = node->kvp;
	for (uint16_t i=0; i<node->items_nr; i++)
		vbpt_hdr_putref(kvp[i].val);
	node->items_nr = 0;
	//memset(node, 0, node_size);
	return node;
}

static vbpt_leaf_t *
vbpt_cache_get_leaf(size_t leaf_size)
{
	assert(leaf_size = VBPT_LEAF_SIZE);
	vbpt_leaf_t *leaf;
	if (vbptCache.mm_leafs_nr == 0) {
		leaf = xmalloc(sizeof(*leaf));
		leaf->data = xmalloc(leaf_size);
		vbptCache.mm_stats.leafs_allocated++;
		return leaf;
	}
	leaf = vbptCache.mm_leafs;
	vbptCache.mm_leafs = leaf->mm_next;
	vbptCache.mm_leafs_nr--;
	return leaf;
}


void
vbpt_mm_init(void)
{
	vbpt_cache_prealloc();
}

void
vbpt_mm_shut(void)
{
}


/*
 * allocate a new node
 *  Version's refcount will be increased
 */
vbpt_node_t *
vbpt_node_alloc(size_t node_size, ver_t *ver)
{
	assert(node_size > sizeof(vbpt_node_t));
	vbpt_node_t *ret = vbpt_cache_get_node(node_size);
	vbpt_hdr_init(&ret->n_hdr, ver, VBPT_NODE);
	ret->items_nr = 0;
	ret->items_total = (node_size - sizeof(vbpt_node_t)) / sizeof(vbpt_kvp_t);
	return ret;
}

/**
 * free a node
 *  version's refcount will be decreased
 *  children's refcount will be decrased
 */
void
vbpt_node_dealloc(vbpt_node_t *node)
{
	ver_putref(node->n_hdr.ver);
	// add node to list
	node->mm_next = vbptCache.mm_nodes;
	vbptCache.mm_nodes = node;
	vbptCache.mm_nodes_nr++;
}


/**
 * allocate a new leaf
 *  Version's refcount will be increased
 */
vbpt_leaf_t *
vbpt_leaf_alloc(size_t leaf_size, ver_t *ver)
{
	vbptCache.mm_stats.leafs_requested++;
	vbpt_leaf_t *ret = vbpt_cache_get_leaf(leaf_size);
	vbpt_hdr_init(&ret->l_hdr, ver, VBPT_LEAF);
	ret->d_len = 0;
	ret->d_total_len = leaf_size;
	return ret;
}
/**
 * free a leaf
 *  vresion's refcount will be decreased
 */
void
vbpt_leaf_dealloc(vbpt_leaf_t *leaf)
{
	vbptCache.mm_stats.leafs_released++;
	ver_putref(leaf->l_hdr.ver);
	// add leaf to list
	leaf->mm_next = vbptCache.mm_leafs;
	vbptCache.mm_leafs = leaf;
	vbptCache.mm_leafs_nr++;
}

void
vbpt_mm_stats_report(char *prefix, vbpt_mm_stats_t *st)
{

	#define pr_cnt(x__) \
		printf("%s" "%24s" ": %lu\n", prefix, "" #x__, st->x__)

	pr_cnt(nodes_allocated);
	pr_cnt(nodes_preallocated);
	pr_cnt(leafs_allocated);
	pr_cnt(leafs_preallocated);
	pr_cnt(leafs_requested);
	pr_cnt(leafs_released);
}
