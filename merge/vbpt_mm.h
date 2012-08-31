#ifndef VBPT_MM_H__
#define VBPT_MM_H__

#include "ver.h"
#include "vbpt.h"

#define VBPT_NODE_SIZE 512
#define VBPT_LEAF_SIZE 1024

void vbpt_mm_init(void);
void vbpt_mm_shut(void);

vbpt_node_t *vbpt_node_alloc(size_t node_size, ver_t *ver);
void         vbpt_node_dealloc(vbpt_node_t *node);

vbpt_leaf_t *vbpt_leaf_alloc(size_t leaf_size, ver_t *ver);
void         vbpt_leaf_dealloc(vbpt_leaf_t *leaf);

struct vbpt_mm_stats {
	size_t   nodes_allocated;
	size_t   leafs_allocated;
	#if 0
	uint64_t node_alloc_ticks;
	uint64_t node_dealloc_ticks;
	uint64_t leaf_alloc_ticks;
	uint64_t leaf_dealloc_ticks;
	#endif
};
typedef struct vbpt_mm_stats vbpt_mm_stats_t;

void vbpt_mm_stats_get(vbpt_mm_stats_t *st);

#endif
