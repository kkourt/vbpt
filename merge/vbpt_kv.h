#ifndef VBPT_KV_H
#define VBPT_KV_H

#if !defined(VBPT_KV_DEFVAL)
#define VBPT_KV_DEFVALBYTE  0xf1 // so that we can use memset()
#define VBPT_KV_DEFVAL      0xf1f1f1f1f1f1f1f1
#endif

void vbpt_kv_insert(vbpt_tree_t *tree, uint64_t kv_key, uint64_t kv_val);

// kv_get returns the inserted value.
// If no value has been inserted, it returns VBPT_KV_DEFVAL
uint64_t vbpt_kv_get(vbpt_tree_t *tree, uint64_t key);

/**
 * Log operations
 */

uint64_t vbpt_logtree_kv_get(vbpt_tree_t *tree, uint64_t key);
void     vbpt_logtree_kv_insert(vbpt_tree_t *tree,
                                uint64_t kv_key, uint64_t kv_val);

#endif
