/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#ifndef VBPT_GV_H
#define VBPT_GV_H

void *vbpt_gv_add_node(vbpt_node_t *node);
void vbpt_gv_write(char *fname);
void vbpt_gv_reset(void);

#endif /* VBPT_GV_H */
