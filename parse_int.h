/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#ifndef PARSE_INT_H__
#define PARSE_INT_H__

long parse_int(const char *s);
void parse_int_tuple(const char *s, int *tuple, unsigned tuple_size);

int tokenize_by_sep(char *str, char sep, int *idxs, int idxs_size);

#endif
