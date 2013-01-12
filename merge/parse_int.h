#ifndef PARSE_INT_H__
#define PARSE_INT_H__

long parse_int(const char *s);
void parse_int_tuple(const char *s, int *tuple, unsigned tuple_size);

int tokenize_by_sep(char *str, char sep, int *idxs, int idxs_size);

#endif
