/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

/*************************************************************************
 *
 * (general) parsing helpers
 *  (used for option parsing)
 *
 *********************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

long
parse_int(const char *s)
{
    long ret;
    char *endptr;

    ret = strtol(s, &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "parse error: '%s' is not a number\n", s);
        exit(1);
    }

    return ret;
}

// @sep is replaced by '\0', and the index of the next character is returned in
// @idxs
// returns -1 if failed
int
tokenize_by_sep(char *str, char sep, int *idxs, int idxs_size)
{
    unsigned idxs_i, str_i;

    str_i = 0;
    for (idxs_i = 0; idxs_i < idxs_size; idxs_i++) {
        for (;;) {
            char c = str[str_i++];
            if (c == sep) {
                str[str_i-1] = '\0';
                idxs[idxs_i] = str_i;
                break;
            } else if (c == '\0') {
                return -1;
            }
        }
    }

    return 0;
}

/*
 * parse a tuple of integers seperated by commas -- e.g., 1,2,3.
 * put the result in @tuple array.
 *  NULL -> tuple remains unchanged
 *  ,,1  -> only tuple[2] is changed
 */
void
parse_int_tuple(const char *s, int *tuple, unsigned tuple_size)
{
    if (s == NULL || tuple_size == 0)
        return;

    // do a copy
    size_t s_size = strlen(s) + 1;
    char str[s_size];
    memcpy(str, s, s_size);

    const char sep = ',';
    unsigned t_i=0, s_i=0;
    for(;;) {
        unsigned start = s_i;
        for (;;) {
            char c = str[s_i++];
            if (c == '\0' || c == sep)
                break;
        }
        unsigned end = s_i - 1;

        bool done = (str[end] == '\0');

        if (start != end) {
            str[end] = '\0';
            tuple[t_i] = parse_int(str + start);
        }

        if (done || ++t_i >= tuple_size)
            break;
    }
}

#if defined(PARSE_INT_TEST)
static void __attribute__((unused))
parse_int_tuple_test(void)
{
    static struct {
        const char *str;
        const int  tuple[16];
        unsigned   tuple_size;
    } test_data[] = {
        {"1,2,3", { 1, 2, 3}, 3},
        {"1,,3",  { 1,-1, 3}, 3},
        {",,," ,  {-1,-1,-1}, 3},
        {""    ,  {-1,-1,-1}, 3},
        {",2," ,  {-1, 2,-1}, 3},
        { NULL  , {0       }, 0}
    };

    for (unsigned ti=0; ;ti++) {
        const char *str     = test_data[ti].str;
        const int  *tuple   = test_data[ti].tuple;
        unsigned tuple_size = test_data[ti].tuple_size;

        if (str == NULL)
            break;

        int test_tuple[tuple_size];
        for (unsigned i=0; i<tuple_size; i++)
            test_tuple[i] = -1;

        parse_int_tuple(str, test_tuple, tuple_size);
        for (unsigned i=0; i<tuple_size; i++)
            if (test_tuple[i] != tuple[i]) {
                fprintf(stderr, "FAIL on '%s' t[%d] %d vs %d\n",
                        str, i, test_tuple[i], tuple[i]);
                abort();
            }
    }
}

int main(int argc, const char *argv[])
{
    parse_int_tuple_test();
    return 0;
}
#endif
