// Copyright (c) 2013-2014. Alex Komnin. All rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#ifndef _SAKHADB_CURSOR_H_
#define _SAKHADB_CURSOR_H_

#include <cpl/cpl_array.h>

#include "btree.h"

typedef struct BtreeCursorStack* sakhadb_btree_cursor_t;
struct BtreeCursorStack
{
    sakhadb_btree_t         tree;
    cpl_array_t             st;         /* stack of cursors */
    int                     dirty;
    cpl_region_t            region;
};

int sakhadb_btree_cursor_create(sakhadb_btree_t tree, sakhadb_btree_cursor_t* cursor);
void sakhadb_btree_cursor_destroy(sakhadb_btree_cursor_t cursor);

int sakhadb_btree_cursor_first(sakhadb_btree_cursor_t cursor);
int sakhadb_btree_cursor_last(sakhadb_btree_cursor_t cursor);

int sakhadb_btree_cursor_next(sakhadb_btree_cursor_t cursor);
int sakhadb_btree_cursor_prev(sakhadb_btree_cursor_t cursor);

Pgno sakhadb_btree_cursor_pgno(sakhadb_btree_cursor_t cursor);
int sakhadb_cursor_find(sakhadb_btree_cursor_t cursor, const void* key, size_t nkey);
int sakhadb_btree_cursor_insert(sakhadb_btree_cursor_t cursor, const void* key, size_t nkey, Pgno no);

#endif // _SAKHADB_CURSOR_H_
