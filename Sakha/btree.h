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

#ifndef _SAKHADB_BTREE_H_
#define _SAKHADB_BTREE_H_

#include <stdint.h>
#include <bson/document.h>
#include "paging.h"

typedef struct Btree* sakhadb_btree_t;
typedef struct BtreeContext* sakhadb_btree_ctx_t;
typedef struct BtreePageHeader* sakhadb_btree_node_t;
typedef struct BtreeCursor* sakhadb_btree_cursor_t;

int sakhadb_btree_ctx_create(sakhadb_file_t __restrict h, sakhadb_btree_ctx_t* ctx);
int sakhadb_btree_ctx_destroy(sakhadb_btree_ctx_t env);

sakhadb_btree_t sakhadb_btree_ctx_get_meta(sakhadb_btree_ctx_t env);
int sakhadb_btree_ctx_commit(sakhadb_btree_ctx_t env);

#endif // _SAKHADB_BTREE_H_
