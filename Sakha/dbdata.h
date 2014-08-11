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
/**
 * API for managing data pages in DB file.
 */

#ifndef _SAKHADB_DBDATA_H_
#define _SAKHADB_DBDATA_H_

#include "paging.h"

#include <cpl/cpl_region.h>

typedef struct DBData* sakhadb_dbdata_t;

int sakhadb_dbdata_create(sakhadb_pager_t pager, sakhadb_dbdata_t* data);
void sakhadb_dbdata_destroy(sakhadb_dbdata_t);

int sakhadb_dbdata_write(sakhadb_dbdata_t dbdata, const void* data, size_t ndata, Pgno* pNo);
int sakhadb_dbdata_read(sakhadb_dbdata_t dbdata, Pgno no, cpl_region_ref reg);

int sakhadb_dbdata_preload(sakhadb_dbdata_t dbdata, Pgno no, void** ppData);

#endif // _SAKHADB_DBDATA_H_
