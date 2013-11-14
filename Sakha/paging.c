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

#include "paging.h"

#include <stdlib.h>

#include "sakhadb.h"
#include "logger.h"

struct Pager
{
    sakhadb_file_t  fd;             /* File handle */
    Pgno            dbSize;         /* Number of pages in database */
    Pgno            fileSize;       /* Size of the file in pages */
    sakhadb_page_t* page1;          /* Pointer to the first page in file. */
    
    int32_t         pageSize;       /* Page size for current database */
};

int sakhadb_pager_create(const sakhadb_file_t fd, sakhadb_pager_t* pPager)
{
    struct Pager* pager = (struct Pager*)malloc(sizeof(struct Pager));
    if(!pager)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to allocate memory for pager");
        return SAKHADB_NOMEM;
    }
    
    pager->fd = fd;
    
    // TODO: calculate size of file
    // TODO: calculate size of database
    pager->pageSize = SAKHADB_DEFAULT_PAGE_SIZE;
    
    // TODO: fetch first page from database
    pager->page1 = 0;
    
    *pPager = pager;
    return SAKHADB_OK;
}

int sakhadb_pager_destroy(sakhadb_pager_t pager)
{
    // TODO: release page1
    free(pager);
    return SAKHADB_OK;
}
