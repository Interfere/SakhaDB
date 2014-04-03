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

#ifndef _SAKHADB_PAGING_H_
#define _SAKHADB_PAGING_H_

#include <stdint.h>
#include "os.h"

/**
 * The default size of a database page.
 */
#ifndef SAKHADB_DEFAULT_PAGE_SIZE
#  define SAKHADB_DEFAULT_PAGE_SIZE 1024
#endif

/**
 * The type used to represent the page number. The first page in a file 
 * is called page 1. 0 is used to represent "not a page".
 */
typedef uint32_t Pgno;

/**
 * Each open file is managed by an instance of the "sakhadb_pager_t" object.
 */
typedef struct Pager* sakhadb_pager_t;
typedef struct Page*  sakhadb_page_t;
struct Page
{
    const Pgno no;         /* No of page */
    void*      data;       /* data itself */
};

/**
 * Creates pager. Consider this method as constructor.
 */
int sakhadb_pager_create(const sakhadb_file_t, sakhadb_pager_t*);

/**
 * Destroy pager. Consider this method as destructor.
 */
int sakhadb_pager_destroy(sakhadb_pager_t);

/**
 * Writes pages to file.
 */
int sakhadb_pager_sync(sakhadb_pager_t);

/**
 * Re-read corrupted pages from file.
 */
int sakhadb_pager_update(sakhadb_pager_t);

/**
 * Creates page and reads content from file if available.
 *
 * If 'readonly' flag had been unset and page did not present in DB file
 * then routine would create new page with ready-to-use content.
 */
int sakhadb_pager_request_page(sakhadb_pager_t pager, Pgno no, sakhadb_page_t* pPage);

/**
 * Save page.
 */
void sakhadb_pager_save_page(sakhadb_pager_t pager, sakhadb_page_t page);

/**
 * Requests next page available for use.
 */
int sakhadb_pager_request_free_page(sakhadb_pager_t pager, sakhadb_page_t* pPage);

/**
 * Marke the page as free and add it to freelist.
 */
void sakhadb_pager_add_freelist(sakhadb_pager_t pager, sakhadb_page_t page);

/**
 * Get page size
 */
size_t sakhadb_pager_page_size(sakhadb_pager_t pager, int page1);


#endif // _SAKHADB_PAGING_H_
