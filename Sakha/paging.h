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

/**
 * Creates pager. Consider this method as constructor.
 */
int sakhadb_pager_create(sakhadb_allocator_t, const sakhadb_file_t, sakhadb_pager_t*);

/**
 * Destroy pager. Consider this method as destructor.
 */
int sakhadb_pager_destroy(sakhadb_pager_t);

/**
 * Creates page and reads content from file if available.
 *
 * You can request readonly page by specifying corresponding flag.
 * If 'readonly' flag had been set and page did not present in DB file
 * then routine would return SAKHADB_NOTAVAIL. 
 *
 * If 'readonly' flag had been unset and page did not present in DB file
 * then routine would create new page with ready-to-use content.
 */
int sakhadb_pager_get_page(sakhadb_pager_t pager, Pgno no, int readonly, void** ppData);


#endif // _SAKHADB_PAGING_H_