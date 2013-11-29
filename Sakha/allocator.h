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

#ifndef _SAKHADB_ALLOCATOR_H_
#define _SAKHADB_ALLOCATOR_H_

#include <stdlib.h>

/**
 * Allocator type to use in SakhaDB internals
 */
typedef struct Allocator* sakhadb_allocator_t;

/**
 * Default allocator accessor. Represents basic allocation routines, such as
 * malloc() and free().
 */
sakhadb_allocator_t sakhadb_allocator_get_default();

/**
 * Constructor and Destructor for pool allocator. Used as main allocator 
 * for internal storage of page chache module.
 */
int sakhadb_allocator_create_pool(size_t chunkSize, int nChunks, sakhadb_allocator_t*);
int sakhadb_allocator_destroy_pool(sakhadb_allocator_t);

/**
 * Alloc() and Free() routines that encapsulates allocator-related internals.
 */
void* sakhadb_allocator_allocate(sakhadb_allocator_t, size_t);
void sakhadb_allocator_free(sakhadb_allocator_t, void*);

#endif // _SAKHADB_ALLOCATOR_H_
