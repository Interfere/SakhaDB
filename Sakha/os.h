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

#ifndef _SAKHADB_OS_H_
#define _SAKHADB_OS_H_

#include <stdint.h>
#include <stdio.h>

/**
 * File handler
 */
typedef struct sakhadb_file* sakhadb_file_t;

/**
 * Allocator type
 */
typedef struct Allocator* sakhadb_allocator_t;

/**
 * Flags to open the file
 */
#define SAKHADB_OPEN_READ       0x1
#define SAKHADB_OPEN_WRITE      0x2
#define SAKHADB_OPEN_READWRITE  (SAKHADB_OPEN_READ|SAKHADB_OPEN_WRITE)
#define SAKHADB_OPEN_CREATE     0x4
#define SAKHADB_OPEN_EXCLUSIVE  0x8

/**
 * Routines for working with FS
 */
int sakhadb_file_open(sakhadb_allocator_t, const char*, int, sakhadb_file_t*);
int sakhadb_file_close(sakhadb_file_t);

int sakhadb_file_read(sakhadb_file_t, void*, int, int64_t);
int sakhadb_file_write(sakhadb_file_t, const void*, int, int64_t);

int sakhadb_file_size(sakhadb_file_t, int64_t*);
const char* sakhadb_file_filename(sakhadb_file_t);

/**
 * Allocators types
 */
typedef enum
{
    sakhadb_default_allocator = 0,
    sakhadb_pool_allocator
} sakhadb_allocator_type_t;

/**
 * Create allocator.
 */
int sakhadb_allocator_get_default(sakhadb_allocator_type_t type, sakhadb_allocator_t* pAllocator);

void* sakhadb_allocator_allocate(sakhadb_allocator_t allocator, size_t sz);
void sakhadb_allocator_free(sakhadb_allocator_t allocator, void* ptr);

#endif // _SAKHADB_OS_H_
