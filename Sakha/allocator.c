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

#include "allocator.h"

#include <assert.h>
#include <stdlib.h>

#include "logger.h"
#include "sakhadb.h"
#include "llist.h"

/**
 * Turn on/off logging for paging routines
 */
#define SLOG_ALLOCATOR_ENABLE    0

#if SLOG_ALLOCATOR_ENABLE
#   define SLOG_ALLOCATOR_INFO  SLOG_INFO
#   define SLOG_ALLOCATOR_WARN  SLOG_WARN
#   define SLOG_ALLOCATOR_ERROR SLOG_ERROR
#   define SLOG_ALLOCATOR_FATAL SLOG_FATAL
#else // SLOG_ALLOCATOR_ENABLE
#   define SLOG_ALLOCATOR_INFO(...)
#   define SLOG_ALLOCATOR_WARN(...)
#   define SLOG_ALLOCATOR_ERROR(...)
#   define SLOG_ALLOCATOR_FATAL(...)
#endif // SLOG_ALLOCATOR_ENABLE

/********************** Default Allocator Implementation **********************/
struct Allocator
{
    void* (*xAllocate)(struct Allocator*, size_t);
    void  (*xFree)(struct Allocator*, void* ptr);
};

void* defaultMalloc(struct Allocator* pAllocator, size_t sz)
{
    return malloc(sz);
}

void defaultFree(struct Allocator* pAllocator, void* ptr)
{
    free(ptr);
}

/*********************** Pool Allocator Implementation ************************/
struct PoolAllocator
{
    void* (*xAllocate)(struct Allocator*, size_t);
    void  (*xFree)(struct Allocator*, void* ptr);
    void*   pool;
    size_t  poolSize;
    size_t  chunkSize;
    int     nChunks;
    
    struct LinkedList* head;
};

void* poolMalloc(struct Allocator* pAllocator, size_t sz)
{
    struct PoolAllocator* pPoolAllocator = (struct PoolAllocator *)pAllocator;
    assert(sz == pPoolAllocator->chunkSize);
    void* ptr = pPoolAllocator->head;
    llist_remove_head(pPoolAllocator->head);
    return ptr;
}

void poolFree(struct Allocator* pAllocator, void* ptr)
{
    struct PoolAllocator* pPoolAllocator = (struct PoolAllocator *)pAllocator;
    llist_add(pPoolAllocator->head, (struct LinkedList *)ptr);
}

/*********************** Public Allocator routines  ***************************/
sakhadb_allocator_t sakhadb_allocator_get_default()
{
    static struct Allocator _default_allocator = { defaultMalloc, defaultFree };
    return &_default_allocator;
}

void* sakhadb_allocator_allocate(sakhadb_allocator_t allocator, size_t sz)
{
    return allocator->xAllocate(allocator, sz);
}

void sakhadb_allocator_free(sakhadb_allocator_t allocator, void* ptr)
{
    return allocator->xFree(allocator, ptr);
}

int sakhadb_allocator_create_pool(size_t chunkSize, int nChunks, sakhadb_allocator_t* pAllocator)
{
    SLOG_ALLOCATOR_INFO("sakhadb_allocator_create_pool: create pool allocator [chunk:%u][count:%d]", chunkSize, nChunks);
    assert(chunkSize < 8192 && chunkSize > 128);
    
    size_t poolSize = chunkSize * nChunks;
    char* poolBuffer = sakhadb_allocator_allocate(sakhadb_allocator_get_default(), sizeof(struct PoolAllocator) + poolSize);
    if(!poolBuffer)
    {
        SLOG_ALLOCATOR_FATAL("sakhadb_allocator_create_pool: failed to allocate memory.");
        return SAKHADB_NOMEM;
    }
    
    struct PoolAllocator* poolAllocator = (struct PoolAllocator*)(poolBuffer + poolSize);
    
    poolAllocator->xAllocate = poolMalloc;
    poolAllocator->xFree = poolFree;
    poolAllocator->pool = poolBuffer;
    poolAllocator->poolSize = poolSize;
    poolAllocator->chunkSize = chunkSize;
    poolAllocator->nChunks = nChunks;
    poolAllocator->head = 0;
    
    for (int i = nChunks-1; i >= 0; --i)
    {
        struct LinkedList* item = (struct LinkedList *)(poolBuffer + i*chunkSize);
        llist_add(poolAllocator->head, item);
    }
    
    *pAllocator = (sakhadb_allocator_t)poolAllocator;
    return SAKHADB_OK;
}

int sakhadb_allocator_destroy_pool(sakhadb_allocator_t allocator)
{
    assert(allocator != sakhadb_allocator_get_default());
    
    SLOG_ALLOCATOR_INFO("sakhadb_allocator_destroy: destroy pool allocator");
    sakhadb_allocator_free(sakhadb_allocator_get_default(), allocator);
    
    return SAKHADB_OK;
}
