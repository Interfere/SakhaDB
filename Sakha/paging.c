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

#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "sakhadb.h"
#include "logger.h"

struct Page
{
    struct Pager* pPager;           /* Pager, which this page belongs to */
    Pgno        pageNumber;         /* Page's number */
    int         isDirty;            /* Should be synced */
    char*       pData;              /* Data buffer */
    struct Page *next;              /* Linked list of pages */
    struct Page *dnext;             /* Dirty next. Useful when page marked as dirty. */
};

struct PagesHashTable
{
    struct Page* _ht[1024];         /* Table of pointers */
};

struct Pager
{
    sakhadb_allocator_t allocator;  /* Allocator to use */
    sakhadb_allocator_t contentAllocator; /* Allocator for page content */
    sakhadb_file_t  fd;             /* File handle */
    Pgno            dbSize;         /* Number of pages in database */
    Pgno            fileSize;       /* Size of the file in pages */
    struct Page     *page1;         /* Pointer to the first page in file. */
    uint16_t        pageSize;       /* Page size for current database */
    
    struct PagesHashTable table;    /* Hash table to store pages */
    struct Page     *dirty;         /* List of pages to sync */
};

/**
 * Add a page into hash table. No-op if page exists.
 */
static void addPageToTable(struct Pager* pager, struct Page* page)
{
    assert(page);
    int hash = page->pageNumber % sizeof(pager->table._ht);
    page->next = pager->table._ht[hash];
    pager->table._ht[hash] = page;
}

static void removePageFromTable(struct Pager* pager, struct Page* page)
{
    assert(page);
    int hash = page->pageNumber % sizeof(pager->table._ht);
    struct Page* pPage = pager->table._ht[hash];
    if(pPage == page)
    {
        pager->table._ht[hash] = pPage->next;
    }
    else
    {
        while ((page != pPage->next) && (pPage = pPage->next));
        if(pPage)
        {
            pPage->next = page->next;
        }
    }
}

static void markAsDirty(struct Page* pPage)
{
    pPage->isDirty = 1;
    pPage->dnext = pPage->pPager->dirty;
    pPage->pPager->dirty = pPage->dnext;
}

/**
 * Lookup page into hash table. Returns 0 if page is not present.
 */
static struct Page* lookupPageInTable(struct Pager* pager, Pgno no)
{
    assert(no > 0);
    int hash = no % sizeof(pager->table._ht);
    struct Page* pPage = pager->table._ht[hash];
    while (pPage && pPage->pageNumber != no) pPage = pPage->next;
    return pPage;
}

/**
 * The Page object contructor.
 */
static int createPage(
    struct Pager *pPager,           /* Pager object, that owns the page */
    sakhadb_allocator_t dataAllocator, /* Allocator for page data */
    Pgno pageNumber,                /* Number of the page */
    struct Page **ppPage
)
{
    struct Page *pPage = (struct Page *)sakhadb_allocator_allocate(pPager->allocator, sizeof(struct Page));
    if(!pPage)
    {
        SLOG_FATAL("createPage: failed to allocate memory for Page object.");
        return SAKHADB_NOMEM;
    }
    
    pPage->pPager = pPager;
    pPage->pageNumber = pageNumber;
    pPage->pData = 0;
    pPage->isDirty = 0;
    pPage->dnext = 0;
    
    addPageToTable(pPager, pPage);
    
    *ppPage = pPage;
    return SAKHADB_OK;
}

/**
 * The Page object destructor.
 */
static void destroyPage(struct Page *pPage)
{
    removePageFromTable(pPage->pPager, pPage);
    sakhadb_allocator_free(pPage->pPager->contentAllocator, pPage->pData);
    sakhadb_allocator_free(pPage->pPager->allocator, pPage);
}

/**
 * Read data from file
 */
static int fetchPageContent(struct Page *pPage)
{
    Pgno pageNumber = pPage->pageNumber;
    int32_t pageSize = pPage->pPager->pageSize;
    
    assert(pageNumber);
    assert(pageSize > 512);
    
    int rc = SAKHADB_OK;
    
    /* Pre-allocate buffer for content */
    assert(pPage->pData == 0);
    pPage->pData = sakhadb_allocator_allocate(pPage->pPager->contentAllocator, pageSize);
    if(!pPage->pData)
    {
        SLOG_FATAL("fetchPageContent: failed to pre-allocate buffer for page content.");
        return SAKHADB_NOMEM;
    }
    
    if(pageNumber <= pPage->pPager->fileSize)
    {
        int64_t offset = (int64_t)pageNumber * pageSize;
        rc = sakhadb_file_read(pPage->pPager->fd, pPage->pData, pageSize, offset);
    }
    return rc;
}

/******************* Public API routines  ********************/

int sakhadb_pager_create(sakhadb_allocator_t allocator,
                         const sakhadb_file_t fd,
                         sakhadb_pager_t* pPager)
{
    struct Pager* pager = (struct Pager*)sakhadb_allocator_allocate(allocator, sizeof(struct Pager));
    if(!pager)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to allocate memory for pager");
        return SAKHADB_NOMEM;
    }
    
    pager->allocator = allocator;
    pager->fd = fd;
    pager->pageSize = SAKHADB_DEFAULT_PAGE_SIZE;
    
    int64_t fileSize = 0;
    int rc = sakhadb_file_size(fd, &fileSize);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to get file size. [%s]", sakhadb_file_filename(fd));
        goto cleanup;
    }
    
    pager->fileSize = (Pgno)(fileSize/pager->pageSize);
    pager->dbSize = pager->fileSize?pager->fileSize:1;
    memset(pager->table._ht, 0, sizeof(pager->table._ht));
    
    rc = sakhadb_allocator_get_default(sakhadb_default_allocator, &pager->contentAllocator);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to get default allocator for page content.");
        goto cleanup;
    }
    
    rc = createPage(pager, pager->contentAllocator, 1, &pager->page1);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to create page 1. [%s]");
        goto cleanup;
    }
    
    if(pager->fileSize > 0)
    {
        rc = fetchPageContent(pager->page1);
        if(rc != SAKHADB_OK)
        {
            SLOG_FATAL("sakhadb_pager_create: failed to fetch data for page 1.");
            goto fetch_failed;
        }
    }
    
    *pPager = pager;
    return SAKHADB_OK;
    
fetch_failed:
    destroyPage(pager->page1);
    
cleanup:
    sakhadb_allocator_free(allocator, pager);
    return rc;
}

int sakhadb_pager_destroy(sakhadb_pager_t pager)
{
    destroyPage(pager->page1);
    sakhadb_allocator_free(pager->allocator, pager);
    return SAKHADB_OK;
}

int sakhadb_pager_sync(sakhadb_pager_t pager)
{
    while (pager->dirty) {
        int rc = sakhadb_file_write(pager->fd,
                                    pager->dirty->pData,
                                    pager->pageSize,
                                    pager->dirty->pageNumber * pager->pageSize);
        pager->dirty->isDirty = 0;
        if(rc != SAKHADB_OK)
        {
            SLOG_ERROR("sakhadb_pager_sync: failed to sync page.");
            return rc;
        }
        pager->dirty = pager->dirty->dnext;
    }
    return SAKHADB_OK;
}

int sakhadb_pager_request_page(sakhadb_pager_t pager, Pgno no, int readonly, void** ppData)
{
    if(no == 1)
    {
        *ppData = pager->page1->pData;
        return SAKHADB_OK;
    }
    
    int isNew = no > pager->dbSize;
    if(readonly && isNew)
    {
        return SAKHADB_NOTAVAIL;
    }
    
    int rc = SAKHADB_OK;
    struct Page* pPage = lookupPageInTable(pager, no);
    if(!pPage)
    {
        rc = createPage(pager, pager->contentAllocator, no, &pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_ERROR("sakhadb_pager_request_page: failed to create page [%d]", no);
            return rc;
        }
        
        rc = fetchPageContent(pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_ERROR("sakhadb_pager_request_page: failed to fetch page content. [%d]", no);
            goto cleanup;
        }
    }
    
    if(!readonly)
    {
        markAsDirty(pPage);
    }
    
    *ppData = pPage->pData;
    return SAKHADB_OK;
    
cleanup:
    return rc;
}
