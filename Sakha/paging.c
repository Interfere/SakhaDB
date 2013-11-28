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

/**
 * Turn on/off logging for file routines
 */
#define SLOG_PAGING_ENABLE    1

#if SLOG_PAGING_ENABLE
#define SLOG_PAGING_INFO  SLOG_INFO
#define SLOG_PAGING_WARN  SLOG_WARN
#define SLOG_PAGING_ERROR SLOG_ERROR
#define SLOG_PAGING_FATAL SLOG_FATAL
#else // SLOG_FILE_ENABLE
#define SLOG_PAGING_INFO(...)
#define SLOG_PAGING_WARN(...)
#define SLOG_PAGING_ERROR(...)
#define SLOG_PAGING_FATAL(...)
#endif // SLOG_FILE_ENABLE

struct Page
{
    struct Pager* pPager;           /* Pager, which this page belongs to */
    Pgno        pageNumber;         /* Number of the page */
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
 * Database header.
 */
struct Header
{
    char            id[16];         /* Magic string */
    uint16_t        pageSize;       /* Actual page size for current DB */
    char            reserved1[2];
    uint32_t        dbVersion;      /* Version of Database */
    char            reserved2[40];  /* Reserved for future */
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
    SLOG_PAGING_INFO("markAsDirty: mark page as dirty [%lld]", pPage->pageNumber);
    pPage->isDirty = 1;
    pPage->dnext = pPage->pPager->dirty;
    pPage->pPager->dirty = pPage;
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
    Pgno pageNumber,                /* Number of the page */
    struct Page **ppPage
)
{
    struct Page *pPage = (struct Page *)sakhadb_allocator_allocate(pPager->allocator, sizeof(struct Page));
    if(!pPage)
    {
        SLOG_PAGING_FATAL("createPage: failed to allocate memory for Page object.");
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
    if(!pPage->pData)
    {
        pPage->pData = sakhadb_allocator_allocate(pPage->pPager->contentAllocator, pageSize);
    }
    
    if(!pPage->pData)
    {
        SLOG_PAGING_FATAL("fetchPageContent: failed to pre-allocate buffer for page content.");
        return SAKHADB_NOMEM;
    }
    
    if(pageNumber <= pPage->pPager->fileSize)
    {
        int64_t offset = (int64_t)pageNumber * pageSize;
        rc = sakhadb_file_read(pPage->pPager->fd, pPage->pData, pageSize, offset);
    }
    return rc;
}

/**
 * Acquire DB header.
 */
static int acquireHeader(
    struct Pager* pager
)
{
    /* Page 1 must be loaded before this call */
    struct Page* page1 = pager->page1;
    assert(page1);
    
    struct Header *header = (struct Header *)page1->pData;
    if(pager->dbSize == 1 && pager->fileSize == 0)
    {
        // No page on disk. Create header.
        memset(header->id, 0, sizeof(header->id));
        strncpy(header->id, SAKHADB_FILE_HEADER, sizeof(header->id));
        header->pageSize = pager->pageSize;
        header->reserved1[0] = header->reserved1[1] = 0;
        header->dbVersion = SAKHADB_VERSION_NUMBER;
        memset(header->reserved2, 0, sizeof(header->reserved2));
        
        markAsDirty(page1);
    }
    else
    {
        // Compare values
        if(strncmp(header->id, SAKHADB_FILE_HEADER, sizeof(header->id)) != 0)
        {
            SLOG_PAGING_FATAL("acquireHeader: file header do not match");
            return SAKHADB_NOTADB;
        }
        
        if(header->dbVersion > SAKHADB_VERSION_NUMBER)
        {
            SLOG_PAGING_ERROR("acquireHeader: creator version is higher than reader's.");
            return SAKHADB_CANTOPEN;
        }
        
        uint16_t pageSize = header->pageSize;
        if(pageSize != pager->pageSize)
        {
            SLOG_PAGING_WARN("acquireHeader: page size do not match.");
            pager->pageSize = pageSize;
            fetchPageContent(page1);
        }
    }
    return SAKHADB_OK;
}

/******************* Public API routines  ********************/

int sakhadb_pager_create(const sakhadb_file_t fd,
                         sakhadb_pager_t* pPager)
{
    SLOG_PAGING_INFO("sakhadb_pager_create: creating pager.");
    sakhadb_allocator_t default_allocator = sakhadb_allocator_get_default();
    struct Pager* pager = (struct Pager*)sakhadb_allocator_allocate(default_allocator, sizeof(struct Pager));
    if(!pager)
    {
        SLOG_PAGING_FATAL("sakhadb_pager_create: failed to allocate memory for pager");
        return SAKHADB_NOMEM;
    }
    
    pager->allocator = default_allocator;
    pager->fd = fd;
    pager->pageSize = SAKHADB_DEFAULT_PAGE_SIZE;
    
    int64_t fileSize = 0;
    int rc = sakhadb_file_size(fd, &fileSize);
    if(rc != SAKHADB_OK)
    {
        SLOG_PAGING_FATAL("sakhadb_pager_create: failed to get file size. [%s]", sakhadb_file_filename(fd));
        goto cleanup;
    }
    
    SLOG_PAGING_INFO("sakhadb_pager_create: got size of file [%lld].", fileSize);
    
    pager->fileSize = (Pgno)(fileSize/pager->pageSize);
    pager->dbSize = pager->fileSize?pager->fileSize:1;
    memset(pager->table._ht, 0, sizeof(pager->table._ht));
    
    rc = sakhadb_allocator_create_pool(pager->pageSize, 1024, &pager->contentAllocator);
    if(rc != SAKHADB_OK)
    {
        SLOG_PAGING_ERROR("sakhadb_pager_create: failed to create pool allocator.");
        goto allocator_failed;
    }
    rc = createPage(pager, 1, &pager->page1);
    if(rc != SAKHADB_OK)
    {
        SLOG_PAGING_FATAL("sakhadb_pager_create: failed to create page 1. [%s]");
        goto allocator_failed;
    }
    
    SLOG_PAGING_INFO("sakhadb_pager_create: created page1");
    
    rc = fetchPageContent(pager->page1);
    if(rc != SAKHADB_OK)
    {
        SLOG_PAGING_FATAL("sakhadb_pager_create: failed to fetch data for page 1.");
        goto fetch_failed;
    }
    
    SLOG_PAGING_INFO("sakhadb_pager_create: fetched content for page1");
    rc = acquireHeader(pager);
    if(rc != SAKHADB_OK)
    {
        goto fetch_failed;
    }
    
    *pPager = pager;
    return SAKHADB_OK;
    
fetch_failed:
    destroyPage(pager->page1);
    
allocator_failed:
    sakhadb_allocator_destroy_pool(pager->contentAllocator);
    
cleanup:
    sakhadb_allocator_free(default_allocator, pager);
    return rc;
}

int sakhadb_pager_destroy(sakhadb_pager_t pager)
{
    SLOG_PAGING_INFO("sakhadb_pager_destroy: destroying pager.");
    destroyPage(pager->page1);
    sakhadb_allocator_free(pager->allocator, pager);
    return SAKHADB_OK;
}

int sakhadb_pager_sync(sakhadb_pager_t pager)
{
    SLOG_PAGING_INFO("sakhadb_pager_sync: syncing pager.");
    while (pager->dirty) {
        int rc = sakhadb_file_write(pager->fd,
                                    pager->dirty->pData,
                                    pager->pageSize,
                                    (pager->dirty->pageNumber-1) * pager->pageSize);
        pager->dirty->isDirty = 0;
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_sync: failed to sync page.");
            return rc;
        }
        pager->dirty = pager->dirty->dnext;
    }
    return SAKHADB_OK;
}

int sakhadb_pager_request_page(sakhadb_pager_t pager, Pgno no, int readonly, void** ppData)
{
    SLOG_PAGING_INFO("sakhadb_pager_request_page: requesting page [%d][%s]", no, readonly?"r":"rw");
    if(no == 1)
    {
        *ppData = pager->page1->pData;
        return SAKHADB_OK;
    }
    
    int isNew = no > pager->dbSize;
    if(readonly && isNew)
    {
        SLOG_PAGING_WARN("sakhadb_pager_request_page: requested non-existing readonly page.");
        return SAKHADB_NOTAVAIL;
    }
    
    int rc = SAKHADB_OK;
    
    SLOG_PAGING_INFO("sakhadb_pager_request_page: looking for page in table.");
    struct Page* pPage = lookupPageInTable(pager, no);
    if(!pPage)
    {
        SLOG_PAGING_INFO("sakhadb_pager_request_page: page not found. create new.");
        rc = createPage(pager, no, &pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to create page [%d]", no);
            return rc;
        }
        
        SLOG_PAGING_INFO("sakhadb_pager_request_page: fetch page content");
        rc = fetchPageContent(pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to fetch page content. [%d]", no);
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
