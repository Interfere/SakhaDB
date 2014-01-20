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
#include <cpl/cpl_allocator.h>
#include "btree.h"

/**
 * Turn on/off logging for paging routines
 */
#define SLOG_PAGING_ENABLE    1

#if SLOG_PAGING_ENABLE
#   define SLOG_PAGING_INFO  SLOG_INFO
#   define SLOG_PAGING_WARN  SLOG_WARN
#   define SLOG_PAGING_ERROR SLOG_ERROR
#   define SLOG_PAGING_FATAL SLOG_FATAL
#else // SLOG_PAGING_ENABLE
#   define SLOG_PAGING_INFO(...)
#   define SLOG_PAGING_WARN(...)
#   define SLOG_PAGING_ERROR(...)
#   define SLOG_PAGING_FATAL(...)
#endif // SLOG_PAGING_ENABLE

#define sakhadb_pager_get_header(p) ((struct Header*)((p)->page1->pData))

struct InternalPage
{
    Pgno        pageNumber;         /* Number of the page */
    char*       pData;              /* Data buffer */
    
    /* Private */
    struct Pager* pPager;           /* Pager, which this page belongs to */
    int         isDirty;            /* Should be synced */
    struct InternalPage *next;              /* Linked list of pages */
    struct InternalPage *dnext;             /* Dirty next. Useful when page marked as dirty. */
};

struct PagesHashTable
{
    struct InternalPage* _ht[1024];         /* Table of pointers */
};

struct Pager
{
    cpl_allocator_ref   allocator;      /* Allocator to use */
    cpl_allocator_ref   pageAllocator;  /* Allocator for page */
    cpl_allocator_ref   contentAllocator; /* Allocator for page content */
    sakhadb_file_t      fd;             /* File handle */
    Pgno                dbSize;         /* Number of pages in database */
    Pgno                fileSize;       /* Size of the file in pages */
    struct InternalPage *page1;         /* Pointer to the first page in file. */
    uint16_t            pageSize;       /* Page size for current database */
    
    struct PagesHashTable table;    /* Hash table to store pages */
    struct InternalPage *dirty;         /* List of pages to sync */
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
    Pgno            freelist;       /* Freelist */
    char            reserved2[28];  /* Reserved for future */
    struct BtreePageHeader page1Header;    /* Header of first page */
};


/**
 * Add a page into hash table. No-op if page exists.
 */
static void addPageToTable(struct Pager* pager, struct InternalPage* page)
{
    assert(page);
    int hash = page->pageNumber % sizeof(pager->table._ht);
    page->next = pager->table._ht[hash];
    pager->table._ht[hash] = page;
}

/**
 * Remove page from hash table. HashTable must contain the page.
 */
static void removePageFromTable(struct Pager* pager, struct InternalPage* page)
{
    assert(page);
    int hash = page->pageNumber % sizeof(pager->table._ht);
    struct InternalPage* pPage = pager->table._ht[hash];
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

static void markAsDirty(struct InternalPage* pPage)
{
    SLOG_PAGING_INFO("markAsDirty: mark page as dirty [%lld]", pPage->pageNumber);
    if(!pPage->isDirty)
    {
        pPage->isDirty = 1;
        pPage->dnext = pPage->pPager->dirty;
        pPage->pPager->dirty = pPage;
    }
}

/**
 * Lookup page into hash table. Returns 0 if page is not present.
 */
static struct InternalPage* lookupPageInTable(struct Pager* pager, Pgno no)
{
    assert(no > 0);
    int hash = no % sizeof(pager->table._ht);
    struct InternalPage* pPage = pager->table._ht[hash];
    while (pPage && pPage->pageNumber != no) pPage = pPage->next;
    return pPage;
}

/**
 * The Page object contructor.
 */
static int createPage(
    struct Pager *pPager,           /* Pager object, that owns the page */
    Pgno pageNumber,                /* Number of the page */
    struct InternalPage **ppPage
)
{
    struct InternalPage *pPage = (struct InternalPage *)cpl_allocator_allocate(pPager->pageAllocator, sizeof(struct InternalPage));
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
static void destroyPage(struct InternalPage *pPage)
{
    removePageFromTable(pPage->pPager, pPage);
    cpl_allocator_free(pPage->pPager->contentAllocator, pPage->pData);
    cpl_allocator_free(pPage->pPager->pageAllocator, pPage);
}

/**
 * Read data from file
 */
static int fetchPageContent(struct InternalPage *pPage)
{
    Pgno pageNumber = pPage->pageNumber;
    int32_t pageSize = pPage->pPager->pageSize;
    
    assert(pageNumber);
    assert(pageSize > 512);
    
    int rc = SAKHADB_OK;
    
    /* Pre-allocate buffer for content */
    if(!pPage->pData)
    {
        pPage->pData = cpl_allocator_allocate(pPage->pPager->contentAllocator, pageSize);
    }
    
    if(!pPage->pData)
    {
        SLOG_PAGING_FATAL("fetchPageContent: failed to pre-allocate buffer for page content.");
        return SAKHADB_NOMEM;
    }
    
#ifdef DEBUG
    memset(pPage->pData, 0xFF, pageSize);
#endif
    
    if(pageNumber <= pPage->pPager->fileSize)
    {
        int64_t offset = (int64_t)(pageNumber-1) * pageSize;
        rc = sakhadb_file_read(pPage->pPager->fd, pPage->pData, pageSize, offset);
    }
    return rc;
}

/**
 * Pre-load some pages
 */
static int preloadPages(
    struct Pager *pPager,           /* Pager object, that owns the page */
    Pgno startNo,                   /* No of first page to load */
    Pgno endNo                      /* No of last page to load */
)
{
    int rc = SAKHADB_OK;
    Pgno npages = endNo - startNo + 1;
    
    for (Pgno i = 0; i <= npages; ++i)
    {
        struct InternalPage* pPage;
        
        rc = createPage(pPager, i + startNo, &pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to create page [%d]", i+startNo);
            break;
        }
        
        rc = fetchPageContent(pPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to fetch page content. [%d]", i+startNo);
            break;
        }
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
    struct InternalPage* page1 = pager->page1;
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
        header->freelist = 0;
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
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    struct Pager* pager = (struct Pager*)cpl_allocator_allocate(default_allocator, sizeof(struct Pager));
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
    
    pager->pageAllocator = cpl_allocator_create_pool(sizeof(struct InternalPage), 1024, 0);
    if(!pager->pageAllocator)
    {
        SLOG_PAGING_ERROR("sakhadb_pager_create: failed to create pool allocator.");
        rc = SAKHADB_NOMEM;
        goto page_allocator_failed;
    }
    
    pager->contentAllocator = cpl_allocator_create_pool(pager->pageSize, 1024, 0x1000);
    if(!pager->pageAllocator)
    {
        SLOG_PAGING_ERROR("sakhadb_pager_create: failed to create pool allocator.");
        rc = SAKHADB_NOMEM;
        goto content_allocator_failed;
    }
    
    rc = createPage(pager, 1, &pager->page1);
    if(rc != SAKHADB_OK)
    {
        SLOG_PAGING_FATAL("sakhadb_pager_create: failed to create page 1. [%s]");
        goto content_allocator_failed;
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
    
    SLOG_PAGING_INFO("sakhadb_pager_create: preload 100 pages");
    rc = preloadPages(pager, 2, 100);
    if(rc != SAKHADB_OK)
    {
        goto preload_failed;
    }
    
    *pPager = pager;
    return SAKHADB_OK;
    
preload_failed:
    // TODO: free peloaded pages
    
fetch_failed:
    destroyPage(pager->page1);
    
content_allocator_failed:
    cpl_allocator_destroy_pool(pager->contentAllocator);
    
page_allocator_failed:
    cpl_allocator_destroy_pool(pager->pageAllocator);
    
cleanup:
    cpl_allocator_free(default_allocator, pager);
    return rc;
}

int sakhadb_pager_destroy(sakhadb_pager_t pager)
{
    SLOG_PAGING_INFO("sakhadb_pager_destroy: destroying pager.");
    destroyPage(pager->page1);
    cpl_allocator_free(pager->allocator, pager);
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

int sakhadb_pager_request_page(sakhadb_pager_t pager, Pgno no, int readonly, sakhadb_page_t* pPage)
{
    SLOG_PAGING_INFO("sakhadb_pager_request_page: requesting page [%d][%s]", no, readonly?"r":"rw");
    if(no == 1)
    {
        *pPage = (sakhadb_page_t)pager->page1;
        return SAKHADB_OK;
    }
    
    int isNew = no > pager->dbSize;
    if(readonly && isNew)
    {
        SLOG_PAGING_WARN("sakhadb_pager_request_page: requested non-existing readonly page.");
        return SAKHADB_NOTAVAIL;
    }
    
    SLOG_PAGING_INFO("sakhadb_pager_request_page: looking for page in table.");
    struct InternalPage* pInternalPage = lookupPageInTable(pager, no);
    if(!pInternalPage)
    {
        SLOG_PAGING_INFO("sakhadb_pager_request_page: page not found. create new.");
        int rc = createPage(pager, no, &pInternalPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to create page [%d]", no);
            return rc;
        }
        
        SLOG_PAGING_INFO("sakhadb_pager_request_page: fetch page content");
        rc = fetchPageContent(pInternalPage);
        if(rc != SAKHADB_OK)
        {
            SLOG_PAGING_ERROR("sakhadb_pager_request_page: failed to fetch page content. [%d]", no);
            return rc;
        }
    }
    
    if(!readonly)
    {
        markAsDirty(pInternalPage);
    }
    
    *pPage = (sakhadb_page_t)pInternalPage;
    
    return SAKHADB_OK;
}

int sakhadb_pager_request_free_page(sakhadb_pager_t pager, sakhadb_page_t* pPage)
{
    struct Header* h = sakhadb_pager_get_header(pager);
    if(h->freelist == 0)
    {
        return sakhadb_pager_request_page(pager, pager->dbSize+1, 0, pPage);
    }
    
    int res = sakhadb_pager_request_page(pager, h->freelist, 0, pPage);
    if(res == SAKHADB_OK)
    {
        h->freelist = *(Pgno*)((*pPage)->data);
        markAsDirty(pager->page1);
    }
    return res;
}

void sakhadb_pager_add_freelist(sakhadb_pager_t pager, sakhadb_page_t page)
{
    SLOG_PAGING_INFO("sakhadb_pager_add_freelist: freeing page [%d]", page->no);
    Pgno* pNo = (Pgno *)page->data;
    struct Header* h = sakhadb_pager_get_header(pager);
    *pNo = h->freelist;
    h->freelist = page->no;
    
    markAsDirty(pager->page1);
}
