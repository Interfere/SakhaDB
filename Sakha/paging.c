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

#include "sakhadb.h"
#include "logger.h"

struct Page
{
    struct Pager* pPager;           /* Pager, which this page belongs to */
    Pgno        pageNumber;         /* Page's number */
    int         isNew;              /* Indicates whether the page persists in DB file */
    char        pData[1];           /* Data buffer */
};

struct Pager
{
    sakhadb_file_t  fd;             /* File handle */
    Pgno            dbSize;         /* Number of pages in database */
    Pgno            fileSize;       /* Size of the file in pages */
    struct Page     *page1;         /* Pointer to the first page in file. */
    uint16_t        pageSize;       /* Page size for current database */
};

/**
 * The Page object contructor.
 */
static int createPage(
    struct Pager *pPager,           /* Pager object, that owns the page */
    Pgno pageNumber,                /* Number of the page */
    int isNew,                      /* Is the page would be new */
    struct Page **ppPage
)
{
    struct Page *pPage = (struct Page *)malloc(sizeof(struct Page) + pPager->pageSize);
    if(!pPage)
    {
        SLOG_FATAL("createPage: failed to allocate memory for Page object.");
        return SAKHADB_NOMEM;
    }
    
    pPage->pPager = pPager;
    pPage->pageNumber = pageNumber;
    pPage->isNew = isNew;
    
    *ppPage = pPage;
    return SAKHADB_OK;
}

/**
 * The Page object destructor.
 */
static void destroyPage(struct Page *pPage)
{
    free(pPage);
}

/**
 * Read data from file
 */
static int fetchPageContent(struct Page *pPage)
{
    Pgno pageNumber = pPage->pageNumber;
    int32_t pageSize = pPage->pPager->pageSize;
    int64_t offset = (int64_t)pageNumber * pageSize;
    
    assert(pageNumber);
    assert(pageSize > 512);
    
    return sakhadb_file_read(pPage->pPager->fd, pPage->pData, pageSize, offset);
}

int sakhadb_pager_create(const sakhadb_file_t fd, sakhadb_pager_t* pPager)
{
    struct Pager* pager = (struct Pager*)malloc(sizeof(struct Pager));
    if(!pager)
    {
        SLOG_FATAL("sakhadb_pager_create: failed to allocate memory for pager");
        return SAKHADB_NOMEM;
    }
    
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
    
    rc = createPage(pager, 1, pager->fileSize == 0, &pager->page1);
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
    free(pager);
    return rc;
}

int sakhadb_pager_destroy(sakhadb_pager_t pager)
{
    destroyPage(pager->page1);
    free(pager);
    return SAKHADB_OK;
}
