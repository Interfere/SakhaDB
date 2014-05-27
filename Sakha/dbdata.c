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

#include "dbdata.h"

#include <string.h>
#include <cpl/cpl_allocator.h>

#include "sakhadb.h"
#include "logger.h"

/**
 * Turn on/off logging for dbdata routines
 */
#define SLOG_DBDATA_ENABLE    1

#if SLOG_DBDATA_ENABLE
#   define SLOG_DBDATA_INFO  SLOG_INFO
#   define SLOG_DBDATA_WARN  SLOG_WARN
#   define SLOG_DBDATA_ERROR SLOG_ERROR
#   define SLOG_DBDATA_FATAL SLOG_FATAL
#else // SLOG_DBDATA_ENABLE
#   define SLOG_DBDATA_INFO(...)
#   define SLOG_DBDATA_WARN(...)
#   define SLOG_DBDATA_ERROR(...)
#   define SLOG_DBDATA_FATAL(...)
#endif // SLOG_DBDATA_ENABLE

/***************************** Private Interface ******************************/
struct DBData
{
    sakhadb_pager_t     pager;      /* middle interface for file representation */
};

/******************************************************************************/

/***************************** Public Interface *******************************/
int sakhadb_dbdata_create(sakhadb_pager_t pager, sakhadb_dbdata_t* dbdata)
{
    SLOG_DBDATA_INFO("sakhadb_dbdata_create: create dbdata [0x%x]", pager);
    struct DBData* pDbData = cpl_allocator_allocate(cpl_allocator_get_default(), sizeof(struct DBData));
    if(!pDbData)
    {
        SLOG_DBDATA_FATAL("sakhadb_dbdata_create: failed to allocate mamory for DbData.");
        return SAKHADB_NOMEM;
    }
    
    pDbData->pager = pager;
    
    *dbdata = pDbData;
    return SAKHADB_OK;
}

void sakhadb_dbdata_destroy(sakhadb_dbdata_t dbdata)
{
    cpl_allocator_free(cpl_allocator_get_default(), dbdata);
}

int sakhadb_dbdata_write(sakhadb_dbdata_t dbdata, const void* data, size_t ndata, Pgno* pNo)
{
    SLOG_DBDATA_INFO("sakhadb_dbdata_write: save data to page [0x%x][len: %d]", dbdata, ndata);
    int rc;
    Pgno* pData;
    const char* inData = data;
    size_t area_size = sakhadb_pager_page_size(dbdata->pager, 0) - sizeof(Pgno);
    
    sakhadb_page_t page;
    rc = sakhadb_pager_request_free_page(dbdata->pager, &page);
    if(rc)
    {
        SLOG_DBDATA_ERROR("sakhadb_dbdata_write: failed to fetch free page [%d]", rc);
        goto Lexit;
    }
    
    SLOG_DBDATA_INFO("sakhadb_dbdata_write: fetched free page [%d]", page->no);
    *pNo = page->no;
    
    while (ndata > area_size)
    {
        pData = page->data;
        sakhadb_pager_save_page(dbdata->pager, page);
        rc = sakhadb_pager_request_free_page(dbdata->pager, &page);
        if(rc)
        {
            SLOG_DBDATA_ERROR("sakhadb_dbdata_write: failed to fetch free page [%d]", rc);
            goto Lexit;
        }
        
        SLOG_DBDATA_INFO("sakhadb_dbdata_write: fetched free page [%d]", page->no);
        
        *pData++ = page->no;
        
        memcpy(pData, inData, area_size);
        ndata -= area_size;
        inData += area_size;
    }
    
    pData = page->data;
    *pData++ = 0;
    memcpy(pData, inData, ndata);
    
    sakhadb_pager_save_page(dbdata->pager, page);
    
Lexit:
    return rc;
}

int sakhadb_dbdata_read(sakhadb_dbdata_t dbdata, Pgno no, cpl_region_ref reg)
{
    int rc = SAKHADB_OK;
    size_t page_size = sakhadb_pager_page_size(dbdata->pager, 0);
    
    do
    {
        sakhadb_page_t page;
        rc = sakhadb_pager_request_page(dbdata->pager, no, &page);
        if(rc) goto Lexit;
        Pgno* pNo = (Pgno*)page->data;
        cpl_region_append_data(reg, pNo + 1, page_size - sizeof(Pgno));
        
        no = *pNo;
    } while(no);
    
Lexit:
    return rc;
}
/******************************************************************************/