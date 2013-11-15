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

#include "sakhadb.h"

#include <stdlib.h>
#include <string.h>

#include "logger.h"
#include "os.h"
#include "paging.h"

struct sakhadb
{
    sakhadb_allocator_t allocator;
    sakhadb_file_t  h;      /* The file handle */
    sakhadb_pager_t pager;  /* The pager */
};

int sakhadb_open(const char *filename, int flags, sakhadb **ppDb)
{
    sakhadb_allocator_t default_allocator;
    int rc = sakhadb_allocator_get_default(sakhadb_default_allocator, &default_allocator);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to get default allocator.");
        return rc;
    }
    sakhadb *db = (sakhadb*)sakhadb_allocator_allocate(default_allocator, sizeof(sakhadb));
    if(!db)
    {
        SLOG_FATAL("sakhadb_open: failed to allocate memory for struct sakhadb");
        return SAKHADB_NOMEM;
    }
    memset(db, 0, sizeof(sakhadb));
    
    db->allocator = default_allocator;
    rc = sakhadb_file_open(default_allocator, filename, SAKHADB_OPEN_READWRITE | SAKHADB_OPEN_CREATE, &db->h);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to open file [code:%d][%s][flags:%d]", rc, filename, flags);
        goto file_open_failed;
    }
    
    rc = sakhadb_pager_create(default_allocator, db->h, &db->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to create pager [code:%d]", rc);
        goto create_pager_failed;
    }
    
    *ppDb = db;
    return rc;
    
create_pager_failed:
    sakhadb_file_close(db->h);
    
file_open_failed:
    sakhadb_allocator_free(default_allocator, db);
    return rc;
}

int sakhadb_close(sakhadb* db)
{
    int rc = sakhadb_pager_destroy(db->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_close: failed to destroy pager [%d]", rc);
    }
    
    rc = sakhadb_file_close(db->h);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_close: failed to close file [%d]", rc);
    }
    
    sakhadb_allocator_free(db->allocator, db);
    return rc;
}
