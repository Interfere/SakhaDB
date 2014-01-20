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
#include <cpl/cpl_allocator.h>
#include "os.h"
#include "btree.h"

struct sakhadb
{
    sakhadb_file_t  h;      /* The file handle */
    sakhadb_btree_t bt;     /* B-tree representation */
};

int sakhadb_open(const char *filename, int flags, sakhadb **ppDb)
{
    SLOG_INFO("sakhadb_open: opening database [%s]", filename);
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    sakhadb *db = (sakhadb*)cpl_allocator_allocate(default_allocator, sizeof(sakhadb));
    if(!db)
    {
        SLOG_FATAL("sakhadb_open: failed to allocate memory for struct sakhadb");
        return SAKHADB_NOMEM;
    }
    memset(db, 0, sizeof(sakhadb));
    
    int rc = sakhadb_file_open(filename, SAKHADB_OPEN_READWRITE | SAKHADB_OPEN_CREATE, &db->h);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to open file [code:%d][%s][flags:%d]", rc, filename, flags);
        goto file_open_failed;
    }
    
    rc = sakhadb_btree_create(db->h, &db->bt);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to create Btree [code:%d]", rc);
        goto create_btree_failed;
    }
    
    *ppDb = db;
    return rc;
    
create_btree_failed:
    sakhadb_file_close(db->h);
    
file_open_failed:
    cpl_allocator_free(default_allocator, db);
    return rc;
}

int sakhadb_close(sakhadb* db)
{
    SLOG_INFO("sakhadb_close: closing database");
    int rc = sakhadb_btree_destroy(db->bt);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_close: failed to destroy B-tree [%d]", rc);
    }
    
    rc = sakhadb_file_close(db->h);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_close: failed to close file [%d]", rc);
    }
    
    cpl_allocator_free(cpl_allocator_get_default(), db);
    return rc;
}
