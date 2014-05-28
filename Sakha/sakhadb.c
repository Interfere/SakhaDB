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
#include "dbdata.h"

struct sakhadb
{
    sakhadb_file_t      h;          /* The file handle */
    sakhadb_pager_t     pager;      /* Pager */
    sakhadb_btree_ctx_t ctx;        /* B-tree environment */
    sakhadb_dbdata_t    dbdata;     /* DbData */
};

struct sakhadb_collection
{
    sakhadb_btree_t     tree;
    sakhadb*            db;
};

static int collectionCreate(sakhadb* db, const char* name, size_t length, struct sakhadb_collection** ppColl)
{
    int rc = SAKHADB_OK;
    sakhadb_btree_cursor_t cursor = 0;
    sakhadb_pager_t pager = db->pager;
    sakhadb_btree_ctx_t ctx = db->ctx;
    sakhadb_btree_t meta = 0;
    
    struct sakhadb_collection* coll = cpl_allocator_allocate(cpl_allocator_get_default(), sizeof(struct sakhadb_collection));
    if(!coll)
    {
        rc = SAKHADB_NOMEM;
        goto Lexit;
    }
    
    rc = sakhadb_btree_create(ctx, 1, &meta);
    if(rc)
    {
        goto Lfail;
    }
    
    rc = sakhadb_btree_cursor_create(&cursor);
    if(rc)
    {
        goto Lfail;
    }
    
    int cmp = sakhadb_btree_find(meta, name, length, cursor);
    Pgno no;
    if(cmp != 0) // Add new collection to Database
    {
        sakhadb_page_t page;
        rc = sakhadb_pager_request_free_page(pager, &page);
        if(rc)
        {
            goto Lfail;
        }
        
        rc = sakhadb_btree_cursor_insert(cursor, name, length, page->no);
        if(rc)
        {
            goto Lfail;
        }
        
        sakhadb_btree_init_new_root(ctx, page);
        no = page->no;
    }
    else
    {
        no = sakhadb_btree_cursor_pgno(cursor);
    }
    
    rc = sakhadb_btree_create(ctx, no, &coll->tree);
    coll->db = db;
    
    *ppColl = coll;
    
    goto Lexit;
    
Lfail:
    cpl_allocator_free(cpl_allocator_get_default(), coll);
    
Lexit:
    sakhadb_btree_cursor_destroy(cursor);
    sakhadb_btree_destroy(meta);
    return rc;
}

static inline void collectionDestroy(sakhadb_collection* coll)
{
    sakhadb_btree_destroy(coll->tree);
    cpl_allocator_free(cpl_allocator_get_default(), coll);
}


/******************* Public API routines  ********************/


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
    
    rc = sakhadb_pager_create(db->h, &db->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to create pager [code:%d]", rc);
        goto create_pager_failed;
    }
    
    rc = sakhadb_btree_ctx_create(db->pager, &db->ctx);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to create Btree [code:%d]", rc);
        goto create_btree_failed;
    }
    
    rc = sakhadb_dbdata_create(db->pager, &db->dbdata);
    if(rc != SAKHADB_OK)
    {
        SLOG_FATAL("sakhadb_open: failed to create DbData [code:%d]", rc);
        goto create_dbdata_failed;
    }
    
    *ppDb = db;
    return rc;
    
create_dbdata_failed:
    sakhadb_btree_ctx_destroy(db->ctx);
    
create_btree_failed:
    sakhadb_pager_destroy(db->pager);
    
create_pager_failed:
    sakhadb_file_close(db->h);
    
file_open_failed:
    cpl_allocator_free(default_allocator, db);
    return rc;
}

int sakhadb_close(sakhadb* db)
{
    SLOG_INFO("sakhadb_close: closing database");
    
    sakhadb_dbdata_destroy(db->dbdata);
    sakhadb_btree_ctx_destroy(db->ctx);
    
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
    
    cpl_allocator_free(cpl_allocator_get_default(), db);
    return rc;
}

int sakhadb_collection_load(sakhadb *db, const char *name, sakhadb_collection **ppColl)
{
    size_t length = strlen(name);
    return collectionCreate(db, name, length, ppColl);
}

void sakhadb_collection_release(sakhadb_collection* coll)
{
    collectionDestroy(coll);
}

int sakhadb_collection_insert(sakhadb_collection* collection, bson_document_ref doc)
{
    int rc = SAKHADB_OK;
    bson_element_ref el = bson_document_get_first(doc);
    if(strcmp(bson_element_fieldname(el), "_id") != 0)
    {
        rc = SAKHADB_INVALID_ARG;
        goto Lexit;
    }
    
    const void* key = bson_element_value(el);
    size_t nkey = bson_element_value_size(el);
    
    Pgno no;
    rc = sakhadb_dbdata_write(collection->db->dbdata, doc->data, bson_document_size(doc), &no);
    if(rc)
    {
        goto Lexit;
    }
    
    rc = sakhadb_btree_insert(collection->tree, key, nkey, no);
    
Lexit:
    return rc;
}

int sakhadb_collection_foreach(const sakhadb_collection* collection, int(*pred)(bson_document_ref))
{
    int rc = SAKHADB_OK;
    sakhadb_btree_cursor_t cursor = 0;
    cpl_region_t region = {0, 0, 0};
    
    rc = sakhadb_btree_cursor_create(&cursor);
    if(rc)
    {
        goto Lexit;
    }
    
    rc = sakhadb_btree_cursor_begin(cursor, collection->tree);
    if(rc)
    {
        goto Lexit;
    }
    
    rc = cpl_region_init(&region, 64);
    if(rc)
    {
        goto Lexit;
    }
    
    do
    {
        // print info
        rc = sakhadb_dbdata_read(collection->db->dbdata, sakhadb_btree_cursor_pgno(cursor), &region);
        if(rc)
        {
            goto Lexit;
        }
        
        pred(bson_document_create_with_data(region.data));
        
        rc = sakhadb_btree_cursor_next(cursor);
        region.offset = 0;
    } while(rc == SAKHADB_OK);
    
Lexit:
    if(rc == SAKHADB_NOTFOUND)
    {
        rc = SAKHADB_OK;
    }
    
    cpl_region_deinit(&region);
    sakhadb_btree_cursor_destroy(cursor);
    return rc;
}
