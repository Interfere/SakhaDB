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

#include <assert.h>

#include <string.h>

#include <cpl/cpl_allocator.h>
#include <bson/documentbuilder.h>
#include <bson/oid.h>
#include <bson/iterator.h>

#include <sys/mman.h>

#include "logger.h"
#include "sakhadb.h"
#include "os.h"
#include "btree.h"
#include "dbdata.h"
#include <bson/jsonparser.h>

int test_db()
{
    sakhadb* db = 0;
    int rc = sakhadb_open("test.db", 0, &db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    sakhadb_btree_ctx_t env = *(sakhadb_btree_ctx_t*)((char*)db + sizeof(sakhadb_file_t) + sizeof(sakhadb_pager_t));
    sakhadb_dbdata_t dbdata = *(sakhadb_dbdata_t*)((char*)db + sizeof(sakhadb_file_t) + sizeof(sakhadb_pager_t) + sizeof(sakhadb_btree_ctx_t));
    
    sakhadb_btree_t meta;
    sakhadb_btree_create(env, 1, &meta);
    
    char* key[] = {
        "indx_index_index_index_index_index_index.t1",
        "indx_index_index_index_index_index_index.t2",
        "indx_index_index_index_index_index_index.t3",
        "indx_index_index_index_index_index_index.t4",
        "indx_index_index_index_index_index_index.t5",
        "indx_index_index_index_index_index_index.t6",
        "indx_index_index_index_index_index_index.t7",
        "indx_index_index_index_index_index_index.t8",
        "indx_index_index_index_index_index_index.t9",
        "indx_index_index_index_index_index_index.t10",
        "indx_index_index_index_index_index_index.t11",
        "indx_index_index_index_index_index_index.t12",
        "indx_index_index_index_index_index_index.t13",
        "indx_index_index_index_index_index_index.t14",
        "indx_index_index_index_index_index_index.t15",
        "indx_index_index_index_index_index_index.t16",
        "indx_index_index_index_index_index_index.t17",
        "indx_index_index_index_index_index_index.t18",
        "indx_index_index_index_index_index_index.t19",
        "indx_index_index_index_index_index_index.t00",
        "indx_index_index_index_index_index_index.t01",
        "indx_index_index_index_index_index_index.t02",
        "indx_index_index_index_index_index_index.t03",
        "indx_index_index_index_index_index_index.t04",
        "indx_index_index_index_index_index_index.t05",
        "indx_index_index_index_index_index_index.t06",
        "indx_index_index_index_index_index_index.t07",
        "indx_index_index_index_index_index_index.t08",
        "indx_index_index_index_index_index_index.t09",
        "indx_index_index_index_index_index_index.t20",
        "indx_index_index_index_index_index_index.t21",
        "indx_index_index_index_index_index_index.t22",
        "indx_index_index_index_index_index_index.t23",
        "indx_index_index_index_index_index_index.t24",
        "indx_index_index_index_index_index_index.t25",
        "indx_index_index_index_index_index_index.t26",
        "indx_index_index_index_index_index_index.t27",
        "indx_index_index_index_index_index_index.t28",
        "indx_index_index_index_index_index_index.t29",
        "indx_index_index_index_index_index_index.t30",
        "indx_index_index_index_index_index_index.t31",
        "indx_index_index_index_index_index_index.t32",
        "indx_index_index_index_index_index_index.t33",
        "indx_index_index_index_index_index_index.t34",
        "indx_index_index_index_index_index_index.t35",
        "indx_index_index_index_index_index_index.t36",
        "indx_index_index_index_index_index_index.t37",
        "indx_index_index_index_index_index_index.t38",
        "indx_index_index_index_index_index_index.t39",
        "indx_index_index_index_index_index_index.t40",
        "indx_index_index_index_index_index_index.t41",
        "indx_index_index_index_index_index_index.t42",
        "indx_index_index_index_index_index_index.t43",
        "indx_index_index_index_index_index_index.t44",
        "indx_index_index_index_index_index_index.t45",
        "indx_index_index_index_index_index_index.t46",
        "indx_index_index_index_index_index_index.t47",
        "indx_index_index_index_index_index_index.t48",
        "indx_index_index_index_index_index_index.t49",
        "indx_index_index_index_index_index_index.t50",
        "indx_index_index_index_index_index_index.t51",
        "indx_index_index_index_index_index_index.t52",
        "indx_index_index_index_index_index_index.t53",
        "indx_index_index_index_index_index_index.t54",
        "indx_index_index_index_index_index_index.t55",
        "indx_index_index_index_index_index_index.t56",
        "indx_index_index_index_index_index_index.t57",
        "indx_index_index_index_index_index_index.t58",
        "indx_index_index_index_index_index_index.t59",
        "indx_index_index_index_index_index_index.t60",
        "indx_index_index_index_index_index_index.t61",
        "indx_index_index_index_index_index_index.t62",
        "indx_index_index_index_index_index_index.t63",
        "indx_index_index_index_index_index_index.t64",
        "indx_index_index_index_index_index_index.t65",
        "indx_index_index_index_index_index_index.t66",
        "indx_index_index_index_index_index_index.t67",
        "indx_index_index_index_index_index_index.t68",
        "indx_index_index_index_index_index_index.t69"
    };
    
    
    for (int32_t i = 0; i < sizeof(key)/sizeof(key[0]); ++i)
    {
        register int32_t len = (int32_t)strlen(key[i]);
        Pgno no;
        rc = sakhadb_dbdata_write(dbdata, key, len, &no);
        if(rc != SAKHADB_OK)
        {
            assert(0);
            return -1;
        }
        rc = sakhadb_btree_insert(meta, key[i], len, no);
        if(rc != SAKHADB_OK)
        {
            assert(0);
            return -1;
        }
    }
    
    sakhadb_btree_ctx_commit(env);
    
//    int idx = 25;
//    register int32_t len = (int32_t)strlen(key[idx]);
//    sakhadb_btree_cursor_t cursor = 0;
//    int cmp = 0;
//    cmp = sakhadb_btree_find(meta, key[idx], len, cursor);
//    Pgno no = sakhadb_btree_cursor_pgno(cursor);
//    sakhadb_btree_cursor_destroy(cursor);
//    
//    cpl_region_t reg;
//    cpl_region_init(&reg, 0);
//    sakhadb_dbdata_read(dbdata, no, &reg);
//    
//    void* d = reg.data;
//    
//    cpl_region_deinit(&reg);
    
    cpl_region_t reg;
    cpl_region_init(cpl_allocator_get_default(), &reg, 64);
    sakhadb_btree_dump(meta, &reg);
    
    cpl_region_append_data(&reg, "\0", 1);
    char* dumpstr = reg.data;
    puts(dumpstr);
    cpl_region_deinit(&reg);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    return 0;
}

bson_document_ref test_create_doc()
{
    struct bson_oid oid;
    bson_document_builder_ref builder = bson_document_builder_create();
    bson_oid_init(&oid);
    
    bson_document_builder_append_oid(builder, "_id", &oid);
    bson_document_builder_append_str(builder, "name", "komnin");
    bson_document_builder_append_i(builder, "age", 25);
    
    return bson_document_builder_finalize(builder);
}

int test_pred(bson_document_ref doc)
{
    bson_element_ref el = bson_document_get_first(doc);

    bson_oid_ref oid = bson_oid_create_with_bytes(bson_element_value(el));
    char* oidstr = bson_oid_string_create(oid);
    bson_oid_destroy(oid);
    SLOG_INFO("%s: %s\n", bson_element_fieldname(el), oidstr);
    free(oidstr);
    
    return 0;
}

int test_db2()
{
    sakhadb* db = 0;
    int rc = sakhadb_open("test.db", 0, &db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    char collname[] = "test_collection";
    
    sakhadb_collection* collection;
    rc = sakhadb_collection_load(db, collname, &collection);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    bson_document_ref doc = test_create_doc();
    
    rc = sakhadb_collection_insert(collection, doc);
    if(rc)
    {
        assert(0);
        return 1;
    }
    
    bson_document_destroy(doc);
    
    sakhadb_collection_release(collection);
    
    sakhadb_btree_ctx_t ctx = *(sakhadb_btree_ctx_t*)((char*)db + sizeof(sakhadb_file_t) + sizeof(sakhadb_pager_t));
    sakhadb_btree_ctx_commit(ctx);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    return 0;
}

int test_db3()
{
    sakhadb* db = 0;
    int rc = sakhadb_open("test.db", 0, &db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    char collname[] = "test_collection";
    
    sakhadb_collection* collection;
    rc = sakhadb_collection_load(db, collname, &collection);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    bson_oid_ref oid = bson_oid_create_with_string("53e8d553f7f8d8548a000001");
    
    sakhadb_cursor* cur;
    sakhadb_collection_find(collection, oid, &cur);
    
    bson_document_ref doc;
    sakhadb_cursor_data(db, cur, &doc);
    
    sakhadb_cursor_data(db, cur, &doc);
    
    sakhadb_cursor_destroy(cur);
    sakhadb_collection_release(collection);
    
    sakhadb_btree_ctx_t ctx = *(sakhadb_btree_ctx_t*)((char*)db + sizeof(sakhadb_file_t) + sizeof(sakhadb_pager_t));
    sakhadb_btree_ctx_commit(ctx);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    return 0;
}

int test_allocator()
{
    cpl_allocator_ref allocator = cpl_allocator_create_dl(0x1000000);
    
    void* ptr_a = cpl_allocator_allocate(allocator, 65480);
//    void* ptr_b = cpl_allocator_allocate(allocator, 32);
    ptr_a = cpl_allocator_realloc(allocator, ptr_a, 65504);
//    cpl_allocator_free(allocator, ptr_b);
    cpl_allocator_free(allocator, ptr_a);
    
//    void* ptr_c = cpl_allocator_allocate(allocator, 16);
//    ptr_c = cpl_allocator_realloc(allocator, ptr_c, 32);
//    cpl_allocator_free(allocator, ptr_c);
    
    cpl_allocator_destroy_dl(allocator);
    return 0;
}

int test_mmap()
{
    void* poolBuffer = 0;
    int i = 0;
    int size = (SAKHADB_MAX_DOCUMENT_SIZE + 0x1000) & ~(0xFFF);
    while (poolBuffer != MAP_FAILED) {
        poolBuffer = mmap(0, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
        SLOG_INFO("buffer %d: %p", ++i, poolBuffer);
    }
    return 0;
}

bson_document_ref create_test_doc()
{
    bson_document_builder_ref root = bson_document_builder_create();
    
    {
        bson_document_builder_ref b1 = bson_document_builder_create();
        
        {
            bson_array_builder_ref b2 = bson_array_builder_create();
            
            {
                bson_document_builder_ref b3 = bson_document_builder_create();
                
                bson_document_builder_append_str(b3, "type", "track");
                
                bson_oid_ref oid = bson_oid_create_with_string("53e8d553f7f8d8548a000001");
                bson_document_builder_append_oid(b3, "_id", oid);
                bson_oid_destroy(oid);
                
                bson_document_builder_append_i(b3, "image number", 123456789);

                bson_document_ref d3 = bson_document_builder_finalize(b3);
                bson_array_builder_append_doc(b2, d3);
                bson_document_destroy(d3);
            }
            
            bson_array_ref d2 = bson_array_builder_finalize(b2);
            bson_document_builder_append_arr(b1, "entries", d2);
            bson_array_destroy(d2);
        }
        
        bson_document_ref d = bson_document_builder_finalize(b1);
        bson_document_builder_append_doc(root, "result", d);
        bson_document_destroy(d);
    }
    
    {
        bson_document_builder_ref b1 = bson_document_builder_create();
        
        bson_document_builder_append_str(b1, "error", "ok");
        bson_document_builder_append_str(b1, "errorMessage", "");
        
        bson_document_ref d = bson_document_builder_finalize(b1);
        bson_document_builder_append_doc(root, "status", d);
        bson_document_destroy(d);
    }
    
    return bson_document_builder_finalize(root);
}

int test_json2bson()
{
    const char* tst = "  {"
    "\"result\": {"
    "\"entries\": [{"
    "\"type\": \"track\","
    "\"_id\": ObjectId(\"53e8d553f7f8d8548a000001\"),"
    "\"image number\": 123456789"
    "}]"
    "},"
    "\"status\": {"
    "\"error\": \"ok\","
    "\"errorMessage\": \"\""
    "}"
    "}";
    bson_document_ref d = json2bson(tst, strlen(tst));
    bson_document_ref d2 = create_test_doc();
    
    if(bson_document_size(d) != bson_document_size(d2))
    {
        return 1;
    }
    
    if(memcmp(d->data, d2->data, bson_document_size(d)))
    {
        return 1;
    }
    
    return 0;
}

int main(int argc, const char * argv[])
{
    return test_json2bson();
}

