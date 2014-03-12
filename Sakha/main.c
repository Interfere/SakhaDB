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

#include "sakhadb.h"
#include "os.h"
#include "btree.h"

int main(int argc, const char * argv[])
{
    sakhadb* db = 0;
    int rc = sakhadb_open("test.db", 0, &db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    sakhadb_btree_ctx_t env = *(sakhadb_btree_ctx_t*)((char*)db + sizeof(sakhadb_file_t));

    sakhadb_btree_t meta = sakhadb_btree_ctx_get_meta(env);
    
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
        rc = sakhadb_btree_insert(meta, key[i], len, key[i], len);
        if(rc != SAKHADB_OK)
        {
            assert(0);
            return -1;
        }
    }
    
    register int32_t len = (int32_t)strlen(key[0]);
    sakhadb_btree_cursor_t cursor = sakhadb_btree_find(meta, key[0], len);
    sakhadb_btree_cursor_destroy(cursor);
    
    sakhadb_btree_ctx_commit(env);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    return 0;
}

