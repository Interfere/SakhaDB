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
    
    sakhadb_btree_env_t env = *(sakhadb_btree_env_t*)((char*)db + sizeof(sakhadb_file_t));

    sakhadb_btree_t meta = sakhadb_btree_env_get_meta(env);
    
    char key[] = "index.title";
    int a = 15;
//    rc = sakhadb_btree_insert(meta, key, sizeof(key) - 1, &a, sizeof(a));
//    if(rc != SAKHADB_OK)
//    {
//        assert(0);
//        return -1;
//    }
    
    sakhadb_btree_cursor_t cursor = sakhadb_btree_find(meta, key, sizeof(key) - 1);
    sakhadb_btree_cursor_destroy(cursor);
    
//    sakhadb_btree_env_commit(env);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        assert(0);
        return 1;
    }
    
    return 0;
}

