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
#include "paging.h"

int main(int argc, const char * argv[])
{
    sakhadb* db = 0;
    int rc = sakhadb_open("test.db", 0, &db);
    if(rc != SAKHADB_OK)
    {
        return 1;
    }
    
    void* pData;
    sakhadb_pager_get_page(*(sakhadb_pager_t*)((char*)db + sizeof(sakhadb_allocator_t) + sizeof(sakhadb_file_t)), 3, 0, &pData);
    
    void* pOtherData;
    sakhadb_pager_get_page(*(sakhadb_pager_t*)((char*)db + sizeof(sakhadb_allocator_t) + sizeof(sakhadb_file_t)), 3, 0, &pOtherData);
    
    rc = sakhadb_close(db);
    if(rc != SAKHADB_OK)
    {
        return 1;
    }
    
    return 0;
}

