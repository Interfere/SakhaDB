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
#include "os.h"

struct sakhadb
{
    sakhadb_file_t  h; /* The file handle */
};

int sakhadb_open(const char *filename, int flags, sakhadb **ppDb)
{
    int rc = SAKHADB_OK;
    sakhadb *db = (sakhadb*)malloc(sizeof(sakhadb));
    if(!db)
    {
        return SAKHADB_NOMEM;
    }
    memset(db, 0, sizeof(sakhadb));
    
    rc = sakhadb_file_open(filename, SAKHADB_OPEN_READWRITE | SAKHADB_OPEN_CREATE, &db->h);
    if(rc != SAKHADB_OK)
    {
        goto cleanup;
    }
    
    *ppDb = db;
    return rc;
    
cleanup:
    free(db);
    return rc;
}

int sakhadb_close(sakhadb* db)
{
    int rc = sakhadb_file_close(db->h);
    if(rc != SAKHADB_OK)
    {
        return rc;
    }
    
    free(db);
    return SAKHADB_OK;
}
