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

#include "os.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "logger.h"
#include "sakhadb.h"

/**
 * Default open mode for SakhaDB
 */
#define SAKHADB_DEFAULT_FILE_PERMISSIONS    (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH)

/**
 * Define various macros that are missing from some systems
 */
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif

#ifndef O_BINARY
#  define O_BINARY 0
#endif

/**
 * POSIX-related structure to store file-related info
 */
typedef struct posixFile posixFile;
struct posixFile
{
    int fd;                         /* The file descriptor */
    char pszFilename[1];
};

/**
 * Invoke open().  Do so multiple times, until it either succeeds or
 * fails for some reason other than EINTR.
 */
static int robust_open(const char* psz, int f, mode_t m)
{
    int fd;
    mode_t m2 = m ? m : SAKHADB_DEFAULT_FILE_PERMISSIONS;
    
    while(1)
    {
        fd = open(psz, f, m2);
        if(fd < 0)
        {
            if(errno == EINTR) continue;
        }
        break;
    }
    
    return fd;
}

/**
 * close() the file
 */
static void robust_close(const char* psz, int h)
{
    assert(psz);
    if(close(h))
    {
        sakhadb_log(SAKHADB_LOGLEVEL_WARN, "robust_close: failed to close file [%s]", psz);
    }
}

/**
 * Opens file POSIX-style
 */
static int posixOpen(
    const char* pszPath,            /* Pathname of file to open */
    int flags,                      /* Input flags to control the opening */
    posixFile** pp                  /* The file descriptor to fill */
){
    assert(pszPath && pp);          /* Check incoming pointers */
    posixFile* p = 0;               /* Pointer to internal file structure */
    int rc = SAKHADB_OK;            /* Function return code */
    int fd = -1;                    /* File descriptor returned by open() */
    int internalFlags = 0;          /* Flags for open() */
    size_t nPath = strlen(pszPath);
    
    int isReadOnly = ((flags & SAKHADB_OPEN_READWRITE) == SAKHADB_OPEN_READ);
    int isCreate = (flags & SAKHADB_OPEN_CREATE);
    int isExclusive = (flags & SAKHADB_OPEN_EXCLUSIVE);
    
    if(isReadOnly)
        internalFlags |= O_RDONLY;
    else
        internalFlags |= O_RDWR;
    if(isCreate) internalFlags |= O_CREAT;
    if(isExclusive) internalFlags |= (O_EXCL|O_NOFOLLOW);
    internalFlags |= (O_LARGEFILE | O_BINARY);
    
    if(fd < 0)
    {
        fd = robust_open(pszPath, internalFlags, 0);
        if(fd < 0)
        {
            sakhadb_log(SAKHADB_LOGLEVEL_FATAL, "posixOpen: failed to open file [%s]", pszPath);
            rc = SAKHADB_CANTOPEN;
            goto open_finished;
        }
    }
    
    p = malloc(sizeof(posixFile) + nPath);
    if(p == 0)
    {
        sakhadb_log(SAKHADB_LOGLEVEL_FATAL, "posixOpen: Failed to allocate memory");
        rc = SAKHADB_NOMEM;
        goto open_finished;
    }
    
    memset(p, 0, sizeof(posixFile));
    
    p->fd = fd;
    memcpy(p->pszFilename, pszPath, nPath);
    p->pszFilename[nPath] = '\0';
    
    *pp = p;
    
open_finished:
    return rc;
}

/**
 * Close file
 */
static int posixClose(
    posixFile* p                    /* The file descriptor */
)
{
    assert(p);
    if(p->fd > 0)
    {
        robust_close(p->pszFilename, p->fd);
    }
    
    free(p);
    
    return SAKHADB_OK;
}

int sakhadb_open_file(const char* pszFilename, int flags, sakhadb_file_t* ppFile)
{
    // TODO: fix calculating absolute path
    return posixOpen(pszFilename, flags, (posixFile**)ppFile);
}

int sakhadb_close_file(sakhadb_file_t pFile)
{
    return posixClose((posixFile*)pFile);
}
