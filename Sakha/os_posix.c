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
#include <sys/mman.h>

#include "logger.h"
#include "sakhadb.h"
#include "os.h"
#include "allocator.h"

/**
 * Turn on/off logging for file routines
 */
#define SLOG_OS_ENABLE    1

#if SLOG_OS_ENABLE
#   define SLOG_OS_INFO  SLOG_INFO
# define SLOG_OS_WARN  SLOG_WARN
#   define SLOG_OS_ERROR SLOG_ERROR
#   define SLOG_OS_FATAL SLOG_FATAL
#else // SLOG_OS_ENABLE
#   define SLOG_OS_INFO(...)
#   define SLOG_OS_WARN(...)
#   define SLOG_OS_ERROR(...)
#   define SLOG_OS_FATAL(...)
#endif // SLOG_OS_ENABLE

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

#ifndef O_NOFOLLOW
#  define O_NOFOLLOW 0
#endif

/**
 * POSIX-related structure to store file-related info
 */
typedef struct posixFile posixFile;
struct posixFile
{
    sakhadb_allocator_t allocator;  /* Allocator to use */
    int fd;                         /* The file descriptor */
    char pszFilename[1];            /* The file name */
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
            SLOG_OS_FATAL("posixOpen: failed to open file [%s][error:%s]", pszPath, strerror(errno));
            rc = SAKHADB_CANTOPEN;
            goto open_finished;
        }
    }
    
    sakhadb_allocator_t default_allocator = sakhadb_allocator_get_default();
    p = sakhadb_allocator_allocate(default_allocator, sizeof(posixFile) + nPath);
    if(p == 0)
    {
        SLOG_OS_FATAL("posixOpen: Failed to allocate memory");
        rc = SAKHADB_NOMEM;
        goto open_finished;
    }
    
    memset(p, 0, sizeof(posixFile));
    
    p->allocator = default_allocator;
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
        robust_close(p->pszFilename, p->fd);
    
    if(p)
        sakhadb_allocator_free(p->allocator, p);

    return SAKHADB_OK;
}

/**
 * Seek to the offset in p then read amt bytes from p into pBuf.
 * Return the number of bytes actually read. Updates the offset.
 */
static long seekAndRead(posixFile* p, void* pBuf, int amt, int64_t offset)
{
    int64_t newOffset = lseek(p->fd, offset, SEEK_SET);
    if(newOffset != offset)
    {
        return -1;
    }

    long got = read(p->fd, pBuf, amt);
    
    SLOG_OS_INFO("READ    %-3d %5ld %7lld", p->fd, got, offset);
    
    return got;
}

/**
 * Seek to the offset in p then write buffer starting at pBuf and amt bytes length.
 * Return the number of bytes actually written. Updates the offset.
 */
static long seekAndWrite(posixFile* p, const void* pBuf, int amt, int64_t offset)
{
    int64_t newOffset = lseek(p->fd, offset, SEEK_SET);
    if(newOffset != offset)
    {
        return -1;
    }
    
    long written = write(p->fd, pBuf, amt);
    
    SLOG_OS_INFO("WRITE   %-3d %5ld %7lld", p->fd, written, offset);
    return written;
}

/**
 * Read data from a file into a buffer. Return SAKHADB_OK if all bytes
 * were read succesfully and SAKHADB_IOERR if anything went wrong.
 */
static int posixRead(
    posixFile* p,                   /* The file descriptor */
    void* pBuf,                     /* Buffer receiever */
    int amt,                        /* Amount of butes to read */
    int64_t offset
)
{
    long got = seekAndRead(p, pBuf, amt, offset);
    if(got == amt)
    {
        return SAKHADB_OK;
    }
    else if(got < 0)
    {
        return SAKHADB_IOERR_READ;
    }
    else
    {
        memset(got + (char *)pBuf, 0, amt-got);
        return SAKHADB_IOERR_SHORT_READ;
    }
}

/**
 * Writes data from a buffer into a file. Returns SAKHADB_OK 
 * on success or some other error code on failure
 */
static int posixWrite(
    posixFile* p,                   /* The file descriptor */
    const void* pBuf,               /* Data buffer */
    int amt,                        /* Length of data */
    int64_t offset
)
{
    long written = 0;
    while(amt > 0 && (written = seekAndWrite(p, pBuf, amt, offset)) > 0)
    {
        amt -= written;
        offset += written;
        pBuf = written + (char*)pBuf;
    }

    if(amt > 0)
    {
        if(written < 0)
        {
            return SAKHADB_IOERR_WRITE;
        }
        return SAKHADB_FULL;
    }


    return SAKHADB_OK;
}

/**
 * Determine the current size of a file in bytes.
 */
static int posixFileSize(
    posixFile* p,                   /* The file descriptor */
    int64_t* pSize
)
{
    assert(p);
    assert(pSize);
    
    struct stat buf;
    int rc = fstat(p->fd, &buf);
    if(rc)
    {
        SLOG_OS_ERROR("posixFileSize: 'fstat' failed [code: %d][%s]", errno, strerror(errno));
        return SAKHADB_IOERR_FSTAT;
    }
    *pSize = buf.st_size;
    
    return SAKHADB_OK;
}


/******************* Public API routines  ********************/
int sakhadb_file_open(const char* pszFilename,
                      int flags,
                      sakhadb_file_t* ppFile)
{
    SLOG_OS_INFO("sakhadb_file_open: opening file [%s]", pszFilename);
    return posixOpen(pszFilename, flags, (posixFile**)ppFile);
}

int sakhadb_file_close(sakhadb_file_t fd)
{
    SLOG_OS_INFO("sakhadb_file_close: closing file [%s]", sakhadb_file_filename(fd));
    return posixClose((posixFile*)fd);
}

int sakhadb_file_read(sakhadb_file_t fd, void* pBuf, int amt, int64_t offset)
{
    SLOG_OS_INFO("sakhadb_file_read: reading from file [%s][len: %d][off: %lld]",
              sakhadb_file_filename(fd), amt, offset);
    return posixRead((posixFile*)fd, pBuf, amt, offset);
}

int sakhadb_file_write(sakhadb_file_t fd, const void* pBuf, int amt, int64_t offset)
{
    SLOG_OS_INFO("sakhadb_file_write: writing to file [%s][len: %d][off: %lld]",
              sakhadb_file_filename(fd), amt, offset);
    return posixWrite((posixFile*)fd, pBuf, amt, offset);
}

int sakhadb_file_size(sakhadb_file_t fd, int64_t* pSize)
{
    SLOG_OS_INFO("sakhadb_file_size: [%s]", sakhadb_file_filename(fd));
    return posixFileSize((posixFile *)fd, pSize);
}

const char* sakhadb_file_filename(sakhadb_file_t fd)
{
    return ((posixFile *)fd)->pszFilename;
}

