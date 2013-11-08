//
//  main.c
//  Sakha
//
//  Created by Alex Komnin on 11/7/13.
//  Copyright (c) 2013 Alex Komnin. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

/**
 * This is a magic string that appears at the beginning of every 
 * Sakha database in order to identify the file as a real dsatabase.
 */
#ifndef SAKHA_FILE_HEADER /* 123456789 123456 */
#  define SAKHA_FILE_HEADER "SakhaDB format 1"
#endif

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
 * Error codes to return from functions
 */
#define SAKHADB_OK              0 /* Successfull result */
#define SAKHADB_CANTOPEN        1 /* Unable to open the DB file */

/**
 * Flags to open the file
 */
#define SAKHADB_OPEN_READ       0x1
#define SAKHADB_OPEN_WRITE      0x2
#define SAKHADB_OPEN_READWRITE  (SAKHADB_OPEN_READ|SAKHADB_OPEN_WRITE)
#define SAKHADB_OPEN_CREATE     0x4
#define SAKHADB_OPEN_EXCLUSIVE  0x8

/**
 * POSIX-related structure to store file-related info
 */
typedef struct posixFile posixFile;
struct posixFile
{
    int fd;                         /* The file descriptor */
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
            break;
        }
    }

    return fd;
}

/**
 * close() the file
 */
static void robust_close(int h)
{
    if(close(h))
    {
        // TODO: log the failure
    }
}

/**
 * Opens file POSIX-style
 */
static int posixOpen(
    const char* pszPath,            /* Pathname of file to open */
    void* pFile,                    /* The file descriptor to fill */
    int flags                       /* Input flags to control the opening */
){
    posixFile* p = (posixFile *)pFile;
    int rc = SAKHADB_OK;            /* Function return code */
    int fd = -1;                    /* File descriptor returned by open() */
    int internalFlags = 0;          /* Flags for open() */

    int isReadOnly = (flags & SAKHADB_OPEN_READWRITE == SAKHADB_OPEN_READ);
    int isCreate = (flags & SAKHADB_OPEN_CREATE);
    int isExclusive = (flags & SAKHADB_OPEN_EXCLUSIVE);
    
    memset(p, 0, sizeof(posixFile));
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
            rc = SAKHADB_CANTOPEN;
            goto open_finished;
        }
    }

    p->fd = fd;
    
open_finished:
    return rc;
}

/**
 * Close file
 */
static int posixClose(
        void* pFile             /* The file descriptor */
)
{
    posixFile* p = (posixFile *)pFile;
    if(p->fd > 0)
    {
        robust_close(p->fd);
    }

    memset(p, 0, sizeof(posixFile));

    return SAKHADB_OK;
}

int main(int argc, const char * argv[])
{

    // insert code here...
    printf("Hello, World!\n");
    return 0;
}

