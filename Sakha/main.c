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
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>

/************************* Logger *****************************/

#define SAKHADB_LOG_BUF_SIZE    64

/**
 * Log levels
 */
#define SAKHADB_LOGLEVEL_NONE   0
#define SAKHADB_LOGLEVEL_FATAL  1
#define SAKHADB_LOGLEVEL_ERROR  2
#define SAKHADB_LOGLEVEL_WARN   3
#define SAKHADB_LOGLEVEL_INFO   4

/**
 * Level to str convertion
 */
static const char* getLevelStr(int iLevel)
{
    const char* pszLevel = 0;   /* String to return */
    switch(iLevel)
    {
        case SAKHADB_LOGLEVEL_FATAL:
            {
                static const char* const pszFatal = "FATAL";
                pszLevel = pszFatal;
            }
            break;

        case SAKHADB_LOGLEVEL_ERROR:
            {
                static const char* const pszError = "ERROR";
                pszLevel = pszError;
            }
            break;

        case SAKHADB_LOGLEVEL_WARN:
            {
                static const char* const pszWarn = "WARN";
                pszLevel = pszWarn;
            }
            break;

        case SAKHADB_LOGLEVEL_INFO:
            {
                static const char* const pszInfo = "INFO";
                pszLevel = pszInfo;
            }
            break;

        case SAKHADB_LOGLEVEL_NONE:
        default:
            assert(0);
    };

    return pszLevel;
}

/**
 * Helper routine
 */
static void logMessage(int iLevel, const char* pszFormat, va_list va)
{
    char pszMsg[SAKHADB_LOG_BUF_SIZE*3];
    int n = vsnprintf(pszMsg, sizeof(pszMsg), pszFormat, va);
    assert(n < sizeof(pszMsg));
    fprintf(stderr, "[%s] %s\n", getLevelStr(iLevel), pszMsg);
}

/**
 * Routine to log the message
 */
void sakhadb_log(int iLevel, const char* pszFormat, ...)
{
    va_list va;
    va_start(va, pszFormat);
    logMessage(iLevel, pszFormat, va);
    va_end(va);
}
/**************************************************************/

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
#define SAKHADB_NOMEM           2 /* A malloc() failed */

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
            break;
        }
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
    void** pFile                    /* The file descriptor to fill */
){
    assert(pszPath && pFile);       /* Check incoming pointers */
    posixFile* p = 0;               /* Pointer to internal file structure */
    int rc = SAKHADB_OK;            /* Function return code */
    int fd = -1;                    /* File descriptor returned by open() */
    int internalFlags = 0;          /* Flags for open() */
    int nPath = strlen(pszPath);

    int isReadOnly = (flags & SAKHADB_OPEN_READWRITE == SAKHADB_OPEN_READ);
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

    posixFile** pp = (posixFile **)pFile;
    *pp = p;
    
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
        robust_close(p->pszFilename, p->fd);
    }

    memset(p, 0, sizeof(posixFile));

    return SAKHADB_OK;
}

int main(int argc, const char * argv[])
{
    return 0;
}

