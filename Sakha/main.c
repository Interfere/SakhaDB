//
//  main.c
//  Sakha
//
//  Created by Alex Ushakov on 11/7/13.
//  Copyright (c) 2013 Alex Ushakov. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/**
 * This is a magic string that appears at the beginning of every 
 * Sakha database in order to identify the file as a real dsatabase.
 */
#ifndef SAKHA_FILE_HEADER /* 123456789 123456 */
#  define SAKHA_FILE_HEADER "SakhaDB format 1"
#endif

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

/**
 * POSIX-related structure to store file-related info
 */
typedef struct unixFile unixFile;
struct unixFile
{
    int fd;                         /* The file descriptor */
};

/**
 * Opens file POSIX-style
 */
static int posixOpen(
    const char* pszPath,            /* Pathname of file to open */
    void* pFile,                    /* The file descriptor to fill */
    int flags                       /* Input flags to control the opening */
){
    unixFile* p = (unixFile *)pFile;
    int rc = SAKHADB_OK;            /* Function return code */
    int fd = -1;                    /* File descriptor returned by open() */
    int internalFlags = 0;          /* Flags for open() */
    
    memset(p, 0, sizeof(unixFile));
    internalFlags |= (O_CREAT | O_BINARY);
    
    return rc;
}

int main(int argc, const char * argv[])
{

    // insert code here...
    printf("Hello, World!\n");
    return 0;
}

