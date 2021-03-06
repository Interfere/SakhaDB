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

#ifndef _SAKHADB_H_
#define _SAKHADB_H_

#include <bson/document.h>
#include <bson/oid.h>
#include <cpl/cpl_region.h>

/**
 * This is a magic string that appears at the beginning of every
 * Sakha database in order to identify the file as a real dsatabase.
 */
#ifndef SAKHADB_FILE_HEADER /* 123456789 123456 */
#  define SAKHADB_FILE_HEADER "SakhaDB ver 1"
#endif

/**
 * This is a version of SakhaDB.
 */
#ifndef SAKHADB_VERSION_NUMBER
#   define SAKHADB_VERSION_NUMBER 000002
#endif

/**
 * This is a limit restrictions for SakhaDB
 */
#ifndef SAKHADB_MAX_DOCUMENT_SIZE
#   define SAKHADB_MAX_DOCUMENT_SIZE 16711680
#endif

/**
 * Database connection handle.
 *
 * Each open Sakha database is represented by a pointer to an instance of the opaque
 * structure named "sakhadb". It is useful to think of an sakhadb pointer as an object.
 * The sakhadb_open() interfaces is its constructor, and sakhadb_close() is its destructor.
 */
typedef struct sakhadb sakhadb;

/**
 * Collection handle
 *
 * Each collection in Sakha database is accessed by a pointer to an instance of the opaque
 * structure named "sakhadb_collection".
 */
typedef struct sakhadb_collection sakhadb_collection;

/**
 * Cursor handle
 */
typedef struct sakhadb_cursor sakhadb_cursor;

/**
 * Predicate
 */
typedef struct sakhadb_pred sakhadb_pred;

/**
 * Opening a new database connection.
 */
int sakhadb_open(const char *filename, int flags, sakhadb **ppDb);

/**
 * Closing a database connection.
 */
int sakhadb_close(sakhadb* db);

/**
 * Loads collection.
 */
int sakhadb_collection_load(sakhadb *db, const char *name, sakhadb_collection **ppColl);

/**
 * Releases collection.
 */
void sakhadb_collection_release(sakhadb_collection* coll);

/**
 * Insert new item in collection.
 */
int sakhadb_collection_insert(sakhadb_collection* collection, bson_document_ref doc);

/**
 * Selects documents in a collection and returns a cursor to the selected documents.
 */
int sakhadb_collection_find(sakhadb_collection* collection, bson_oid_ref oid,
                            sakhadb_cursor **pCur);

int sakhadb_cursor_next(sakhadb_cursor *cur);
int sakhadb_cursor_data(sakhadb* db, sakhadb_cursor *cur, bson_document_ref* doc);
void sakhadb_cursor_destroy(sakhadb_cursor* cur);

/**
 * Results Codes
 *
 * Many SakhaDB functions return an integer result code from the set shown
 * here in order to indicate success or failure.
 */
#define SAKHADB_OK                  0 /* Successfull result */
#define SAKHADB_INVALID_ARG         1
#define SAKHADB_NOMEM               2 /* A malloc() failed */
#define SAKHADB_IOERR               3 /* Some kind of disk I/O error occured */
#define SAKHADB_IOERR_READ          4
#define SAKHADB_IOERR_SHORT_READ    5
#define SAKHADB_IOERR_WRITE         6
#define SAKHADB_IOERR_FSTAT         7
#define SAKHADB_FULL                8 /* Insertion failed because database is full */
#define SAKHADB_NOTAVAIL            9 /* Requesting page is not available */
#define SAKHADB_NOTADB             10 /* File is not a valid DB */
#define SAKHADB_NOTFOUND           11 /* Not found */
#define SAKHADB_CANTOPEN           12 /* Unable to open the DB file */


#endif // _SAKHADB_H_
