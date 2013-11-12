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

/**
 * This is a magic string that appears at the beginning of every
 * Sakha database in order to identify the file as a real dsatabase.
 */
#ifndef SAKHADB_FILE_HEADER /* 123456789 123456 */
#  define SAKHADB_FILE_HEADER "SakhaDB format 1"
#endif

/**
 * Results Codes
 *
 * Many SakhaDB functions return an integer result code from the set shown
 * here in order to indicate success or failure.
 */
#define SAKHADB_OK                  0 /* Successfull result */
#define SAKHADB_CANTOPEN            1 /* Unable to open the DB file */
#define SAKHADB_NOMEM               2 /* A malloc() failed */
#define SAKHADB_IOERR               3 /* Some kind of disk I/O error occured */
#define SAKHADB_IOERR_READ          4
#define SAKHADB_IOERR_SHORT_READ    5
#define SAKHADB_IOERR_WRITE         6
#define SAKHADB_FULL                7 /* Insertion failed because database is full */


#endif // _SAKHADB_H_
