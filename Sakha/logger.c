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

#include "logger.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>

#define SAKHADB_LOG_BUF_SIZE    64

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
 * Routine to form and print logger message
 */
static void logMessage(int iLevel, const char* pszFormat, va_list va)
{
    char pszMsg[SAKHADB_LOG_BUF_SIZE*3];
    int n = vsnprintf(pszMsg, sizeof(pszMsg), pszFormat, va);
    assert(n < sizeof(pszMsg));
    struct timeval tm;
    gettimeofday(&tm, 0);
    fprintf(stderr, "[%s][%ld.%d] %s\n", getLevelStr(iLevel), tm.tv_sec, tm.tv_usec, pszMsg);
}

void sakhadb_log(int iLevel, const char* pszFormat, ...)
{
    va_list va;
    va_start(va, pszFormat);
    logMessage(iLevel, pszFormat, va);
    va_end(va);
}

