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

#ifndef _SAKHADB_LOGGER_H_
#define _SAKHADB_LOGGER_H_

/**
 * Log levels
 */
#define SAKHADB_LOGLEVEL_NONE   0
#define SAKHADB_LOGLEVEL_FATAL  1
#define SAKHADB_LOGLEVEL_ERROR  2
#define SAKHADB_LOGLEVEL_WARN   3
#define SAKHADB_LOGLEVEL_INFO   4

/**
 * Some useful macroses for different logging levels
 */
#ifdef DEBUG
#   define SLOG_FATAL(pszFormat, ...) sakhadb_log(SAKHADB_LOGLEVEL_FATAL, pszFormat, ##__VA_ARGS__)
#   define SLOG_ERROR(pszFormat, ...) sakhadb_log(SAKHADB_LOGLEVEL_ERROR, pszFormat, ##__VA_ARGS__)
#   define SLOG_WARN(pszFormat, ...) sakhadb_log(SAKHADB_LOGLEVEL_WARN, pszFormat, ##__VA_ARGS__)
#   define SLOG_INFO(pszFormat, ...) sakhadb_log(SAKHADB_LOGLEVEL_INFO, pszFormat, ##__VA_ARGS__)
#else
#   define SLOG_FATAL(pszFormat, ...)
#   define SLOG_ERROR(pszFormat, ...)
#   define SLOG_WARN(pszFormat, ...)
#   define SLOG_INFO(pszFormat, ...)
#endif

/**
 * Format and write a message to the log if logging is enabled
 */
void sakhadb_log(int iLevel, const char* pszFormat, ...);

#endif //_SAKHADB_LOGGER_H_
