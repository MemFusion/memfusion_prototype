//
// Copyright (c) 2014-2015 Benedetto Proietti
//
//
//  This program is free software: you can redistribute it and/or  modify
//  it under the terms of the GNU Affero General Public License, version 3,
//  as published by the Free Software Foundation.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
//  As a special exception, the copyright holders give permission to link the
//  code of portions of this program with the OpenSSL library under certain
//  conditions as described in each individual source file and distribute
//  linked combinations including the program with the OpenSSL library. You
//  must comply with the GNU Affero General Public License in all respects for
//  all of the code used other than as permitted herein. If you modify file(s)
//  with this exception, you may extend this exception to your version of the
//  file(s), but you are not obligated to do so. If you do not wish to do so,
//  delete this exception statement from your version. If you delete this
//  exception statement from all source files in the program, then also delete
//  it in the license file.

#pragma once

#ifdef WIN32

#include <windows.h>

const DWORD MS_VC_EXCEPTION = 0x406D1388;

#pragma pack(push,8)
typedef struct tagTHREADNAME_INFO
{
    DWORD dwType; // Must be 0x1000.
    LPCSTR szName; // Pointer to name (in user addr space).
    DWORD dwThreadID; // Thread ID (-1=caller thread).
    DWORD dwFlags; // Reserved for future use, must be zero.
} THREADNAME_INFO;
#pragma pack(pop)

#include "Inline.h"

namespace MemFusion
{
struct Utils
{
    INLINE static void SetThreadName(const char * threadName)
    {
        (void) threadName;
#ifndef NDEBUG
        THREADNAME_INFO info;
        info.dwType = 0x1000;
        info.szName = threadName;
        info.dwThreadID = GetCurrentThreadId();
        info.dwFlags = 0;

        __try
        {
            RaiseException(MS_VC_EXCEPTION, 0, sizeof(info) / sizeof(ULONG_PTR), (ULONG_PTR*) &info);
        }
        __except (EXCEPTION_EXECUTE_HANDLER)
        {
        }
#endif
    }
};
}

#else

namespace MemFusion
{
struct Utils
{
    INLINE static void SetThreadName(std::string)
    {
    }
}
}
#endif

#ifndef NDEBUG
#define DEBUG_ONLY_SET_THREAD_NAME(THE_NAME)            \
Utils::SetThreadName(THE_NAME);

#define DEBUG_ONLY_SET_THREAD_NAME_WITH_INDEX(THE_NAME,THE_INDEX)  \
{   \
std::string fullname = std::string(THE_NAME) + std::to_string(THE_INDEX); \
Utils::SetThreadName(fullname.c_str()); \
}

#else
#define DEBUG_ONLY_SET_THREAD_NAME(THE_NAME)
#define DEBUG_ONLY_SET_THREAD_NAME_WITH_INDEX(THE_NAME,THE_INDEX)
#endif

