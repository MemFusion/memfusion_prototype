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

#include "stdafx.h"

#include "retail_assert.h"
#include "MemFusion/Exceptions.h"

namespace MemFusion
{

std::exception MFException(const char * fmt, const char * file, int linenum, ...)
{
    const int MAX_FMT_SIZE = 256;
    char mybuffer[MAX_FMT_SIZE];
    char mybuffer2[MAX_FMT_SIZE];
    va_list argptr;
    va_start(argptr, fmt);
    vsprintf_s(mybuffer, MAX_FMT_SIZE, fmt, argptr);
    va_end(argptr);
    sprintf_s(mybuffer2, "%s:%d %s  %s", file, linenum, __FUNCTION__, mybuffer);
    return (std::move(std::exception(mybuffer2)));
}

void retail_assert_impl(bool condition, const char * message, const char * function, int linenum)
{
    if (!condition)
    {
        std::string fullmessage = std::string(message) + " at " + function + std::to_string(linenum);
        throw EXCEPTION(fullmessage.c_str());
    }
}



}
