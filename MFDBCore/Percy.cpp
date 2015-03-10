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

#include <sstream>
#include "MemFusion/Percy.h"

namespace MemFusion
{

std::atomic<uint64>    Percy::s_handleIdx(1000);

Percy::Handle Percy::CreateHandle()
{
    Handle handle = s_handleIdx++;
    return (handle);
}


#pragma region PercyFS

PercyFS::PercyFS(Path path)
    : m_path(path),
    m_writtenBytes(0ULL),
    m_file(nullptr)
{
    if (0 != fopen_s(&m_file, std::string(m_path).c_str(), "rb+"))
    {
        std::stringstream ss;
        ss << "File " << std::string(m_path) << " does not exist in PercyFS ctor";
        throw (std::exception(ss.str().c_str()));
    }
}

PercyFS::~PercyFS()
{
    if (m_file)
    {
        fclose(m_file);
    }
}


void PercyFS::PersistUint32(uint32 value)
{
    fwrite(&value, sizeof(value), 1, m_file);
    m_writtenBytes += sizeof(value);
}

void PercyFS::PersistUint64(uint64 value)
{
    fwrite(&value, sizeof(value), 1, m_file);
    m_writtenBytes += sizeof(value);
}

Percy::HandleUint32 PercyFS::PersistPromiseUint32()
{
    uint64 curpos = _ftelli64(m_file);
    uint32 value = 0;
    fwrite(&value, sizeof(value), 1, m_file);
    m_writtenBytes += sizeof(value);
    Handle handle = CreateHandle();
    m_handles32bit.insert(std::make_pair(handle, curpos));

    return static_cast<HandleUint32>(handle);
}

Percy::HandleUint64 PercyFS::PersistPromiseUint64()
{
    uint64 curpos = _ftelli64(m_file);
    uint64 value = 0;
    fwrite(&value, sizeof(value), 1, m_file);
    m_writtenBytes += sizeof(value);
    Handle handle = CreateHandle();
    m_handles64bit.insert(std::make_pair(handle, curpos));

    return static_cast<HandleUint64>(handle);
}

void PercyFS::FullfillPromiseUint32(HandleUint32 handle, uint32 value)
{
    auto iter = m_handles32bit.find(handle);
    if (iter != m_handles32bit.cend())
    {
        FilePosition pos = iter->second;
        FilePosition curpos = _ftelli64(m_file);
        _fseeki64(m_file, pos, SEEK_SET);
        fwrite(&value, sizeof(value), 1, m_file);
        _fseeki64(m_file, curpos, SEEK_SET);
        m_handles32bit.erase(iter);
        return;
    }
    throw std::exception("handle not recognized in PercyFS");
}

void PercyFS::FullfillPromiseUint64(HandleUint64 handle, uint64 value)
{
    auto iter = m_handles64bit.find(handle);
    if (iter != m_handles64bit.cend())
    {
        FilePosition pos = iter->second;
        FilePosition curpos = _ftelli64(m_file);
        _fseeki64(m_file, pos, SEEK_SET);
        fwrite(&value, sizeof(value), 1, m_file);
        _fseeki64(m_file, curpos, SEEK_SET);
        m_handles64bit.erase(iter);
        return;
    }
    throw std::exception("handle not recognized in PercyFS");
}

void PercyFS::PersistBlob(const void * start, uint64 size)
{
    uint64 bytesWritten = fwrite(start, 1, size, m_file);
    if (bytesWritten != size)
    {
        throw(std::exception("Written wrong number of bytes in PercyFS::PersistBlob()."));
    }
    m_writtenBytes += size;
}

void PercyFS::Flush()
{
    fflush(m_file);
}

uint64 PercyFS::GetCurrentSize()
{
    uint64 curpos = _ftelli64(m_file);
    if (curpos != m_writtenBytes)
    {
        throw std::exception("Error in file position in PercyFS");
    }

    return (curpos);
}


#pragma endregion


#pragma region DepercyFS

DepercyFS::DepercyFS(Path path)
    : m_path(path),
    m_readBytes(0ULL)
{
    if (0 != fopen_s(&m_file, std::string(m_path).c_str(), "rb+"))
    {
        std::stringstream ss;
        ss << "File " << std::string(m_path) << " does not exist in DepercyFS ctor";
        throw (std::exception(ss.str().c_str()));
    }
}

DepercyFS::~DepercyFS()
{
    if (m_file)
    {
        fclose(m_file);
    }
}

uint64 DepercyFS::GetCurrentSize()
{
    uint64 curpos = _ftelli64(m_file);
    if (curpos != m_readBytes)
    {
        throw std::exception("Error in file position in PercyFS");
    }

    return (curpos);
}

uint64 DepercyFS::DeserializeUint64()
{
    uint64 value = 0ULL;
    uint64 bytesread = fread_s(&value, sizeof(value), 1, sizeof(value), m_file);
    if (bytesread != sizeof(value))
        throw(std::exception("Read wrong number of bytes in DepercyFS::DeserializeUint64()."));
    m_readBytes += sizeof(value);
    return (value);
}

uint32 DepercyFS::DeserializeUint32()
{
    uint32 value = 0UL;
    uint64 bytesread = fread_s(&value, sizeof(value), 1, sizeof(value), m_file);
    if (bytesread != sizeof(value))
        throw(std::exception("Read wrong number of bytes in DepercyFS::DeserializeUint32()."));
    m_readBytes += sizeof(value);
    return (value);
}

void DepercyFS::DeserializeBlob(void * destBuf, uint64 size)
{
    uint64 bytesread = fread_s(destBuf, size, 1, size, m_file);
    if (bytesread != size)
        throw(std::exception("Read wrong number of bytes in DepercyFS::DeserializeBlob()."));
    m_readBytes += size;
}

#pragma endregion


}


