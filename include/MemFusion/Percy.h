//
// Copyright (c) 2015  Benedetto Proietti
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

#include <string>
#include <atomic>
#include <map>

#include "types.h"
#include "Platform/FileSystem.h"

namespace MemFusion
{

class PercyTraits
{
public:
    enum PersistencyType : uint32
    {
        LocalFileSystem = 1,
        CloudFileSystem = 2,
    };

    template <typename T>
    static T * Instantiate(PersistencyType ptype, Path path)
    {
        T * ret = nullptr;

        switch (ptype)
        {
        case PersistencyType::LocalFileSystem:
            ret = new T(path);
            break;
        case PersistencyType::CloudFileSystem:
            throw std::exception("Percy CloudFileSystem not implemented.");
            break;
        default:
            break;
        }
        return (ret);
    }

};


struct CollectionPercyCfg
{
    Path basePath;
    PercyTraits::PersistencyType ptype;

    explicit CollectionPercyCfg(Path base, PercyTraits::PersistencyType pt)
        : basePath(base),
        ptype(pt)
    {
    }
private:
    CollectionPercyCfg();
};

typedef const CollectionPercyCfg cCollectionPercyCfg;

class Percy
{
public:
    typedef uint64     HandleUint32;
    typedef uint64     HandleUint64;
    typedef uint64     Handle;
    typedef uint64     FilePosition;

protected:
    static Handle CreateHandle();

private:
    static std::atomic<uint64>    s_handleIdx;

    virtual uint64 GetCurrentSize() = 0;

    virtual void PersistUint32(uint32 value) = 0;
    virtual void PersistUint64(uint64 value) = 0;
    virtual HandleUint32 PersistPromiseUint32() = 0;
    virtual HandleUint64 PersistPromiseUint64() = 0;
    virtual void FullfillPromiseUint32(HandleUint32 handle, uint32 value) = 0;
    virtual void FullfillPromiseUint64(HandleUint64 handle, uint64 value) = 0;
    virtual void PersistBlob(const void * start, uint64 size) = 0;
    virtual void Flush() = 0;
};

class Depercy
{
public:
    virtual uint64 DeserializeUint64() = 0;
    virtual uint32 DeserializeUint32() = 0;
    virtual void DeserializeBlob(void * destBuf, uint64 size) = 0;

    virtual uint64 GetCurrentSize() = 0;
};

class PercyFS : public Percy
{
    PercyFS();
    Path     m_path;
    FILE *   m_file;
    uint64   m_writtenBytes;

    std::map<HandleUint32, FilePosition> m_handles32bit;
    std::map<HandleUint64, FilePosition> m_handles64bit;
public:
    explicit PercyFS(Path path);
    ~PercyFS();

    void PersistUint32(uint32 value);
    void PersistUint64(uint64 value);
    HandleUint32 PersistPromiseUint32();
    HandleUint64 PersistPromiseUint64();
    void FullfillPromiseUint32(HandleUint32 handle, uint32 value);
    void FullfillPromiseUint64(HandleUint64 handle, uint64 value);

    void PersistBlob(const void * start, uint64 size);

    void Flush();

    uint64 GetCurrentSize();
};

class DepercyFS : public Depercy
{
    DepercyFS();
    Path     m_path;
    FILE *   m_file;
    uint64   m_readBytes;

public:
    explicit DepercyFS(Path path);
    ~DepercyFS();

    uint64 DeserializeUint64();
    uint32 DeserializeUint32();
    void DeserializeBlob(void * destBuf, uint64 size);
    uint64 GetCurrentSize();
};

}
