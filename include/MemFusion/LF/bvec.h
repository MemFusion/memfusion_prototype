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

#include <stdexcept>
#include <atomic>
#include "MemFusion/Cache.h"
#include "MemFusion/Exceptions.h"

namespace MemFusion
{
namespace LF
{

#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

template <typename T>
class bvec
{
    void operator = (const bvec &);

protected:
    T * data;
    uint32 m_size;
    bool empty;
    CACHE_ALIGN std::atomic<uint32> s_idx;

public:
    static const int alignment_needed = CACHE_LINE;

    bvec(uint32 maxsize)
        : data(new T[maxsize]),
        m_size(maxsize),
        empty(true),
        s_idx(0)
    {
        memset(data, 0, maxsize * sizeof(T));
    }

    virtual ~bvec()
    {
        if (data)
            delete [] data;
    }

    bvec(const bvec & other)
        : data(other.data),
        empty(other.empty),
        s_idx(other.s_idx.load()),
        m_size(other.m_size)
    {
    }

    uint32 size() const { return s_idx.load(); }

    uint32 add(T val)
    {
        long lastIdx = s_idx.load();
        if (lastIdx+1 >= (long)m_size)
            throw EXCEPTION("bvec full exception. this 0x%x, maxsize %u", this, m_size);
        lastIdx = s_idx++;
        data[lastIdx] = val;
        empty = false;
        return (lastIdx);
    }

    T operator [] (uint idx) const
    {
        if (!empty && (idx < s_idx.load()))
        {
            return (data[idx]);
        }
        throw EXCEPTION("bvec out-of-bound exception. this 0x%x, maxsize %u, idx %u", this, m_size, idx);
    }

    T back() const
    {
        if (!empty)
            return (data[s_idx.load() - 1]);
        throw EXCEPTION("bvec out-of-bound exception. this 0x%x, bac() called on empty bvec", this);
    }

    const T * begin() const
    {
        return (&data[0]);
    }
    const T * end() const
    {
        return (&data[s_idx.load()]);
    }

    // create another class that specialized bvec with this?
    void reset()
    {
        s_idx.store(0);
    }
};

#pragma warning(pop)

}

}
