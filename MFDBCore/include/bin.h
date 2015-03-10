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

#include "z2types.h"
#include "MemFusion/cache.h"
#include "MemFusion/Exceptions.h"

#include <vector>
#include <tuple>
#include <exception>
#include <atomic>
#include <thread>
#include <algorithm> 

#pragma warning(push)
#pragma warning(disable: 4201)  // nonstandard extension used : nameless struct/union

namespace MFDB
{
namespace Core
{
class Collection;

struct ReleaseBufferError
{
    const void * buffer;
    uint32 elemIdx;
    ReleaseBufferError(const void * b, uint32 idx)
        : buffer(b), elemIdx(idx) {}
};

struct BinFull
{
    uint32 binIdx;
    BinFull(uint32 idx) : binIdx(idx)
    {}
};

enum class ElemState : byte
{
    ElemInactive = 0,
    ElemAcquired,
    ElemActive,
    ElemForgotten,
};

// tuple<idx, size, bool>
#pragma pack(push)
#pragma pack(1)

class ElemInfo
{
    volatile uint64 qword;
public:
    ElemInfo(uint64 q = 0ULL) : qword(q) {}
    operator uint64 () const { return qword; }

    void set(uint64 q) { qword = q; }

    uint32 atomIdx() const      { return (qword &  0x00000000FFFFFFFF); }
    void atomIdx(uint32 value)  { qword = (qword & 0xFFFFFFFF00000000) | uint64(value); }

    uint32 atomSize() const      { return (static_cast<uint32>((qword &  0x00FFFFFF00000000) >> 32)); }
    void atomSize(uint32 value)  { qword = (qword & 0xFF000000FFFFFFFF) | (uint64(value) << 32); }

    ElemState status() const      { return static_cast<ElemState>((qword & 0xFF00000000000000) >> (24 + 32)); }
    void status(ElemState value)  { qword = (qword & 0x00FFFFFFFFFFFFFF) | (uint64(value) << (24 + 32)); }
};

#pragma pack(pop)

template <typename ZT>
class AtomRange
{
    const ZT * m_begin;
    const ZT * m_end;
public:
    AtomRange(const ZT * b, const ZT * e)
        : m_begin(b),
        m_end(e)
    {}
    const ZT * begin() const { return m_begin; }
    const ZT * end() const { return m_end; }
};

// s_  synchronized (depending on type)
// x_  almost synchronized ('x' from approximate)
// f_  fixed

typedef std::vector<ElemInfo> ElemListType;

template <typename ZT>
struct BinCore
{
    std::atomic_uint_fast32_t s_nFreeElemIdx;
    ElemListType s_vElems;
    ZT * f_pRaw;

    BinCore(uint32 maxElems)
        : s_vElems(maxElems) {}

    BinCore(BinCore && other)
        : s_vElems(other.s_vElems),
        f_pRaw(other.f_pRaw)
    {
        other.f_pRaw = nullptr;
        s_nFreeElemIdx.store(other.s_nFreeElemIdx);
        other.s_nFreeElemIdx.store(0);
    }

    BinCore(const BinCore &) = delete;
private:
};

// This data structure has been designed to be append only (for now)
// and concurrent updates.
//
template <typename ZT>
class Bin : private BinCore<ZT>
{
public:
    typedef BinCore<ZT>   base_type;
    typedef Bin<ZT>       self_type;

    static const int a_to_bytes = sizeof(ZT);

    friend class Collection;

    static uint64 ComputeSize(uint32 maxElems, uint64 binsize)
    {
        uint64 static_size  = sizeof(self_type) + sizeof(base_type);
        uint64 dynamic_size = maxElems * sizeof(ElemInfo);
        dynamic_size += binsize;
        return (static_size + dynamic_size);
    }
private:
    // storage required *only* for members below....
    // -----------------------------------------------------------------------
    uint64 f_binSizeBytes;
    uint64 f_binSizeAtoms;
    std::atomic_uint_fast64_t x_nNumActive;
    std::atomic_uint_fast64_t x_nNumDeleted;
    uint32  f_binIdx;
    // -----------------------------------------------------------------------
    // storage required *only* for members above....

    Bin(const Bin &);
    void operator = (const Bin &);

    void ZeroMemory()
    {
        memset(f_pRaw, 0, f_binSizeBytes);
        memset((void*) &s_vElems[0], 0, s_vElems.size()*sizeof(ElemInfo));
    }

    // this must be re-entrant
    uint32 AcquireAtoms(cuint32 elemAtomSize_a)
    {
        auto copy_nFreeElemIdx = s_nFreeElemIdx++;
        uint32 prevAtomLastIdx = 0;

        if (copy_nFreeElemIdx < s_vElems.size() - 1)
        {
            if (copy_nFreeElemIdx > 0)
            {
                ElemInfo & prev = s_vElems[copy_nFreeElemIdx - 1];
                if ((prev.atomIdx() == 0) && (prev.atomSize() == 0))
                {
                    uint32 iterations = 0;
                    do {
                        _mm_pause();
                        if ((++iterations % 1000) == 0)
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 11));
                        }
                    } while ((prev.atomIdx() == 0) && (prev.atomSize() == 0));
                }
                prevAtomLastIdx = prev.atomIdx() + prev.atomSize();
            }
            if (elemAtomSize_a + prevAtomLastIdx >= f_binSizeAtoms)
            {
                --s_nFreeElemIdx;
                throw BinFull(f_binIdx);   // handled
            }
            return copy_nFreeElemIdx;
        }
        else
        {
            --s_nFreeElemIdx;
            throw BinFull(f_binIdx);   // handled
        }
    }

    Bin(uint32 idx, uint64 binsize_, BinCore<ZT> && core)
        : f_binIdx(idx),
        f_binSizeBytes(binsize_),
        f_binSizeAtoms(binsize_ / a_to_bytes),
        base_type(std::move(core))
    {
    }

public:
    ~Bin()
    {
        _aligned_free(f_pRaw);
    }
    Bin(uint32 idx, uint32 maxElems, uint64 binsize_, ZT * pRaw)
        : f_binIdx(idx),
        f_binSizeBytes(binsize_),
        f_binSizeAtoms(binsize_ / a_to_bytes),
        BinCore(maxElems)
    {
#pragma warning(suppress: 6387)
        f_pRaw = pRaw;
    }

    static ZT * AllocateBinRaw(uint64 binsize)
    {
        return (static_cast<ZT*>(_aligned_malloc(binsize, sizeof(CACHE_LINE))));
    }

    Bin(uint32 idx, uint32 maxElems, uint64 binsize_)
        : Bin(idx, maxElems, binsize_, AllocateBinRaw(binsize_))
    {
        ZeroMemory();
    }

    uint32 binIdx() const { return f_binIdx; }
    uint64 binByteSize() const  { return f_binSizeBytes;  }
    uint64 binSizeAtoms() const { return f_binSizeAtoms; }

    void DisableElem(uint32 idx)
    {
        s_vElems[idx].status(ElemState::ElemInactive);
    }

    // this must be re-entrant
    void * AcquireBuffer(uint32 sizeBytes)
    {
        if ((sizeBytes % sizeof(ZT) != 0))
        {
            sizeBytes += sizeof(ZT) - (sizeBytes % sizeof(ZT));
        }
        cuint32 elemAtomSize_a = (sizeBytes / a_to_bytes) + ((sizeBytes % a_to_bytes) > 0 ? 1 : 0);
        uint32 copy_nFreeElemIdx = AcquireAtoms(elemAtomSize_a);
        ++x_nNumActive;
        ElemInfo & elem = s_vElems[copy_nFreeElemIdx];
        cuint32 atomIdx_a = (copy_nFreeElemIdx == 0) ? 0 :
            (s_vElems[copy_nFreeElemIdx - 1].atomIdx() + s_vElems[copy_nFreeElemIdx - 1].atomSize());
        elem.atomIdx(atomIdx_a);
        elem.atomSize(elemAtomSize_a);
        elem.status(ElemState::ElemAcquired);
        void * ret = &f_pRaw[elem.atomIdx()];
        return (ret);
    }

    void ReleaseBuffer(const void * buffer)
    {
        ElemInfo target(0ULL);
        uint64 diff64bit = static_cast<const ZT*>(buffer) - f_pRaw;
        target.atomIdx(static_cast<uint32>(diff64bit));

        auto last = s_vElems.begin() + s_nFreeElemIdx;
        while (last->atomSize() == 0)
        {
            --last;
            if (last == s_vElems.begin())
                break;
        }

        auto ret = std::lower_bound<ElemListType::iterator, ElemInfo>
            (s_vElems.begin(), last, target,
            [](const ElemInfo & left, const ElemInfo & right) -> bool
        {
            return (left.atomIdx() < right.atomIdx());
        });

        if (ret != s_vElems.end())
        {
            ElemInfo & elem = *ret;
            void * check = &f_pRaw[elem.atomIdx()];
            if (check != buffer)
                throw ReleaseBufferError(buffer, elem.atomIdx());  //handled
            elem.status(ElemState::ElemActive);
        }
        else {
            throw ReleaseBufferError(buffer, 0xFFFFFFFF);  //handled
        }
    }

    bool contains(const void * buffer) const
    {
        const ZT * ptr = static_cast<const ZT*>(buffer);
        return ((ptr >= f_pRaw) && (ptr < &f_pRaw[f_binSizeAtoms]));
    }

    // -------------------------------------------------------
    const BinCore<ZT> * Get() const
    {
        return (this);
    }

    AtomRange<ZT> get_elem_range(uint idx) const
    {
        if (idx < s_nFreeElemIdx)
        {
            const ElemInfo & elem = s_vElems[idx];
            return AtomRange<ZT>(&f_pRaw[elem.atomIdx()],
                                 &f_pRaw[elem.atomIdx() + elem.atomSize()]);
        }
        throw EXCEPTION("BinOOB exception. this 0x%x, binIdx %u, elemidx", binIdx(), idx);
    }
};

template <typename ZT>
class ElemIter
{
    const Bin<ZT> & bin;
    uint32 elemIdx;
    bool end;

    ElemIter & operator = (const ElemIter & other);
public:
    ElemIter(const Bin<ZT> & b, uint32 idx = 0)
        : bin(b),
        elemIdx(idx)
    {
        auto binelems = bin.Get()->s_nFreeElemIdx.load();
        end = (elemIdx == binelems);
        if (elemIdx > binelems)
        {
            throw EXCEPTION("Bin out-of-range exception. this 0x%x, binIdx %u, elemidx", binIdx(), idx);
        }
    }

    ElemIter(const ElemIter & other)
        : bin(other.bin),
        elemIdx(other.elemIdx)
    {}

    uint32 get_index() const { return elemIdx; }

    ElemIter next() const
    {
        return ElemIter(bin, end ? elemIdx : elemIdx + 1);
    }

    uint32 operator - (const ElemIter & other) const
    {
        if (elemIdx >= other.elemIdx)
            return (elemIdx - other.elemIdx);
        throw EXCEPTION("Bin difference exception. this 0x%x, binIdx %u, elemidx", bin.binIdx());
    }

    const ElemInfo & get_elem() const
    {
        const ElemInfo & elem = bin.Get()->s_vElems[elemIdx];
        return (elem);
    }

    AtomRange<ZT> get_range() const
    {
        const ElemInfo & elem = bin.Get()->s_vElems[elemIdx];
        const ZT * raw = bin.Get()->f_pRaw;
        return AtomRange<ZT>(&raw[elem.atomIdx()], &raw[elem.atomIdx() + elem.atomSize()]);
    }

    bool operator == (const ElemIter<ZT> & other) const
    {
        if (bin.binIdx() != other.bin.binIdx())
            throw EXCEPTION("Bin cmp exception. this 0x%x, binIdx1 %u, binIdx2", bin.binIdx(), other.bin.binIdx());
        return (elemIdx == other.elemIdx);
    }
    bool operator != (const ElemIter<ZT> & other) const
    {
        return !(operator == (other));
    }

    void operator ++ ()
    {
        if (++elemIdx > bin.Get()->s_nFreeElemIdx.load())
        {
            throw EXCEPTION("OutOfRange exception. bin %u.. elemIdx %u", bin.binIdx(), elemIdx);
        }
    }
};

template <typename ZT>
class ElemRange
{
    ElemIter<ZT> m_begin;
    ElemIter<ZT> m_end;
public:
    ElemRange(const ElemIter<ZT> & b, const ElemIter<ZT> & e)
        : m_begin(b),
        m_end(e)
    {}
    ElemIter<ZT> begin() const { return m_begin; }
    ElemIter<ZT> end() const { return m_end; }
};

}
}

#pragma warning(pop)

// transaction: search 

