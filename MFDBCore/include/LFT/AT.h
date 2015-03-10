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

#include <mutex>
#include <atomic>
#include <thread>

#include "LFT/LFT.h"
//#include "LFT/ATtypes.h"

namespace MFDB
{
namespace Core
{
using MemFusion::LF::bvec;


#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

class NV
{
    Z2raw group;
    double accvalue;

public:
    template <typename TAcc>
    NV(Z2raw gr, TAcc acc)
        : group(gr),
        accvalue(static_cast<double>(acc))
    {
    }

    NV() : accvalue(0) {}

    Z2raw Group() const { return (group); }
    double AccValue() const { return (accvalue); }
};

struct AggrSUM
{
public:
    static const QO AccOp = QO::SUM;
};

template <typename T>
class IZ2LAT
{
public:
    virtual void apply_filter(const Bin<Z2raw> * bin, IStage1Producer<T, Stage1Payload> * stage1) const = 0;
};

struct SUMExtractPolicy
{
    static Z2value Apply(const Z2raw z)
    {
        return (Z2(z).z2value());
    }
};

struct COUNTExtractPolicy
{
    static Z2value Apply(const Z2raw)
    {
        return (Z2value(1));
    }
};

class Z2LATBase : public IZ2LFT < NV >
{
    Z2LATBase(const Z2LATBase &);
    void operator = (const Z2LATBase &);

protected:
    const Z2name z2groupname;
    const Z2name z2accname;
    const Z2name z2tgtname;
    const QO accop;
    cuint32 LFTidx;

    // TBD: this should come from configuration
    static const int STAGE1_MAX_NUM_ELEMS = 100000;

public:
    // [UTF8String ("_id", (7, "$state "));
    //      EmbeddedDoc ("totalPop", (125, [UTF8String ("$sum", (5, "$pop "))]));
    // parameters would be:
    //  group:  $state
    //  tgtname: totalPop
    //  accname: pop
    //  op: $sum
    //
    Z2LATBase(Z2name groupname, Z2name accname, Z2name tgtname, QO op, uint32 idx)
        : z2groupname(groupname),
        z2accname(accname),
        z2tgtname(tgtname),
        LFTidx(idx),
        accop(op)
    {
    }

    Z2name GetGroupName() const
    {
        return (z2groupname);
    }

    Z2name GetAccName() const
    {
        return (z2accname);
    }

    Z2name GetTgtName() const
    {
        return (z2tgtname);
    }
    QO GetAccOp() const
    {
        return (accop);
    }

    cuint32 GetLFTidx() const
    {
        return (LFTidx);
    }
};

#pragma warning(push)
#pragma warning(disable: 4701)  // potentially uninitialized local variable ... used

template <typename ExtractPolicy>
class Z2LAT : public Z2LATBase
{
public:
    Z2LAT(Z2name groupname, Z2name accname, Z2name tgtname, QO op, uint32 idx)
        : Z2LATBase(groupname, accname, tgtname, op, idx)
    {}

    void apply_filter(const Bin<Z2raw> * bin, IStage1Producer<NV, Stage1Payload> * realstage1) const
    {
        uint64 numAtoms = 0ULL;
        auto core = bin->Get();
        cuint32 binIdx = bin->binIdx();
        auto numElems = core->s_nFreeElemIdx.load();

        xHandle handle;
        EmptySlot<NV> stage1slot = realstage1->get_stage1(handle);
        auto stage1_begin = stage1slot.first;
        auto stage1_end = stage1slot.second;
        auto stage1_cur = stage1_begin;
        uint32 stage1_elems = (uint32)std::distance(stage1_begin, stage1_end);
        auto payload = std::make_pair(LFTidx, binIdx);

        for (uint32 idx = 0; idx < numElems; ++idx)
        {
            // for each Z2 in this elem
            bool notactive = (core->s_vElems[idx].status() != ElemState::ElemActive);
            AtomRange<Z2raw> range = bin->get_elem_range(idx);
            auto range_begin = range.begin();
            auto range_end = range.end();

            if (notactive) continue;

            bool valueFound = false;
            bool matchedGroup = false;
            Z2value accvalue;
            Z2raw group;

            for (const __m128i * z2ptr = range_begin; z2ptr < range_end; ++z2ptr)
            {
                const Z2raw z = *z2ptr;
                if (z2groupname == Z2(z).z2name())
                {
                    matchedGroup = true;
                    group = z;
                    if (valueFound)
                        break;
                }
                if (z2accname == Z2(z).z2name())
                {
                    valueFound = true;
                    //accvalue = Z2(z).z2value();
                    accvalue = ExtractPolicy::Apply(z);
                    if (matchedGroup)
                        break;
                }
            }
            if (valueFound && matchedGroup)
            {
                *stage1_cur++ = NV(group, accvalue);
                if (stage1_cur == stage1_end)
                {
                    // this one might block
                    realstage1->promote(handle, stage1_elems, payload);
                    stage1slot = realstage1->get_stage1(handle);
                    stage1_cur = stage1_begin = stage1slot.first;
                    stage1_end = stage1slot.second;
                }
            }
            numAtoms += range_end - range_begin;
        }

        if (stage1_cur != stage1_begin)
        {
            // this one might block
            realstage1->promote(handle, (uint32) std::distance(stage1_begin, stage1_cur), payload);
        }
        MemFusion::Perfy::Instance().add<1>(0, numElems, numAtoms);
    }

};

#pragma warning(pop)

typedef std::vector<NV> ATStage3;


}

}
