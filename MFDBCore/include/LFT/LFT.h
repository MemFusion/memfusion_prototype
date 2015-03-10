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

#include "LFT/LFTtypes.h"
#include "QueryContext.h"
#include "Perfy.h"
#include "MemFusion/non_copyable.h"
#include "LFT/QueryOperators.h"
#include "MemFusion/LF/bvec.h"
#include "bin.h"

namespace MFDB
{
namespace Core
{
using MemFusion::LF::bvec;

typedef std::pair<uint32, uint32> Stage1Payload;

template <typename T>
class IZ2LFT
{
public:
    virtual void apply_filter(const Bin<Z2raw> * bin, IStage1Producer<T, Stage1Payload> * stage1) const = 0;
};


#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

template <typename T>
class Z2LFT : public IZ2LFT<uint32>
{
    const Z2raw z2raw;
    cuint32 LFTidx;

    // TBD: this should come from configuration
    static const int STAGE1_MAX_NUM_ELEMS = 100000;

    Z2LFT(const Z2LFT &);
    void operator = (const Z2LFT &);
public:
    Z2LFT(Z2raw z2raw_, uint32 idx)
        //: z2raw(z2raw_),
        : z2raw(Z2::remove_doc(z2raw_)),
        LFTidx(idx)
    {
    }

    cuint32 GetLFTidx() const
    {
        return (LFTidx);
    }

    void apply_filter(const Bin<Z2raw> * bin, IStage1Producer<uint32, Stage1Payload> * realstage1) const
    {
        uint64 numAtoms = 0ULL;
        auto core = bin->Get();
        auto numElems = core->s_nFreeElemIdx.load();
        // Note: allocate on stack or on heap?
        xHandle handle;
        EmptySlot<uint32> stage1slot = realstage1->get_stage1(handle);
        auto stage1_begin = stage1slot.first;
        auto stage1_end = stage1slot.second;
        auto stage1_cur = stage1_begin;
        cuint32 binIdx = bin->binIdx();
        auto payload = std::make_pair(LFTidx, binIdx);

        for (uint32 idx = 0; idx < numElems; ++idx)
        {
            // for each Z2 in this elem
            bool notactive = (core->s_vElems[idx].status() != ElemState::ElemActive);
            AtomRange<Z2raw> range = bin->get_elem_range(idx);
            auto range_begin = range.begin();
            auto range_end = range.end();

            if (notactive) continue;

            for (const __m128i * z2ptr = range_begin; z2ptr < range_end; ++z2ptr)
            {
                if (T::apply(z2raw, *z2ptr))
                {
                    *stage1_cur++ = idx;
                    if (stage1_cur == stage1_end)
                    {
                        realstage1->promote(handle, (uint32) std::distance(stage1_begin, stage1_end), payload);
                        // this one might block
                        stage1slot = realstage1->get_stage1(handle);
                        stage1_cur = stage1_begin = stage1slot.first;
                        stage1_end = stage1slot.second;
                    }
                    break;
                }
            }
            numAtoms += range_end - range_begin;
        }

        if (stage1_cur != stage1_begin)
        {
            realstage1->promote(handle, (uint32) std::distance(stage1_begin, stage1_cur), payload);
        }
        MemFusion::Perfy::Instance().add<1>(0, numElems, numAtoms);
    }

};


#pragma warning(pop)


}

}
