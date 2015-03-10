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

#include <vector>
#include <algorithm>

#include "MemFusion/types.h"
#include "z2types.h"
#include "MemFusion/non_copyable.h"
#include "MemFusion/syncqueue.h"
#include "MemFusion/opt.h"
#include "MemFusion/Cache.h"
#include "MemFusion/Buffer.h"
#include "MemFusion/LF/bvec.h"
#include "MemFusion/Utils.h"
#include "MemFusion/CatchAllThread.h"
#include "MemFusion/cancellation_token.h"

namespace MFDB
{
namespace Core
{
using MemFusion::non_copyable;
using MemFusion::Buffer;
using MemFusion::LF::bvec;
using MemFusion::cancellation_token;

template <typename T>
class Z2Query;

#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

//  LFTidx, binIdx
typedef std::tuple<uint32, uint32> Chore;

// TBD: from configuration
enum Constants
{
    STAGE1_ELEMS_PER_THREAD = 100 * 1024,
    STAGE2_MAX_ELEMS_PER_BIN = 100 * 1024,
    COMPOSER_SLEEP_MS = 1,
};

struct QueryMetrics
{
    CACHE_ALIGN vuint64 numCores;
    CACHE_ALIGN vuint64 numChores;
    CACHE_ALIGN vuint64 numLFTs;
    CACHE_ALIGN vuint64 numBins;
    CACHE_ALIGN vuint64 prepare_us;
    CACHE_ALIGN vuint64 lfts_us;
    CACHE_ALIGN vuint64 composer_us;
    CACHE_ALIGN vuint64 project_us;
    CACHE_ALIGN vuint64 queueWait_ms;
    CACHE_ALIGN vuint64 cancellations;
    CACHE_ALIGN vuint64 choresDonePerThread[256];
    CACHE_ALIGN vuint64 composerIterations;
    CACHE_ALIGN vuint64 stage1Iterations;
    CACHE_ALIGN vuint64 stage1ThreadsSlowed;
};

template <typename T1, typename T4, typename Payload1>
struct QueryContext
{
    typedef QueryContext<T1, T4, Payload1> self_type;
    typedef self_type QC;
    typedef T1 T1;
    typedef T4 T4;
    typedef Payload1 Payload1;

    static cuint32 STAGE1_ELEMS_PER_THREAD = 100 * 1024;
private:
    QueryContext(const self_type &);
    void operator = (const self_type &);
    QueryContext();

public:
    CACHE_ALIGN volatile long workDone;
    uint32 numLFTs;
    uint32 numBins;
    uint32 LFTthreads;
    Buffer & retbuf;

    const Z2Query<T1> * pz2query;
    const bvec<Bin<Z2raw>*> & bins;

    MemFusion::syncqueue<opt<Chore>> chorequeue;
    Stage1<T1,Payload1> stage1Common;
    std::vector<T4> matchesPerBin;
    std::vector<vuint64*> choresDonePerBin;
    QueryMetrics metrics;
    std::function<uint32(xHandle)> handleDecoder;

    ~QueryContext()
    {
        for (auto chore : choresDonePerBin)
        {
            delete chore;
        }
    }

    QueryContext(const Z2Query<T1> * pz2query_, const bvec<Bin<Z2raw>*> & bins_, Buffer & retbuf_, uint32 stage1ElemsPerThread, std::function<uint32(xHandle)> decoder)
        : pz2query(pz2query_),
        stage1Common(stage1ElemsPerThread),
        numLFTs(pz2query_->lft_size()),
        retbuf(retbuf_),
        bins(bins_),
        handleDecoder(decoder)
    {
        numBins = bins.size();

        for (uint32 idx = 0; idx < numBins; ++idx)
        {
            choresDonePerBin.push_back(new vuint64);
            *choresDonePerBin.back() = 0ULL;
        }

        matchesPerBin.resize(numBins);
        memset(&metrics, 0, sizeof(metrics));

        uint32 numCores = std::thread::hardware_concurrency();

        for (uint32 binIdx = 0; binIdx < numBins; ++binIdx)
        {
            for (uint32 LFTidx = 0; LFTidx < numLFTs; ++LFTidx)
            {
                chorequeue.enqueue(std::make_tuple(LFTidx, binIdx));
            }
        }
        metrics.numChores = chorequeue.size();
        LFTthreads = std::min<uint32>(static_cast<uint32>(metrics.numChores), (numCores * 3) / 2);
        metrics.numLFTs = numLFTs;
        metrics.numCores = numCores;
        metrics.numBins = numBins;
        workDone = 0L;
    }

    void RunLeafs(uint32 thdIdx, cancellation_token & token)
    {
        uint64 myWaitMS = 0ULL;

        // pick from chore queue
        while (!InterlockedAdd(&workDone, 0) && (!token.canceled()))
        {
            TimeStamp one;
            opt<Chore> chore = chorequeue.dequeue();
            TimeStamp two;
            myWaitMS += TimeStamp::millis(one, two);

            if (token.canceled())
                break;

            if (chore.is_initialized() && (!InterlockedAdd(&workDone, 0)))
            {
                uint32 LFTidx = std::get<0>(chore.get());
                uint32 binIdx = std::get<1>(chore.get());
                const IZ2LFT<T1> * lft = pz2query->get_lft(LFTidx);

                lft->apply_filter(bins[binIdx], &stage1Common);

                InterlockedIncrement64(choresDonePerBin[binIdx]);
                InterlockedIncrement64(&metrics.choresDonePerThread[thdIdx]);
            }
        }

        if (token.canceled())
        {
            InterlockedIncrement64(&metrics.cancellations);
        }

        InterlockedAdd64(&metrics.queueWait_ms, myWaitMS);
    }

    void BeDone()
    {
        InterlockedExchange(&workDone, 1);
        for (uint32 threadIdx = 0; threadIdx < LFTthreads; ++threadIdx)
        {
            chorequeue.enqueue(opt<Chore>());
        }
    }

    void CancelComputations()
    {
        BeDone();
        InterlockedIncrement64(&metrics.cancellations);
    }

    void ProcessQuery(std::function<void(FullSlot<T1,Payload1>)> stage2_lambda, std::function<void(uint32)> stage3_lambda)
    {
        TimeStamp start;

        cancellation_token token([this]()
        {
            CancelComputations();
        });

        CatchAllThread ComposerThread(
            [this, stage2_lambda, stage3_lambda](cancellation_token & token)
        {
            DEBUG_ONLY_SET_THREAD_NAME("Composer");
            Composer(stage2_lambda, stage3_lambda, token);
        }, token);

        MemFusion::parallel_for(0U, LFTthreads,
            [this](uint thdIdx, cancellation_token & token)
        {
            DEBUG_ONLY_SET_THREAD_NAME_WITH_INDEX("Worker ", thdIdx);
            RunLeafs(thdIdx, token);
        }, token);
        TimeStamp joinedLTFs;

        ComposerThread.join();
        TimeStamp joinedComposer;

        auto st1 = stage1Common.GetMetrics();
        metrics.stage1Iterations = st1.m128i_u64[0];
        metrics.stage1ThreadsSlowed = st1.m128i_u64[1];
        metrics.lfts_us = TimeStamp::millis(start, joinedLTFs);
        metrics.composer_us = TimeStamp::millis(joinedLTFs, joinedComposer);
    }

    void Composer(std::function<void(FullSlot<T1, Payload1>)> stage2_lambda, std::function<void(uint32)> stage3_lambda, cancellation_token & token)
    {
        std::vector<uint32> binIdexes(numBins);
        for (uint32 idx = 0; idx < numBins; ++idx) { binIdexes[idx] = idx; }

        while ((binIdexes.size() > 0) && (!token.canceled()))
        {
            std::vector<uint32> binDelenda;

            while (stage1Common.GetNumberOfPromotedSlots() > 0)
            {
                xHandle handle;
                FullSlot<T1, Payload1> slot = stage1Common.consume_promoted_slot(handle);
                if (std::get<0>(slot) != std::get<1>(slot))
                {
                    stage2_lambda(slot);
                    stage1Common.release_promoted_slot();
                }
            }

            // collect stage1 and merge into stage2 for active Bins
            //
            for (uint32 binIdx : binIdexes)
            {
                auto choresDone = InterlockedAdd64(choresDonePerBin[binIdx], 0ULL);
                if (choresDone == numLFTs)
                {
                    binDelenda.push_back(binIdx);
                }
            }  // for each Bin

            std::for_each(std::begin(binDelenda), std::end(binDelenda),
                [&binIdexes](uint32 delidx)
            {
                auto iter = std::find(std::begin(binIdexes), std::end(binIdexes), delidx);
                if (iter != std::end(binIdexes))
                {
                    binIdexes.erase(iter);
                }
            });

            // Bins that have done scanning we can do stage3 (QP)
            for (uint32 binIdx = 0; binIdx < numBins; ++binIdx)
            {
                auto choresDone = InterlockedAdd64(choresDonePerBin[binIdx], 0ULL);
                if (choresDone == numLFTs)
                {
                    stage3_lambda(binIdx);
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(COMPOSER_SLEEP_MS));
            InterlockedIncrement64(&metrics.composerIterations);
        }

        // done! (or cancelled)
        BeDone();
    }

};

#pragma warning(pop)

}
}


