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
#include <array>
#include <map>
#include <atomic>

#include "MemFusion/types.h"
#include "z2types.h"
#include "MemFusion/non_copyable.h"
#include "MemFusion/syncqueue.h"
#include "MemFusion/opt.h"

namespace MFDB
{
namespace Core
{
using MemFusion::non_copyable;

enum class QO : uint32 {
    EQ = 1,
    GT = 2,
    GTE = 3,
    IN = 4,
    LT = 5,
    LTE = 6,
    NE = 7,
    NIN = 8,
    OR = 9,
    AND = 10,
    NOT = 11,
    NOR = 12,
    EXISTS = 13,
    TYPE = 14,
    MOD = 15,
    REGEX = 16,
    WHERE = 17,
    GEOWITHIN = 18,
    GEOINTERSECTS = 19,
    NEAR = 20,
    NEARSPHERE = 21,
    ALL = 22,
    ELEMMATCH = 23,
    SIZE = 24,
    DOLLAR = 25,
    SLICE = 26,
    GROUP = 27,
    SUM = 28,
    MULTIPLY = 29,
    ADD = 30,
    AVG = 31,
    COUNT = 32,
    MIN = 33,
    MAX = 34,

    START = 9999,
    END = 9998,
    AND_ALL = 9997,
};

#pragma pack(push)
#pragma pack(1)

struct LFTraw
{
    uint32 idx;
    QO qo;
    uint64 pad;
    Z2raw z2raw;
};

struct QPraw
{
    QO command;
    uint32 kids;
};
#pragma pack(pop)


enum LFTConstants
{
    LFT_RAW_SIZE = 20,
    QP_RAW_SIZE = 8,
};

template <typename T>
using veciter = typename std::vector<T>::iterator;

template <typename T>
using cveciter = typename std::vector<T>::const_iterator;

template <typename T, typename Payload>
using FullSlot = std::tuple<cveciter<T>, cveciter<T>, Payload>;

template <typename T>
using EmptySlot = std::pair<veciter<T>, veciter<T>>;

#define make_fullslot(A,B,C) std::make_tuple(A,B,C)
#define make_emptyslot(A,B) std::make_pair(A,B)


typedef uint64 xHandle;

template <typename T, typename Payload>
class IStage1Producer
{
public:
    virtual EmptySlot<T> get_stage1(xHandle &) = 0;
    virtual void promote(const xHandle, uint32 numElems, Payload payload) = 0;
};

template <typename T, typename Payload>
class IStage1Consumer
{
public:
    virtual FullSlot<T, Payload> consume_promoted_slot(xHandle &) = 0;
    virtual void release_promoted_slot() = 0;
};

struct Stage1Full {};
struct Stage1PromoteFailed {};
struct Stage1ReleaseFailed {};

template <typename T, typename Payload>
class Stage1 : public IStage1Producer<T, Payload>,
    public IStage1Consumer<T, Payload>,
               public non_copyable
{
    static const uint32 ADD_ITERATIONS_SPIN_LIMIT = 10000;
    static const uint32 STAGE1_NUM_SLOTS = 10;
    static const uint32 MAX_STAGE1_WAIT_ITERATIONS = 1000;
    static const uint32 STAGE1_WAIT_ITERATIONS_MS = 1;

    std::vector<std::vector<T>> stage1Slots;
    std::vector<std::atomic<uint32> *> slotOwners;
    std::array<uint32, STAGE1_NUM_SLOTS> slotStatus;
    std::array<uint32, STAGE1_NUM_SLOTS> slotElems;
    std::array<Payload, STAGE1_NUM_SLOTS> slotPayloads;
    uint32 consumerSlotIdx;

    // status
    std::atomic<uint64> s_promotedSlots;

    // metrics
    std::atomic<uint64> fullStage1Iterations;
    std::atomic<uint64> fullStage1Threads;

    Stage1(const Stage1 &);

    enum SLOT : uint32
    {
        Available = 0,
        ProducerOwned,
        ProducerDone,
        ConsumerOwned,
    };

    opt<uint32> AcquireFreeSlot()
    {
        for (uint32 slotIdx = 0; slotIdx < STAGE1_NUM_SLOTS; ++slotIdx)
        {
            uint32 expected = STAGE1_NUM_SLOTS;
            if (slotOwners[slotIdx]->compare_exchange_strong(expected, slotIdx))
            {
                slotStatus[slotIdx] = SLOT::ProducerOwned;
                return (opt<uint32>(slotIdx));
            }
        }
        return (opt<uint32>());
    }
public:
    Stage1(uint32 elemsPerConsumer)
        : stage1Slots(STAGE1_NUM_SLOTS),
        slotOwners(STAGE1_NUM_SLOTS),
        fullStage1Threads(0LL),
        fullStage1Iterations(0LL),
        consumerSlotIdx(STAGE1_NUM_SLOTS),
        s_promotedSlots(0ULL)
    {
        for (uint32 idx = 0; idx < stage1Slots.size(); ++idx)
        {
            stage1Slots[idx].resize(elemsPerConsumer);
        }
        for (uint32 idx = 0; idx < slotOwners.size(); ++idx)
        {
            slotOwners[idx] = new std::atomic<uint32>(STAGE1_NUM_SLOTS);
            slotElems[idx] = 0;
            slotStatus[idx] = SLOT::Available;
            slotPayloads[idx] = Payload();
        }
    }

    EmptySlot<T> get_stage1(xHandle & handle)
    {
        bool waited = false;
        for (uint32 times = 0; times < MAX_STAGE1_WAIT_ITERATIONS; ++times)
        {
            opt<uint32> oslotIdx = AcquireFreeSlot();
            if (oslotIdx.is_initialized())
            {
                if (waited)   ++fullStage1Threads;
                uint32 slotIdx = oslotIdx.get();
                handle = slotIdx | 0x1111111100000000;
                std::vector<T> & vec = stage1Slots[slotIdx];
                return (make_emptyslot(vec.begin(), vec.end()));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(STAGE1_WAIT_ITERATIONS_MS));
            waited = true;
            ++fullStage1Iterations;
        }
        throw (Stage1Full());
    }

    void promote(const xHandle handle, uint32 numElems, Payload payload)
    {
        uint32 slotIdx = static_cast<uint32>(handle & 0xFFFFFFFF);
        if (*slotOwners[slotIdx] == slotIdx)
        {
            slotElems[slotIdx] = numElems;
            slotPayloads[slotIdx] = payload;
            slotStatus[slotIdx] = SLOT::ProducerDone;
            ++s_promotedSlots;
            return;
        }
        throw (Stage1PromoteFailed());
    }

    uint64 GetNumberOfPromotedSlots() const
    {
        return (s_promotedSlots);
    }

    FullSlot<T, Payload> consume_promoted_slot(xHandle & handle)
    {
        if (s_promotedSlots.load() > 0)
        {
            for (uint32 slotIdx = 0; slotIdx < STAGE1_NUM_SLOTS; ++slotIdx)
            {
                if (slotStatus[slotIdx] == SLOT::ProducerDone)
                {
                    consumerSlotIdx = slotIdx;
                    slotStatus[slotIdx] = SLOT::ConsumerOwned;
                    uint32 numElems = slotElems[slotIdx];
                    handle = slotIdx;
                    --s_promotedSlots;
                    const std::vector<T> & vec = stage1Slots[slotIdx];
                    return (make_fullslot(vec.begin(), vec.begin() + numElems, slotPayloads[slotIdx]));
                }
            }
        }
        handle = 0;
        return (make_fullslot(stage1Slots[0].cend(), stage1Slots[0].cend(), Payload()));
    }
    void release_promoted_slot()
    {
        if (consumerSlotIdx < STAGE1_NUM_SLOTS)
        {
            slotStatus[consumerSlotIdx] = SLOT::Available;
            slotOwners[consumerSlotIdx]->store(STAGE1_NUM_SLOTS);
            slotPayloads[consumerSlotIdx] = Payload();
            consumerSlotIdx = STAGE1_NUM_SLOTS;
            return;
        }
        throw (Stage1ReleaseFailed());
    }

    __m128i GetMetrics() const
    {
        __m128i ret;
        ret.m128i_u64[0] = fullStage1Iterations.load();
        ret.m128i_u64[1] = fullStage1Threads.load();
        return (ret);
    }
};

struct Stage2
{
protected:
    typedef std::map<cuint32, std::vector<cuint32>> DataType;
    typedef DataType::iterator iterator;

    DataType elem2LFTidxs;  // matching LFTs

public:
    void add(cuint32 elemIdx, cuint32 LFTidx)
    {
        elem2LFTidxs[elemIdx].push_back(LFTidx);
    }

    iterator begin() { return (elem2LFTidxs.begin()); }
    iterator end() { return (elem2LFTidxs.end()); }

    size_t size() const { return (elem2LFTidxs.size()); }

    void clear()
    {
        elem2LFTidxs.clear();
    }
};

typedef std::vector<uint32> LFTStage3;

}

}

