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
#include <mutex>
#include <algorithm>
#include <assert.h>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <memory>

#include "Collection.h"
#include "MemFusion/parallel_for.h"
#include "MemFusion/Logger.h"
#include "MemFusion/LF/bvec2.h"
#include "MemFusion/TimeStamp.h"
#include "MemFusion/Buffer.h"

#undef min

namespace MFDB
{
namespace Core
{
using namespace std;


#define TRUE    1
#define FALSE   0

const char * Collection::BIN_SERIALIZATION_EXTENSION = "bin";


void FindProcessData(uint32 numLFTs, Stage2 & stage2PerBin, const Z2FindQuery & z2query, LFTStage3 & matchesPerBin)
{
    for (auto stage2elem : stage2PerBin)
    {
        uint32 elemIdx = stage2elem.first;
        vector<cuint32> & trues = stage2elem.second;
        vector<bool> lfts(numLFTs, false);

        for (uint LFTidx = 0; LFTidx < numLFTs; ++LFTidx)
        {
            auto iter = std::find(trues.begin(), trues.end(), LFTidx);
            if (iter != trues.end())
            {
                lfts[LFTidx] = true;
                trues.erase(iter);
            }
        }

        bool match = z2query.apply_qp(lfts);
        if (match)
        {
            matchesPerBin.push_back(elemIdx);
        }
    }
    stage2PerBin.clear();
}


template <typename T>
class SortSecond
{
public:
    bool operator () (const T & left, const T & right) const
    {
        return (left.second < right.second);
    }
};

// returns number of z2 elements in retbuf
uint32 Collection::Aggregate(uint64 transId, Buffer & retbuf, const Z2AggrQuery * z2query)
{
    typedef QueryContext<NV, ATStage3, Stage1Payload> QC;

    uint32 ret = 0;
    try
    {
        // db.test.aggregate({ $group: {_id:"$state", totalPop: {$sum: "$pop"} } } )
        // totalPop, $pop: Z2TargetName,Z2AccName  ... assuming short name
        // state:          Z2GroupValue   ... need to keep vlen
        // count, $pop:

        map<Z2AccName, map<Z2Group, double, CompGroupRaw>> stage2_data;
        map<Z2Group, map<Z2AccName, double>, CompGroupRaw> stage3_data;

        std::vector<Z2name> tgtnames;
        std::vector<QO> accops;
        for (uint32 idx = 0; idx < z2query->lft_size(); ++idx)
        {
            auto z2lat = dynamic_cast<const Z2LATBase *>(z2query->get_lft(idx));
            tgtnames.push_back(z2lat->GetTgtName());
            accops.push_back(z2lat->GetAccOp());
        }

        auto stage2_lambda = [&stage2_data, this, tgtnames, accops]
            (FullSlot<NV,Stage1Payload> slot)
        {
            cuint32 LFTidx = std::get<2>(slot).first;
            QO accop = accops[LFTidx];
            map<Z2Group, double, CompGroupRaw> & stage2 = stage2_data[tgtnames[LFTidx]];

            auto iter = accumulators.find(accop);
            if (iter != accumulators.end()) {
                accumulators[accop](std::get<0>(slot), std::get<1>(slot), stage2);
            } else {
                throw EXCEPTION("Unknown accumulator %u in aggregation.", accop);
            }
        };

        //map<Z2AccName, map<Z2GroupValue, double>> stage2_data;
        //map<Z2Group, map<Z2AccName, double>> stage3_data;

        auto stage3_lambda = [&stage2_data, &stage3_data](uint32 binIdx)
        {
            (void) binIdx;
            for (auto stage2elem : stage2_data)
            {
                Z2AccName accname = stage2elem.first;
                const map<Z2Group, double, CompGroupRaw> & onemap = stage2elem.second;

                for (auto iter2 = onemap.cbegin(); iter2 != onemap.cend(); ++iter2)
                {
                    Z2Group group = iter2->first;
                    double accvalue = iter2->second;
                    stage3_data[group][accname] = accvalue;
                }
            }
        };

        auto decoder = [](xHandle handle) -> uint32 { return (static_cast<uint32>(handle)); };
        auto queryCtx = CreateQueryProcessor<QC>(transId, retbuf, z2query, decoder);
        queryCtx->ProcessQuery(stage2_lambda, stage3_lambda);

        //map<Z2AccName, map<Z2GroupValue, double>> stage2_data;
        //map<Z2Group, map<Z2AccName, double>> stage3_data;

        if (z2query->GetSort() != 0)
        {
            typedef pair<Z2Group, map<Z2AccName, double>> ValuePair;
            vector<ValuePair> stage3_data_sorted;
            stage3_data_sorted.reserve(stage3_data.size());

            for (auto groupvec : stage3_data)
            {
                stage3_data_sorted.push_back(groupvec);
            }
            std::sort(stage3_data_sorted.begin(), stage3_data_sorted.end(),
                [](const ValuePair & left, const ValuePair & right) -> bool
            {
                return (left.second.begin()->second < right.second.begin()->second);
            });
            // write output
            ret = WriteAggregationOutput<double>(stage3_data_sorted, retbuf);
        }
        else {
            // write output
            ret = WriteAggregationOutput<double>(stage3_data, retbuf);
        }

        lastQueryCounters = queryCtx->metrics;

        //printf("Stage1: iterations %llu,  slow threads=%llu,  cancellations=%llu\n",
        //    lastQueryCounters.stage1Iterations, lastQueryCounters.stage1ThreadsSlowed, lastQueryCounters.cancellations);
    }
    catch (std::exception & ex)
    {
        std::stringstream ss;
        ss << "Collection " << name() << " c++ exception in " << __FUNCTION__ << ": " << ex.what();
        LOG(ss.str());
    }
    catch (...)
    {
        std::stringstream ss;
        ss << "Collection " << name() << " unknown exception in " << __FUNCTION__;
        LOG(ss.str());
    }

    return (ret);
}

// returns number of z2 elements in retbuf
uint32 Collection::FindAndProject(uint64 transId, opt<Projections&> onames, Buffer & retbuf, const Z2FindQuery * z2query)
{
    typedef QueryContext<uint32, LFTStage3, Stage1Payload> QC;  // 3rd is LFTidx

    uint32 ret = 0;
    try
    {
        std::vector<Stage2> stage2PerBin(bins.size());
        uint32 numLFTs = z2query->lft_size();

        auto stage2_lambda = [this, &stage2PerBin]
        // elemIdx, LFTidx
        (FullSlot<uint32, Stage1Payload> slot)
        {
            cuint32 LFTidx = std::get<2>(slot).first;
            cuint32 binIdx = std::get<2>(slot).second;
            Stage2 & stage2 = stage2PerBin[binIdx];
            std::for_each(std::get<0>(slot), std::get<1>(slot),
                [&stage2, LFTidx](uint32 elemIdx)
            {
                stage2.add(elemIdx, LFTidx);
            });
        };
        auto handle2LFTidx = [](xHandle handle) -> uint32 { return (static_cast<uint32>(handle & 0xFFFFFFFF)); };
        auto queryCtx = CreateQueryProcessor<QC>(transId, retbuf, z2query, handle2LFTidx);

        auto stage3_lambda =
            [&stage2PerBin, &queryCtx, z2query, numLFTs](uint32 binIdx)
        {
            if (stage2PerBin[binIdx].size() > 0)
            {
                FindProcessData(numLFTs, stage2PerBin[binIdx], *z2query, queryCtx->matchesPerBin[binIdx]);
            }
        };

        queryCtx->ProcessQuery(stage2_lambda, stage3_lambda);

        ret = FindProjectPhase(queryCtx->matchesPerBin, retbuf, onames);

        lastQueryCounters = queryCtx->metrics;
    }
    catch (std::exception & ex)
    {
        std::stringstream ss;
        ss << "Collection " << name() << " c++ exception in " << __FUNCTION__ << ": " << ex.what();
        LOG(ss.str());
    }
    catch (...)
    {
        std::stringstream ss;
        ss << "Collection " << name() << " unknown exception in " << __FUNCTION__;
        LOG(ss.str());
    }

    return (ret);
}

uint32 Collection::FindAndReturnAll(uint64 transId, Buffer & retbuf, const Z2FindQuery * z2query)
{
    return FindAndProject(transId, opt<Projections&>(), retbuf, z2query);
}

void Collection::grow(Bin<Z2raw>* bin)
{
    bins.add(bin);
}

void Collection::grow()
{
    std::stringstream ss;
    ss << "Collection " << name() << " adding one Bin to " << bins.size() + 1;
    if (m_cfgi.binMaxSize >= 1024 * 1024)
    {
        ss << " (" << m_cfgi.binMaxSize / (1024*1024) << " MB each Bin).";
    }
    else {
        ss << " (" << m_cfgi.binMaxSize / (1024) << " KB each Bin).";
    }
#ifndef NDEBUG
    if (bins.size() > 0) {
        ss << " Last Bin contains " << bins.back()->s_nFreeElemIdx << " elements";
    }
#endif
    LOG(ss.str());


    uint64 expected_size = Bin<Z2raw>::ComputeSize(m_cfgi.binMaxElems, m_cfgi.binMaxSize);
    (void) expected_size;
    //Create

    grow(new Bin<Z2raw>((uint32) bins.size(), m_cfgi.binMaxElems, m_cfgi.binMaxSize));
}

Path Collection::ComposeBinSerializedPath(cuint32 binIdx)
{
    Path retpath = m_percyCollectionBasePath
        .Append(Platform::DOT)
        .Append(std::to_string(binIdx))
        .Append(Platform::DOT)
        .Append(BIN_SERIALIZATION_EXTENSION);

    return (retpath);
}

void Collection::PersistAll()
{
    // Persist configuration
    // ...

    std::for_each(bins.begin(), bins.end(),
        [this](const Bin<Z2raw>* bin)
    {
        SerializeBin(bin);
    });
}

void Collection::DeserializeAll()
{
    for (uint32 idx = 0;; ++idx)
    {
        Path path = ComposeBinSerializedPath(idx);
        if (!path.Exists())
            break;
        auto bin = DeserializeBin(path);
        grow(bin);
    }
    // ....
}

void Collection::SerializeBin(const Bin<Z2raw> * bin)
{
    uint32 binIdx = bin->binIdx();
    uint64 binByteSize = bin->binByteSize();
    const BinCore<Z2raw> * bincore = bin->Get();
    uint32 elemsToCopy = bincore->s_nFreeElemIdx;
    uint32 elemsSize = static_cast<uint32>(bincore->s_vElems.size());

    if (elemsToCopy >= elemsSize)
    {
        std::stringstream msg;
        msg << "Collection " << m_cfgi.name << ".\n";
        msg << "Error serializing Bin " << binIdx << ". s_nFreeElemIdx (" << elemsToCopy;
        msg << ") is not with size of elems: " << elemsSize;
        throw std::exception(msg.str().c_str());
    }

    Path binSerializedPath = ComposeBinSerializedPath(bin->binIdx());
    PercyFS percy(binSerializedPath);

    Percy::HandleUint64 handleSize = percy.PersistPromiseUint64();
    percy.PersistUint32(binIdx);
    percy.PersistUint64(binByteSize);
    percy.PersistUint32(elemsToCopy);
    percy.PersistUint32(elemsSize);
    percy.PersistUint64(bin->x_nNumActive);
    percy.PersistUint64(bin->x_nNumDeleted);

    std::for_each(bincore->s_vElems.cbegin(), bincore->s_vElems.cbegin() + elemsToCopy,
        [&percy] (const ElemInfo & elem)
    {
        percy.PersistUint64(elem);
    });

    percy.PersistBlob(bincore->f_pRaw, binByteSize);

    uint64 cursize = percy.GetCurrentSize();

    percy.FullfillPromiseUint64(handleSize, cursize);
    percy.Flush();
}

Bin<Z2raw> *Collection::DeserializeBin(Path path)
{
    DepercyFS percy(path);

    uint64 binarysize = percy.DeserializeUint64();
    uint32 binIdx = percy.DeserializeUint32();
    uint64 binByteSize = percy.DeserializeUint64();
    uint32 elemsToCopy = percy.DeserializeUint32();
    uint32 elemsSize = percy.DeserializeUint32();
    uint64 nNumActive = percy.DeserializeUint64();
    uint64 nNumDeleted = percy.DeserializeUint64();

    BinCore<Z2raw> bincore(elemsSize);
    bincore.s_nFreeElemIdx.store(elemsToCopy);
    bincore.s_vElems.resize(elemsSize);

    bincore.f_pRaw = Core::Bin<Z2raw>::AllocateBinRaw(static_cast<uint32>(binByteSize));

    for (uint32 idx=0U; idx!=elemsToCopy; ++idx)
    {
        bincore.s_vElems[idx].set(percy.DeserializeUint64());
    }

    percy.DeserializeBlob(bincore.f_pRaw, binByteSize);

    if (percy.GetCurrentSize() != binarysize)
    {
        std::stringstream msg;
        msg << "Collection " << m_cfgi.name << ".\n";
        msg << "Error deserializing Bin " << binIdx << ". s_nFreeElemIdx (" << elemsToCopy;
        msg << ") is not with size of elems: " << elemsSize;
        throw std::exception(msg.str().c_str());
    }

    Bin<Z2raw> * bin = new Bin<Z2raw>(binIdx, binByteSize, std::move(bincore));
    bin->x_nNumActive = nNumActive;
    bin->x_nNumDeleted = nNumDeleted;
    return (bin);
}

tuple<bool, string> Collection::Compare(const Collection * coll1, const Collection * coll2)
{
    bool different = false;
    std::stringstream ss;

    if (coll1->GetNumBins() != coll2->GetNumBins())
    {
        ss << "Num bins is different: " << coll1->GetNumBins() << ", " << coll2->GetNumBins() << "\n";
        different = true;
        return (std::make_tuple(!different, ss.str()));
    }

    for (uint idx = 0; idx != coll1->GetNumBins(); ++idx)
    {
        auto ret = CompareBins(coll1->bins[idx], coll2->bins[idx]);
        different = (different || !std::get<0>(ret));
        ss << std::get<1>(ret);
    }

    return (std::make_tuple(!different, ss.str()));
}

tuple<bool, string> Collection::CompareBins(const Bin<Z2raw> * bin1, const Bin<Z2raw> * bin2)
{
    bool same = true;
    std::stringstream ss;

    bool diff_f_binSizeBytes = (bin1->f_binSizeBytes != bin2->f_binSizeBytes);
    bool diff_f_binSizeAtoms = (bin1->f_binSizeAtoms != bin2->f_binSizeAtoms);
    bool diff_x_nNumActive   = (bin1->x_nNumActive != bin2->x_nNumActive);
    bool diff_x_nNumDeleted  = (bin1->x_nNumDeleted != bin2->x_nNumDeleted);
    bool diff_f_binIdx       = (bin1->f_binIdx != bin2->f_binIdx);
    bool diff_s_nFreeElemIdx = (bin1->s_nFreeElemIdx != bin2->s_nFreeElemIdx);

    bool diff_Temp1 = (*(uint*) bin1->f_pRaw != *(uint*) bin2->f_pRaw);
    if (diff_Temp1)  { ss << "oops "; }

    bool diff_p_pRaw         = (0 != memcmp(bin1->f_pRaw, bin2->f_pRaw, std::min<uint64>(bin1->f_binSizeBytes, bin2->f_binSizeBytes)));

    if (diff_f_binSizeBytes)      { ss << "f_binSizeBytes: " << bin1->f_binSizeBytes << ", " << bin2->f_binSizeBytes << "\n"; same = false;   }
    if (diff_f_binSizeAtoms)      { ss << "f_binSizeAtoms: " << bin1->f_binSizeAtoms << ", " << bin2->f_binSizeAtoms << "\n"; same = false;   }
    if (diff_x_nNumActive)      { ss << "x_nNumActive: " << bin1->x_nNumActive << ", " << bin2->x_nNumActive << "\n"; same = false;   }
    if (diff_x_nNumDeleted)      { ss << "x_nNumDeleted: " << bin1->x_nNumDeleted << ", " << bin2->x_nNumDeleted << "\n"; same = false;   }
    if (diff_f_binIdx)      { ss << "f_binIdx: " << bin1->f_binIdx << ", " << bin2->f_binIdx << "\n"; same = false;   }
    if (diff_s_nFreeElemIdx)      { ss << "s_nFreeElemIdx: " << bin1->s_nFreeElemIdx << ", " << bin2->s_nFreeElemIdx << "\n"; same = false;   }
    if (diff_p_pRaw)      { ss << "diff_p_pRaw: " << diff_p_pRaw << "\n"; same = false;  }

    if (!diff_s_nFreeElemIdx)
    {
        for (uint64 idx = 0ULL; idx < bin1->s_nFreeElemIdx; ++idx)
        {
            if (bin1->s_vElems[idx] != bin2->s_vElems[idx])
            {
                ss << "s_vElems[" << idx << "] = " << bin1->s_vElems[idx] <<
                        ", " << bin2->s_vElems[idx] << "\n";
                same = false;
                break;
            }
        }
    }
    return (std::make_tuple(same, ss.str()));
}

bool Collection::ReleaseInsertBuffer(void * buffer)
{
    bool found = false;

    std::for_each(bins.begin(), bins.end(),
        [&found, buffer](Bin<Z2raw>* bin)
    {
        if (bin->contains(buffer))
        {
            bin->ReleaseBuffer(buffer);
            found = true;
        }
    });

    return (found);
}

void * Collection::AcquireInsertBuffer(uint32 sizeBytes)
{
    for (;;)
    {
        try {
            auto bin = bins.back();
            return (bin->AcquireBuffer(sizeBytes));
        }
        catch (BinFull & ex)
        {
            try
            {
                (void) ex;
                bool newvalue = true;
                bool expected = false;
                if (s_growing.compare_exchange_strong(expected, newvalue))
                {
                    grow();
                    s_growing.store(false);
                }
                else
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            catch (...)
            {
                std::stringstream ss;
                ss << "Collection " << name() << ": failed adding Bin. Current bin number is " << bins.size() << ". Insert failed.";
                LOG(ss.str());
                return nullptr;
            }
        }
    }
}

Collection * Collection::InstantiateBase(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp, bool deserialize)
{
    auto aligned_mem = _aligned_malloc(sizeof(Collection), CACHE_LINE);
    auto newcoll = new (aligned_mem) Collection(cfgi, cfgp, deserialize);
    return (newcoll);
}

Collection * Collection::Instantiate(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp)
{
    return (InstantiateBase(cfgi, cfgp, false));
}

Collection * Collection::DeserializeFromFile(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp)
{
    return (InstantiateBase(cfgi, cfgp, true));
}

Collection::Collection(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp, bool deserialize)
    : m_cfgi(cfgi),
    m_cfgp(cfgp),
    bins(cfgi.maxBinNum),
    m_percyCollectionBasePath(cfgp.basePath.Append(Platform::DIR_SEPARATOR)
                                           .Append(m_cfgi.name))
{
    static_assert(sizeof(ElemInfo) == 8, "sizeof ElemInfo is not what you think");

    //FILE_LOG(logINFO) << "Collection " << name << " started. MaxElems=" << binMaxElems << ", MaxSize=" << binMaxSize << ", maxBins=" << (uint32)maxBinNum;
    if (deserialize)
    {
        DeserializeAll();
    } else
    {
        grow();
    }
}

Collection::~Collection()
{
    for (auto bin : bins)
    {
        delete (bin);
    }
}

namespace
{
    auto AccumulatorNone = [](double &, double) {};
    auto AccumulatorSum = [](double & acc, double value)
    {
        acc += value;
    };
    auto AccumulatorCount = [](double & acc, double)
    {
        ++acc;
    };
    auto AccumulatorMin = [](double & acc, double value)
    {
        acc = std::min<double>(acc, value);
    };
    auto AccumulatorMax = [](double & acc, double value)
    {
        acc = std::max<double>(acc, value);
    };
}

std::map<QO, AccumulatorLambda> Collection::accumulators
{
    { QO::SUM,   lambdaToAll(AccumulatorSum) },
    { QO::COUNT, lambdaToAll(AccumulatorCount) },
    { QO::MIN,   lambdaToAll(AccumulatorMin) }
};

}
}




