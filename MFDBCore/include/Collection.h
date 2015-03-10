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

#include <ppl.h>
#include <numeric>
#include <set>
#include <vector>
#include <memory>
#include <unordered_map>

#include "LFT/LFTtypes.h"
#include "bin.h"
#include "z2types.h"
#include "MemFusion/TimeStamp.h"
#include "MemFusion/LF/bvec.h"
#include "Filter.h"
#include "LFT/LFT.h"
#include "LFT/AT.h"
#include "Perfy.h"
#include "Z2Query.h"
#include "QueryContext.h"
#include "MemFusion/Platform/FileSystem.h"
#include "MemFusion/Percy.h"


namespace MFDB
{
namespace Core
{
using namespace MemFusion;
using namespace MemFusion::LF;
using std::tuple;

typedef std::set<Z2name> Projections;

class align_deleter
{
public:
    template <typename T>
    void operator() (T * p)
    {
        _aligned_free(p);
    }
};

typedef Z2name Z2TargetName;
typedef Z2name Z2AccName;
typedef Z2raw Z2Group;

struct CompGroupRaw
{
    bool operator () (const Z2raw & left, const Z2raw & right)
    {
        Z2 zleft(left);
        Z2 zright(right);
        auto vlenleft = zleft.z2vlen();
        auto vlenright = zright.z2vlen();
        if ((vlenleft == 0) && (vlenright == 0))
        {
            return (zleft.z2value() < zright.z2value());
        }
        if (vlenleft == 0) return true;
        if (vlenright == 0) return false;

        // both are short
        return (zleft.z2value() < zright.z2value());
    }
};

typedef std::function<void(cveciter<NV>, cveciter<NV>, std::map<Z2Group, double, CompGroupRaw> &)> AccumulatorLambda;

class lambdaToAll
{
public:
    typedef std::function<void(double &, double)> T;
private:
    T lambda;
public:
    lambdaToAll(T l)
        : lambda(l)
    {
    }
    void operator () (cveciter<NV> begin, cveciter<NV> end, std::map<Z2Group, double, CompGroupRaw> & stage2)
    {
        for (auto iter = begin; iter != end; ++iter)
        {
            const NV & nv = *iter;
            auto groupiter = stage2.find(nv.Group());
            if (groupiter != stage2.end()) {
                lambda(groupiter->second, nv.AccValue());
            }
            else {
                stage2[nv.Group()] = nv.AccValue();
            }
        }
    }
};

struct CollectionIntrinsicCfg
{
    std::string name;
    uint32 binMaxElems;
    uint64 binMaxSize;
    uint32 maxBinNum;

    explicit CollectionIntrinsicCfg(std::string n, uint32 bme, uint64 bms, uint32 mbn)
        : name(n),
        binMaxElems(bme),
        binMaxSize(bms),
        maxBinNum(mbn)
    {}
private:
    CollectionIntrinsicCfg();
};

typedef const CollectionIntrinsicCfg cCollectionIntrinsicCfg;


class Collection
{
    enum PerfMetrics
    {
        Field_EQ_Value = 1,
    };

    static const char * BIN_SERIALIZATION_EXTENSION;

    std::atomic<bool> s_growing;
    void grow();
    void grow(Bin<Z2raw> * bin);

    cCollectionIntrinsicCfg m_cfgi;
    cCollectionPercyCfg     m_cfgp;
    Path                    m_percyCollectionBasePath;

    Path ComposeBinSerializedPath(cuint32 binIdx);

    // These are all relative to LFT queries
    //
    uint32 FindProjectPhase(std::vector<LFTStage3> & matchesPerBin, Buffer & retbuf, opt<Projections &> onames);
    uint32 FindProject(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin, opt<Projections &> onames);
    uint32 FindProjectSome(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin, bool projectId, Projections & names);
    uint32 FindProjectAll(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin);
    //

    INLINE static void AddDocDelimiter(Z2raw *& dstPtr)
    {
        *dstPtr++ = Z2({ BSONtypeCompressed::CMaxKey, 8 }, 0, 0, -1);
    }

    QueryMetrics lastQueryCounters;

    template <typename QC, typename T1 = QC::T1, typename T4 = QC::T4>
    std::unique_ptr<QC, align_deleter> CreateQueryProcessor(uint64 transId, Buffer & retbuf, const Z2Query<T1> * z2query, std::function<uint32 (xHandle)> decoder)
    {
        (void) transId;
        TimeStamp start;
        auto mem = _aligned_malloc(sizeof(QC), CACHE_LINE);
        std::unique_ptr<QC, align_deleter>
            queryCtx(new (mem) QC(z2query, bins, retbuf, STAGE1_ELEMS_PER_THREAD, decoder));
        TimeStamp preparation;

        queryCtx->metrics.prepare_us = TimeStamp::millis(start, preparation);
        return (std::move(queryCtx));
    }
private:
    // Ctor is priate because we instantiate with static Instantiate()
    Collection(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp, bool deserialize = false);

    const std::string & name() const { return m_cfgi.name; }

    void SerializeBin(const Bin<Z2raw> * bin);
    Bin<Z2raw> * DeserializeBin(Path path);
    static std::tuple<bool, std::string> CompareBins(const Bin<Z2raw> *, const Bin<Z2raw> *);

    static Collection * InstantiateBase(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp, bool deserialize = false);
    void DeserializeAll();

public:
    ~Collection();

    static Collection * Instantiate(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp);
    static Collection * DeserializeFromFile(cCollectionIntrinsicCfg & cfgi, cCollectionPercyCfg & cfgp);

    static std::tuple<bool, std::string> Compare(const Collection * coll1, const Collection * coll2);

    void PersistAll();

    QueryMetrics GetLastQueryCounters() const
    {
        return (lastQueryCounters);
    }

    enum MongoDB : uint32
    {
        MAX_DOCUMENT_SIZE = 16*1024*1024,
    };

    uint32 FindAndReturnAll(uint64 transId, Buffer & retbuf, const Z2FindQuery * z2query);
    uint32 FindAndProject(uint64 transId, opt<Projections&> onames, Buffer & retbuf, const Z2FindQuery * z2query);

    uint32 Aggregate(uint64 transId, Buffer & retbuf, const Z2AggrQuery * z2query);

    bool ReleaseInsertBuffer(void * buffer);

    void * AcquireInsertBuffer(uint32 sizeBytes);

    uint32 GetNumBins() const { return static_cast<uint32>(bins.size()); }

private:
    LF::bvec<Bin<Z2raw>*> bins;

    static std::map<QO, AccumulatorLambda> accumulators;

    //map<Z2Group, map<Z2AccName, double>> stage3_data;
    // _id = Group:  vlen and value
    //  for each elem in inner map:
    //    - name=Z2AccName, value=double
    template <typename TAcc,
        typename MapOfSortedVec = std::map<Z2raw, std::vector<std::pair<Z2name, TAcc>>>>
        uint32 WriteAggregationOutput(const MapOfSortedVec & data, Buffer & retbuf)
    {
        Z2raw * pstart = static_cast<Z2raw*>(retbuf.get());
        Z2raw * dstPtr = pstart+1;
        uint64 doccount = 0;

        for (auto iter1 = data.cbegin(); iter1 != data.cend(); ++iter1)
        {
            Z2 zgroup(iter1->first);
            Z2 zid(zgroup.z2typeinfo(), MFDB::Constants::Id_1, zgroup.z2value());
            *dstPtr++ = zid;

            for (auto iter2 = iter1->second.cbegin(); iter2 != iter1->second.cend(); ++iter2)
            {
                Z2typeinfo ti = { Z2type(BSONtypeCompressed::CFloatnum), 8 };
                uint64 uintvalue = *(const uint64*) &iter2->second;
                Z2 acc(ti, iter2->first, uintvalue);
                *dstPtr++ = acc;
            }
            AddDocDelimiter(dstPtr);
            ++doccount;
        }

        *pstart = Z2({ BSONtypeCompressed::CArrayDoc, 0 }, 0, doccount, -1);
        uint32 ret = ((uint32) std::distance(pstart, dstPtr))-1;
        return (ret);
    }
};


void FindProcessData(uint32 numLFTs, Stage2 & stage2PerBin, const Z2FindQuery & z2query, LFTStage3 & matchesPerBin);



} // namespace MFDB::Core

} // namespace MFDB

