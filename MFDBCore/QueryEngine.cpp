// Copyright (c) 2014-2015 Benedetto Proietti
//

#include "stdafx.h"
#include <malloc.h>
#include <assert.h>

#include "QueryEngine.h"
#include "Collection.h"
#include "MemFusion/RetryTimes.h"
#include "MemFusion/Logger.h"
#include "LFT/LFT.h"

using namespace MFDB;
using namespace std;
using namespace MemFusion;

Perfy * Perfy::m_instance = nullptr;

QueryEngine * QueryEngine::m_instance = nullptr;
std::atomic<bool> QueryEngine::m_initializing = false;

void QueryEngine::InitializeQueryEngine(uint32 maxConcIB, uint32 bmaxelems, uint32 bmaxsize, uint32 maxbins, Path percypath)
{
    Logger::Initialize("BE");

    LOG("Logger initialized.");

    bool expected = false;
    bool desired = true;
    if (!m_initializing.compare_exchange_strong(expected, desired, std::memory_order_release,
        std::memory_order_relaxed))
    {
        return;
    }
    auto aligned_mem = _aligned_malloc(sizeof(QueryEngine), CACHE_LINE);
    Config cfg = { maxConcIB, bmaxelems, bmaxsize, maxbins, percypath };
    auto temp = new (aligned_mem) QueryEngine(cfg);
    m_instance = temp;

    Perfy::Initialize();
}

// Obviously re-entrant 
void * QueryEngine::AcquireInsertBuffer(Candle ch, const std::string & collection, uint32 size)
{
    (void) ch;
    void * buffer = nullptr;
    try
    {
        auto iter = m_collections.findinsert(collection,
                [this, collection]() -> Core::Collection *
        {
            Core::cCollectionIntrinsicCfg cfgi(collection, cfg.m_binMaxElems, cfg.m_binMaxSize, cfg.m_maxBinNum);
            Core::cCollectionPercyCfg cfgp(cfg.m_percyBasePath, PercyTraits::PersistencyType::LocalFileSystem);
            return (Core::Collection::Instantiate(cfgi, cfgp));
        });

        buffer = iter->AcquireInsertBuffer(size);
    }
    catch (std::exception & ex)
    {
        std::stringstream ss;
        ss << "std::exception in " << __FUNCTION__ << " collection=" << collection;
        ss << " '" << ex.what() << " '";
        LOG(ss.str());
    }
    catch (...)
    {
        std::stringstream ss;
        ss << "Unknown exception in " << __FUNCTION__ << " collection=" << collection;
        LOG(ss.str());
    }

    return (buffer);
}

using namespace Core;

// Obviously re-entrant 
bool QueryEngine::ReleaseBufferForInsert(Candle ch, const char * collection, void * buffer)
{
    (void) ch;

    try {
        auto optiter = m_collections.find(string(collection));
        if (optiter.is_initialized())
        {
            return (optiter.get()->ReleaseInsertBuffer(buffer));
        }
    }
    catch (ReleaseBufferError e)
    {
        std::stringstream ss;
        ss << "ReleaseBufferError exception in " << __FUNCTION__ << " collection=" << collection;
        ss << " buffer=" << buffer << " elemIdx=" << e.elemIdx;
        LOG(ss.str());
    }
    catch (...)
    {
        std::stringstream ss;
        ss << "Unknown exception in " << __FUNCTION__ << " collection=" << collection;
        ss << " buffer=" << buffer;
        LOG(ss.str());
    }
    return (false);
}

uint32 QueryEngine::Query_Aggregate(uint64 ch, const char * collection, void * z2query, uint32 queryBytes, void * retbuf, uint32 uintsort)
{
    (void) ch, queryBytes, retbuf, z2query;
    // z2name + List(z2name z2name qo)   4+ Nx(4+4+4)
    cuint32 size1Agg = 2 * sizeof(Z2name) + sizeof(QO);
    (void) size1Agg;
    assert(queryBytes >= sizeof(Z2name) + size1Agg);
    assert(((queryBytes - sizeof(Z2name)) % size1Agg) == 0);

    auto optiter = m_collections.find(string(collection));
    if (optiter.is_initialized())
    {
        auto query_end = ((byte*) z2query) + queryBytes;
        auto query_start = ((byte*) z2query) + sizeof(Z2name);
        std::vector<Aggr1> aggrlist((Aggr1*) query_start, (Aggr1*) query_end);

        Z2AggrQuery z2query(*(Z2name*) z2query, aggrlist, uintsort);

        auto iter = optiter.get();
        uint64 transId = 0ULL;
        Buffer buffer(retbuf, MongoRetBufferSize);
        return (iter->Aggregate(transId, buffer, &z2query));
    }

    return (0);
}

uint32 QueryEngine::Query_Find(uint64 ch, const char * collection, void * z2selector, uint32 selectBytes, void * z2queryraw, uint32 lftBytes, uint32 qpBytes, void * retbuf)
{
    (void) ch, z2selector, retbuf, z2queryraw, selectBytes, collection, lftBytes, qpBytes;
    assert(lftBytes >= sizeof(Z2raw));

    auto optiter = m_collections.find(string(collection));
    if (optiter.is_initialized())
    {
        auto iter = optiter.get();
        uint64 transId = 0ULL;
        auto lft_end = ((byte*) z2queryraw) + lftBytes;
        auto all_end = lft_end + qpBytes;

        std::vector<LFTraw> lfts_raw((LFTraw*) z2queryraw, (LFTraw*) lft_end);
        std::vector<QPraw> qps_raw((QPraw*) lft_end, (QPraw*) all_end);

        Z2FindQuery z2query(lfts_raw, qps_raw);
        Buffer buffer(retbuf, MongoRetBufferSize);

        if (selectBytes == 0)
        {
            return (iter->FindAndReturnAll(transId, buffer, &z2query));
        }
        else
        {
            std::vector<Z2raw> z2sels((Z2raw*) z2selector, (Z2raw*) z2selector + selectBytes / sizeof(Z2raw));
            std::set<Z2name> names = ExtractProjections(z2sels);
            return (iter->FindAndProject(transId, names, buffer, &z2query));
        }
    }

    return (0);
}

Core::Projections QueryEngine::ExtractProjections(std::vector<Z2raw> sels)
{
    std::set<Z2name> ret;

    std::for_each(std::begin(sels), std::end(sels),
        [&ret](Z2raw raw)
    {
        Z2 z2(raw);
        Z2name name = z2.z2name();
        ret.insert(name);
    });

    return (std::move(ret));
}

QueryEngine::QueryEngine(Config cfg_)
    : cfg(cfg_),
    outputs(MAX_BIN_NUMBER, OUTPUT_BVEC_SIZE_PER_BIN),
    m_collections(MAX_NUMBER_OF_COLLECTIONS)
{
}
