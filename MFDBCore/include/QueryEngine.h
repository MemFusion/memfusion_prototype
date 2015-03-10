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

#include <map>
#include <string>
#include "MemFusion/types.h"
#include "MemFusion/LF/bvec.h"
#include "MemFusion/LF/smallmap.h"
#include "Collection.h"
#include "StrangeTypes.h"
#include "Candle.h"

namespace MFDB
{
using namespace MemFusion;

class QueryEngine
{
public:
    static cuint32 MongoRetBufferSize = 16 * 1024 * 1024;

    struct Config
    {
        uint32 m_maxConcIB;
        uint32 m_binMaxElems;
        uint32 m_binMaxSize;
        uint32 m_maxBinNum;
        Path   m_percyBasePath;

        Config(uint32 mcib, uint32 bme, uint32 bms, uint32 mbn, Path pbp)
            : m_maxConcIB(mcib),
            m_binMaxElems(bme),
            m_binMaxSize(bms),
            m_maxBinNum(mbn),
            m_percyBasePath(pbp)
        {}
    private:
        Config();
    };

private:
    static QueryEngine * m_instance;
    static std::atomic<bool> m_initializing;

    // TBD: read from configuration
    static const int OUTPUT_BVEC_SIZE_PER_BIN = 10 * 1000;
    static const int MAX_BIN_NUMBER = 32;
    static const int MAX_NUMBER_OF_COLLECTIONS = 1000;

    Config cfg;
    LF::smallmap<std::string, Core::Collection *> m_collections;
    //LF::smallmap<Candle, Slow::set<uint32>> m_clientsActiveTrans;

    std::vector<bvec<uint32>> outputs;

    QueryEngine(Config cfg);  // uint32 maxConcIB, uint32 bmaxelems, uint32 bmaxsize, uint8 maxbins
    QueryEngine(const QueryEngine &);
    void operator = (const QueryEngine &);

    static Core::Projections ExtractProjections(std::vector<Z2raw>);
public:
    static QueryEngine * Instance()
    {
        return (m_instance);
    }

    static void InitializeQueryEngine(uint32 maxConcIB, uint32 bmaxelems, uint32 bmaxsize, uint32 maxbins, Path percypath);

    void * AcquireInsertBuffer(Candle, const std::string & collection, uint32 size);

    bool ReleaseBufferForInsert(Candle, const char * collection, void * buffer);

    uint32 Query_Find(uint64 ch, const char * collection, void * z2selector, uint32 selectBytes, void * z2query, uint32 lftBytes, uint32 qpBytes, void * retbuf);

    uint32 Query_Aggregate(uint64 ch, const char * collection, void * z2query, uint32 queryBytes, void * retbuf, uint32 uintsort);
};

}

#define QE QueryEngine::Instance()

