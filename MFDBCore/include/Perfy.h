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

#include "MemFusion/LF/bvec.h"
#include "MemFusion/TimeStamp.h"
#include <array>
#include <memory>


namespace MemFusion
{
using namespace MemFusion::LF;

struct AtomsMetrics
{
    uint64 nTotalAtoms;
    uint64 nTotalElems;
    uint64 nElapsedMS;
};

class Perfy
{
public:
    typedef AtomsMetrics Metrics;

private:
    typedef std::tuple<uint64, uint64, uint64> datatype;
    static const int NumMetrics = 10;
    //std::array<bvec<datatype>, NumMetrics> data;

    // cache line ???
    std::atomic<uint64> s_supportTotalAtoms;
    std::atomic<uint64> s_supportTotalElems;
    std::shared_ptr<TimeStamp> plastStart;

    Perfy() 
    {
    }
    Perfy(const Perfy &);

    static Perfy * m_instance;
public:
    template <int N>
    void add(uint64 transId, uint64 numElems, uint64 numAtoms)
    {
        (void) transId;
        //data[N].add(std::make_tuple(transId, numElems, numAtoms));
        s_supportTotalAtoms += numAtoms;
        s_supportTotalElems += numElems;
    }

    template <>
    void add<12>(uint64, uint64, uint64)
    {
        abort();
    }

    void StartMetrics()
    {
        plastStart.reset(new TimeStamp());
        s_supportTotalAtoms.store(0);
        s_supportTotalElems.store(0);
    }

    Metrics GetAndResetMetrics()
    {
        TimeStamp end;
        uint64 deltaMS = TimeStamp::millis(*plastStart, end);
        Metrics ret;
        ret.nElapsedMS = deltaMS;
        ret.nTotalAtoms = s_supportTotalAtoms.exchange(0ULL);
        ret.nTotalElems = s_supportTotalElems.exchange(0ULL);
        plastStart.reset(new TimeStamp());
        return std::move(ret);
    }

    static void Initialize()
    {
        if (m_instance)
            throw std::exception("Perfy already initialized.");
        auto aligned_mem = _aligned_malloc(sizeof(Perfy), CACHE_LINE);
        m_instance = new (aligned_mem) Perfy();
    }

    static Perfy & Instance()
    {
        return (*m_instance);
    }
};

}
