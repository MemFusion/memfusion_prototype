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

#include "MFDBCore/include/Collection.h"
#include <stdio.h>
#include <atomic>
#include <thread>
#include <ppl.h>
#include <numeric>

typedef unsigned int uint;

using namespace MFDB;
using namespace MFDB::Core;
using namespace MemFusion;

static const char * TestTempPath = "C:\\Temp";

__m128i H_Z1name(uint64 low, uint64 high)
{
    __m128i ret;
    ret.m128i_u64[0] = low;
    ret.m128i_u64[1] = high;
    return (ret);
}
void Slow_Write_to_Collection(Collection & coll, void * buffer, uint32 sizeBytes)
{
    auto ib = coll.AcquireInsertBuffer(sizeBytes);
    memcpy(ib, buffer, sizeBytes);
    coll.ReleaseInsertBuffer(ib);
}

typedef std::tuple<uint64, uint64, uint64, uint64, uint64, uint64> PerfState;

void Test_QP_AND2()
{
    printf("\nTest: QP AND\n");

    Collection & coll = *Collection::Instantiate(CollectionIntrinsicCfg("test", 1000 * 1000, 100 * 1024 * 1024, 10),
        CollectionPercyCfg(Path(TestTempPath), PercyTraits::PersistencyType::LocalFileSystem));

    Z2name namestart = 10;
    cuint32 NUM_ELEMS = 1000 * 1000;
    cuint32 ATOMS_PER_ELEM = 30;
    Z2typeinfo a = { Z2type(BSONtypeCompressed::CFloatnum), 8 };
    Z2 elemfirst(a, namestart, 444448888ULL);
    Z2typeinfo typeb = { Z2type(BSONtypeCompressed::CUTF8String), 3 };
    Z2 elemlast(typeb, namestart, 4444499ULL);
    Z2 elemwrong(a, namestart - 1, 444448888ULL);
    __m128i temp = elemfirst;
    Slow_Write_to_Collection(coll, &temp, sizeof(__m128i));

    for (int i = 0; i < NUM_ELEMS - 1; ++i) {
        Z2name name = namestart + i;
        if (name != elemwrong.z2name())
        {
            Z2 z2(a, name, 444448888ULL);
            __m128i temp[ATOMS_PER_ELEM];
            for (int j = 0; j < ATOMS_PER_ELEM; ++j)
            {
                temp[j] = z2;
            }
            Slow_Write_to_Collection(coll, &temp, ATOMS_PER_ELEM*sizeof(__m128i));
        }
        else
        {
            printf("ooops...\n");
        }
    }
    temp = elemlast;
    Slow_Write_to_Collection(coll, &temp, sizeof(__m128i));

    printf("Collection has %u elems, %u bin\n", NUM_ELEMS, coll.GetNumBins());

    std::vector<uint> testCores = { 1, 4, 4, 4, 4, 4, 4, 4, 4, 4 };
    uint64 transId = 1234;

    LFTraw lfts5[2];
    lfts5[0].idx = 0;
    lfts5[0].qo = QO::NE;
    lfts5[0].pad = 0;
    lfts5[1].idx = 1;
    lfts5[1].qo = QO::GTE;
    lfts5[1].pad = 0;

    Z2 elemother5(a, namestart + NUM_ELEMS - 3, 444448888ULL);
    Z2 elembig(a, namestart + NUM_ELEMS - 3, 444448888ULL);
    lfts5[0].z2raw = elemother5;
    lfts5[1].z2raw = elembig;

    QPraw qps5[3];
    qps5[1].command = QO::AND;
    qps5[1].kids = 2;
    Z2FindQuery queryFilter5(std::vector<LFTraw>(lfts5, &lfts5[2]), std::vector<QPraw>(qps5, &qps5[3]));

    std::vector<QueryMetrics> perfCounters;
    //cuint32 N_TIMES = 1;

    Perfy::Instance().StartMetrics();
    for (uint numCores : testCores)
    {
        TimeStamp start;

        perfCounters.clear();
        perfCounters.reserve(1000);
        uint32 iterations = 0;
        PerfState zeros4 = std::make_tuple(0ULL, 0ULL, 0ULL, 0ULL, 0ULL, 0ULL);

        try {
            int SIZE = 1024 * 1024;
            void * retbuf = (void*) new byte[SIZE];
            Buffer buffer(retbuf, SIZE);
            uint32 docsReturned;

            //for (int times = 0; times < N_TIMES; ++times)
            {
                // NE : *should* not be there
                auto numz2returned = coll.FindAndReturnAll(transId, buffer, &queryFilter5);
                docsReturned = numz2returned / (ATOMS_PER_ELEM + 1);
                if (docsReturned > 0)
                    throw std::exception("test 1e failed.");
                perfCounters.push_back(coll.GetLastQueryCounters());

                ++iterations;
            }

            TimeStamp end;

            uint64 mymillis = TimeStamp::millis(start, end);

            PerfState totals =
                std::accumulate(std::begin(perfCounters),
                std::end(perfCounters),
                zeros4,
                [](PerfState state, const QueryMetrics & qc) -> PerfState
            {
                return (std::make_tuple(
                    std::get<0>(state) +qc.prepare_us,
                    std::get<1>(state) +qc.lfts_us,
                    std::get<2>(state) +qc.composer_us,
                    std::get<3>(state) +qc.project_us,
                    std::get<4>(state) +qc.composerIterations,
                    std::get<5>(state) +qc.queueWait_ms
                    ));
            });

            auto metrics1 = Perfy::Instance().GetAndResetMetrics();
            printf("%u cores: Metrics: %llu atoms %llu elems %llu ms.... about %u Matoms/sec, %u MB/sec (%llu mymillis)\n",
                numCores,
                metrics1.nTotalAtoms,
                metrics1.nTotalElems,
                metrics1.nElapsedMS,
                static_cast<uint32>(metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                static_cast<uint32>(Z2::size() * metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                mymillis);
            printf("  prepare=%llums, lfts=%llums, composer=%llums, project=%llums, avgCompIters=%llu, queueWait=%llums\n",
                std::get<0>(totals) / iterations,
                std::get<1>(totals) / iterations,
                std::get<2>(totals) / iterations,
                std::get<3>(totals) / iterations,
                std::get<4>(totals) / iterations,
                std::get<5>(totals) / iterations
                );
        }
        catch (std::exception & ex)
        {
            printf("Exception '%s' during test with %u cores.\n", ex.what(), numCores);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

//void Test_Aggregate1() {}

void Test_Aggregate1()
{
    printf("\nTest: aggregate1\n");

    Collection & coll = *Collection::Instantiate(CollectionIntrinsicCfg("test", 1000 * 1000, 100 * 1024 * 1024, 10),
        CollectionPercyCfg(Path(TestTempPath), PercyTraits::PersistencyType::LocalFileSystem));

    Z2name targetName = 2050;
    Z2name accName    = 3050;
    Z2name groupName  = 4050;

    Aggr1 aggr1 = { targetName, accName, QO::SUM };

    Z2name name = 1110;
    cuint32 NUM_ELEMS = 1000 * 1000;
    //cuint32 ATOMS_PER_ELEM = 30;
    Z2typeinfo typeCount = { Z2type(BSONtypeCompressed::CFloatnum), 8 };
    Z2typeinfo typeGroup = { Z2type(BSONtypeCompressed::CUTF8String), 3 };
    Z2typeinfo typeOther = { Z2type(BSONtypeCompressed::CInt32), 2 };

    std::vector<uint64> groups = { 0x006464 }; // 0x4545, 0x4646, 0x4747, 0x4848, 0x4949, 0x5050
    uint64 totalSum = 0ULL;

    for (int i = 0; i < NUM_ELEMS - 1; ++i) {
        auto value = 2;

        std::vector<Z2> elems =
        {
            Z2(typeOther, name + 10, 10),
            Z2(typeGroup, groupName, groups[i % groups.size()]),
            Z2(typeOther, name + 11, 11),
            Z2(typeCount, accName, value),
            Z2(typeOther, name + 12, 12),
            Z2(typeOther, name + 13, 13),
            Z2(typeOther, name + 14, 14),
            Z2(typeOther, name + 15, 15),
            Z2(typeOther, name + 11, 16),
        };
        totalSum += value;
        Slow_Write_to_Collection(coll, &elems[0], (uint32)(elems.size()*sizeof(__m128i)));
    }

    printf("Collection has %u elems, %u bin\ntotalSum= %llu", NUM_ELEMS, coll.GetNumBins(), totalSum);

    std::vector<uint> testCores = { 1, 4, 4, 4, 4, 4, 4, 4, 4, 4 };
    uint64 transId = 1234;

    std::vector<Aggr1> aggrlist{ aggr1 };

    Z2AggrQuery z2query(groupName, aggrlist, 0);

    std::vector<QueryMetrics> perfCounters;
    cuint32 N_TIMES = 1;

    Perfy::Instance().StartMetrics();
    for (uint numCores : testCores)
    {
        TimeStamp start;

        perfCounters.clear();
        perfCounters.reserve(1000);
        uint32 iterations = 0;
        PerfState zeros4 = std::make_tuple(0ULL, 0ULL, 0ULL, 0ULL, 0ULL, 0ULL);

        try {
            int SIZE = 1024 * 1024;
            void * retbuf = (void*) new byte[SIZE];
            memset(retbuf, 0, SIZE);
            Buffer buffer(retbuf, SIZE);

            for (int times = 0; times < N_TIMES; ++times)
            {
                auto numz2returned = coll.Aggregate(transId, buffer, &z2query);
                if (numz2returned != 3)
                    throw std::exception("test Aggregation1 failed. Wrong number of z2 returned.");
                uint64 result = reinterpret_cast<Z2*>(retbuf)[2].z2value();
                double resdouble = *(double*)&result;
                if (resdouble != (double) totalSum)
                    throw std::exception("test Aggregation1 failed. Wrong numeric result.");
                perfCounters.push_back(coll.GetLastQueryCounters());
                ++iterations;
            }

            TimeStamp end;

            uint64 mymillis = TimeStamp::millis(start, end);

            PerfState totals =
                std::accumulate(std::begin(perfCounters),
                std::end(perfCounters),
                zeros4,
                [](PerfState state, const QueryMetrics & qc) -> PerfState
            {
                return (std::make_tuple(
                    std::get<0>(state) +qc.prepare_us,
                    std::get<1>(state) +qc.lfts_us,
                    std::get<2>(state) +qc.composer_us,
                    std::get<3>(state) +qc.project_us,
                    std::get<4>(state) +qc.composerIterations,
                    std::get<5>(state) +qc.queueWait_ms
                    ));
            });

            auto metrics1 = Perfy::Instance().GetAndResetMetrics();
            printf("%u cores: Metrics: %llu atoms %llu elems %llu ms.... about %u Matoms/sec, %u MB/sec (%llu mymillis)\n",
                numCores,
                metrics1.nTotalAtoms,
                metrics1.nTotalElems,
                metrics1.nElapsedMS,
                static_cast<uint32>(metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                static_cast<uint32>(Z2::size() * metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                mymillis);
            printf("  prepare=%llums, lfts=%llums, composer=%llums, project=%llums, avgCompIters=%llu, queueWait=%llums\n",
                std::get<0>(totals) / iterations,
                std::get<1>(totals) / iterations,
                std::get<2>(totals) / iterations,
                std::get<3>(totals) / iterations,
                std::get<4>(totals) / iterations,
                std::get<5>(totals) / iterations
                );
        }
        catch (std::exception & ex)
        {
            printf("Exception '%s' during test with %u cores.\n", ex.what(), numCores);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

void Test_FilterValue(bool serialize = false)
{
    printf("MFDBCoreTest :   Collection\n");

    Collection & coll = *Collection::Instantiate(CollectionIntrinsicCfg("test", 200 * 1000, 100 * 1024 * 1024, 10),
        CollectionPercyCfg(Path(TestTempPath), PercyTraits::PersistencyType::LocalFileSystem));

    Z2name namestart = 10;
    cuint32 NUM_ELEMS = 1 * 1000 * 1000;
    cuint32 ATOMS_PER_ELEM = 30;
    Z2typeinfo a = { Z2type(BSONtypeCompressed::CFloatnum), 8 };
    Z2 elemfirst(a, namestart, 444448888ULL);
    Z2typeinfo typeb = { Z2type(BSONtypeCompressed::CUTF8String), 3 };
    Z2 elemlast(typeb, namestart, 4444499ULL);
    Z2 elemwrong(a, namestart-1, 444448888ULL);
    __m128i temp = elemfirst;
    Slow_Write_to_Collection(coll, &temp, sizeof(__m128i));

    for (int i = 0; i < NUM_ELEMS - 1; ++i) {
        Z2name name = namestart + i;
        if (name != elemwrong.z2name())
        {
            Z2 z2(a, name, 444448888ULL);
            __m128i temp[ATOMS_PER_ELEM];
            for (int j = 0; j < ATOMS_PER_ELEM; ++j)
            {
                temp[j] = z2;
            }
            Slow_Write_to_Collection(coll, &temp, ATOMS_PER_ELEM*sizeof(__m128i));
        }
        else
        {
            printf("ooops...\n");
        }
    }
    temp = elemlast;
    Slow_Write_to_Collection(coll, &temp, sizeof(__m128i));

    printf("Collection has %u elems, %u bin\n", NUM_ELEMS, coll.GetNumBins());

    Collection * coll2 = nullptr;
    if (serialize)
    {
        coll.PersistAll();
        coll2 = Collection::DeserializeFromFile(CollectionIntrinsicCfg("test", 200 * 1000, 100 * 1024 * 1024, 10),
            CollectionPercyCfg(Path(TestTempPath), PercyTraits::PersistencyType::LocalFileSystem));
        auto ret = Collection::Compare(&coll, coll2);
        if (!std::get<0>(ret))
        {
            printf("Serialization/Deserialization failed!!\n%s", std::get<1>(ret).c_str());
            exit(-1);
        }
        printf("Serialization/Deserialization succeded.\n");
    }
    else
    {
        coll2 = &coll;
    }

    std::vector<uint> testCores = { 1, 4, 4, 4, 4, 4, 4, 4, 4, 4 };
    uint64 transId = 1234;

    QPraw qps[2];
    LFTraw lfts1 [1];
    lfts1[0].idx = 0;
    lfts1[0].qo = QO::EQ;
    lfts1[0].pad = 0;
    lfts1[0].z2raw = elemlast;
    Z2FindQuery queryFilter1(std::vector<LFTraw>(lfts1, &lfts1[1]), std::vector<QPraw>(qps, &qps[2]));

    LFTraw lfts2[1];
    lfts2[0].idx = 0;
    lfts2[0].qo = QO::EQ;
    lfts2[0].pad = 0;
    lfts2[0].z2raw = elemwrong;
    Z2FindQuery queryFilter2(std::vector<LFTraw>(lfts2, &lfts2[1]), std::vector<QPraw>(qps, &qps[2]));

    LFTraw lfts3[1];
    lfts3[0].idx = 0;
    lfts3[0].qo = QO::EQ;
    lfts3[0].pad = 0;
    lfts3[0].z2raw = elemfirst;
    Z2FindQuery queryFilter3(std::vector<LFTraw>(lfts3, &lfts3[1]), std::vector<QPraw>(qps, &qps[2]));

    LFTraw lfts4[1];
    lfts4[0].idx = 0;
    lfts4[0].qo = QO::GTE;
    lfts4[0].pad = 0;
    Z2 elembig(a, namestart + NUM_ELEMS - 3, 444448888ULL);
    lfts4[0].z2raw = elembig;
    Z2FindQuery queryFilter4(std::vector<LFTraw>(lfts4, &lfts4[1]), std::vector<QPraw>(qps, &qps[2]));

    LFTraw lfts5[2];
    lfts5[0].idx = 0;
    lfts5[0].qo = QO::NE;
    lfts5[0].pad = 0;
    lfts5[1].idx = 1;
    lfts5[1].qo = QO::GTE;
    lfts5[1].pad = 0;

    Z2 elemother5(a, namestart + NUM_ELEMS - 3, 444448888ULL);
    lfts5[0].z2raw = elemother5;
    lfts5[1].z2raw = elembig;

    QPraw qps5[3];
    qps5[1].command = QO::AND;
    qps5[1].kids = 2;
    Z2FindQuery queryFilter5(std::vector<LFTraw>(lfts5, &lfts5[2]), std::vector<QPraw>(qps5, &qps5[3]));


    std::vector<QueryMetrics> perfCounters;
    //cuint32 N_TIMES = 1;

    Perfy::Instance().StartMetrics();
    for (uint numCores : testCores)
    {
        TimeStamp start;

        perfCounters.clear();
        perfCounters.reserve(1000);
        uint32 iterations = 0;
        PerfState zeros4 = std::make_tuple(0ULL, 0ULL, 0ULL, 0ULL, 0ULL, 0ULL);

        try {
            int SIZE = 1024 * 1024;
            void * retbuf = (void*) new byte[SIZE];
            Buffer buffer(retbuf, SIZE);
            uint32 docsReturned;

            //for (int times = 0; times < N_TIMES; ++times)
            {
                // looking for elemLast: *should* be there
                if (!coll.FindAndReturnAll(transId, buffer, &queryFilter1))
                    throw std::exception("test 1a failed.");

                if (!coll2->FindAndReturnAll(transId, buffer, &queryFilter1))
                    throw std::exception("test 1a failed.");
                perfCounters.push_back(coll2->GetLastQueryCounters());

                // looking for elemwrong: should *not* be there
                if (coll2->FindAndReturnAll(transId, buffer, &queryFilter2))
                    throw std::exception("test 1b failed.");
                perfCounters.push_back(coll2->GetLastQueryCounters());

                // looking for elemFirst: *should* be there
                if (!coll2->FindAndReturnAll(transId, buffer, &queryFilter3))
                    throw std::exception("test 1c failed.");
                perfCounters.push_back(coll2->GetLastQueryCounters());

                // looking for *big* elements: *should* be there
                auto numz2returned = coll2->FindAndReturnAll(transId, buffer, &queryFilter4);
                docsReturned = numz2returned / (ATOMS_PER_ELEM + 1);
                if (docsReturned == 0)
                    throw std::exception("test 1d failed.");
                perfCounters.push_back(coll2->GetLastQueryCounters());

                ++iterations;
            }

            TimeStamp end;

            uint64 mymillis = TimeStamp::millis(start, end);

            PerfState totals =
                std::accumulate(std::begin(perfCounters),
                std::end(perfCounters),
                zeros4,
                [](PerfState state, const QueryMetrics & qc) -> PerfState
             {
                return (std::make_tuple(
                    std::get<0>(state) + qc.prepare_us,
                    std::get<1>(state) + qc.lfts_us,
                    std::get<2>(state) + qc.composer_us,
                    std::get<3>(state) + qc.project_us,
                    std::get<4>(state) + qc.composerIterations,
                    std::get<5>(state) + qc.queueWait_ms
                    ));
             });

            auto metrics1 = Perfy::Instance().GetAndResetMetrics();
            printf("%u cores: Metrics: %llu atoms %llu elems %llu ms.... about %u Matoms/sec, %u MB/sec (%llu mymillis)\n",
                numCores,
                metrics1.nTotalAtoms,
                metrics1.nTotalElems,
                metrics1.nElapsedMS,
                static_cast<uint32>(metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                static_cast<uint32>(Z2::size() * metrics1.nTotalAtoms / (1000 * metrics1.nElapsedMS)),
                mymillis);
            printf("  prepare=%llums, lfts=%llums, composer=%llums, project=%llums, avgCompIters=%llu, queueWait=%llums\n",
                std::get<0>(totals) / iterations,
                std::get<1>(totals) / iterations,
                std::get<2>(totals) / iterations,
                std::get<3>(totals) / iterations,
                std::get<4>(totals) / iterations,
                std::get<5>(totals) / iterations
                );
        }
        catch (std::exception & ex)
        {
            printf("Exception '%s' during test with %u cores.\n", ex.what(), numCores);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}


