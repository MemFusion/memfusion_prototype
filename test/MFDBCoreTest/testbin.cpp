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


#include "MFDBCore/include/bin.h"
#include <stdio.h>
#include <atomic>
#include <thread>
#include <ppl.h>
#include <numeric>


typedef unsigned int uint;


using namespace MFDB;
using namespace MFDB::Core;


void reader(volatile long & shuttingDown, Bin<Z2raw> * pmybin, uint thdIdx)
{
    (void) thdIdx;
    uint iter = 0;
    while (shuttingDown != 1)
    {
        auto ret = pmybin->Get();
        auto raw = ret->f_pRaw;
        const ElemListType & elems = ret->s_vElems;
        uint numElems = ret->s_nFreeElemIdx;
        uint maxAtomIdx = elems[0].atomIdx();

        if ((++iter % 1000) == 0)
        {
            uint idx = rand() % ret->s_nFreeElemIdx.load();
            pmybin->DisableElem(idx);
           // printf("\n\n--------- Thread %u disabled elem idx %u\n", thdIdx, idx);
        }
        else
        {
            uint numOn = 0;
            uint numOff = 0;
            uint totLen = 0;
            for (uint elemIdx = 0; elemIdx < numElems; ++elemIdx)
            {
                const ElemInfo & elem = elems[elemIdx];
                if (elem.status() == ElemState::ElemActive)
                {
                    const char * str = (const char *) &raw[elem.atomIdx() * Bin<Z2raw>::a_to_bytes];
                    maxAtomIdx = std::max<uint>(maxAtomIdx, (uint) elem.atomIdx());
                    ++numOn;
                    totLen += (uint) strlen(str);
                }
                numOff += (elem.status() == ElemState::ElemInactive ? 1 : 0);
            }
            //printf("Thread %u found %u elems, %u atoms, %u disabled elems, %u total lenght.\n", thdIdx, numElems, maxAtomIdx, numOff, totLen);
        }
    }
}

void writer(volatile long & shuttingDown, Bin<Z2raw> * pmybin, uint thdIdx)
{
    char buffer[2048];
    uint iter = 0;

    try {
        while (shuttingDown != 1)
        {
            sprintf_s<2048>(buffer, "Thread ....................................................................................................%u iter %u.", thdIdx, iter++);
            uint sizeBytes = (uint) strlen(buffer) + 1;
            char * binbuffer = static_cast<char*>(pmybin->AcquireBuffer(sizeBytes));
            memcpy(binbuffer, buffer, sizeBytes);
            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
        }
    }
    catch (BinFull & ex)
    {
        printf("Bin %u is full.------------------------------------------------------\n", ex.binIdx);
    }
    shuttingDown = 1;
}

void Test_Bin()
{
    printf("MFDBCoreTest :   Bin\n");

    volatile long shuttingDown = 0;

    auto * pmybin = new Bin<Z2raw>(12, 29123, 1024 * 1024);

    Concurrency::parallel_for(0, 7, [&shuttingDown, pmybin](uint idx)
    {
        if (idx % 2)
        {
            reader(shuttingDown, pmybin, idx);
        }
        else {
            writer(shuttingDown, pmybin, idx);
        }
    });

    auto ret = pmybin->Get();
    const ElemListType & elems = ret->s_vElems;
    auto cend = elems.cbegin() + ret->s_nFreeElemIdx.load();

    std::accumulate(elems.cbegin(), cend, 0,
        [] (uint32 state, const ElemInfo & elem) -> uint32
    {
        if (elem.atomIdx() != state)
        {
            throw std::exception("no good!!!!!!!!!");
        }
        return (elem.atomSize() + state);
    });

    delete pmybin;
}
