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

#include <stdio.h>
#include <atomic>
#include <thread>
#include <ppl.h>
#include <numeric>
#include "MFDBCore/include/QueryEngine.h"
#include "MFDBCore/include/StrangeTypes.h"

typedef unsigned int uint;

using namespace MFDB;
using namespace MFDB::Core;
using namespace MemFusion;

void Prepare_QA()
{
    QueryEngine::InitializeQueryEngine(10, 1000 * 1000, 400 * 1000 * 1000, 10, Path("."));
}

void test_QE_Collections_insert_simple1()
{
    printf("%s\n", __FUNCTION__);

    Candle candle1 = rand();
    uint32 size1 = 1200;

    void * ib = QE->AcquireInsertBuffer(candle1, "TestColl1", size1);
    if (!ib)
        throw std::exception("ib null");

    if (!QE->ReleaseBufferForInsert(candle1, "TestColl1", ib))
        throw std::exception("ReleaseBufferForInsert failed");

    // wrong collection
    if (QE->ReleaseBufferForInsert(candle1, "XXXXX", ib))
        throw std::exception("ReleaseBufferForInsert did not fail with wrong coll name");

    // wrong ib
    if (QE->ReleaseBufferForInsert(candle1, "TestColl1", (void*)123))
        throw std::exception("ReleaseBufferForInsert did not fail with wrong ib");

}
