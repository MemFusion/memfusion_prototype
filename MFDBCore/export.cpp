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

#include "MFDBCore.h"
#include "QueryEngine.h"


extern "C" EXPORT_FUNC void * MFDBCore_AcquireBufferForInsert(MFDB::Candle ch, const char * collection, uint32 size)
{
    return (MFDB::QueryEngine::Instance()->AcquireInsertBuffer(ch, collection, size));
}

extern "C" EXPORT_FUNC uint32 MFDBCore_ReleaseBufferForInsert(MFDB::Candle ch, const char * collection, void * buffer)
{
    return (MFDB::QueryEngine::Instance()->ReleaseBufferForInsert(ch, collection, buffer) ? 1 : 0);
}

extern "C" EXPORT_FUNC void MFDBCore_Initialize_QueryEngine(uint32 maxConcIB, uint32 bmaxelems, uint32 bmaxsize, uint32 maxbins, const char * datapath)
{
    return (MFDB::QueryEngine::InitializeQueryEngine(maxConcIB, bmaxelems, bmaxsize, maxbins, MemFusion::Path(datapath)));
}

extern "C" EXPORT_FUNC uint32 MFDBCore_Query_Find(MFDB::Candle ch, const char * collection, void * z2selector, uint32 selectBytes, void * z2query, uint32 lftBytes, uint32 qpBytes, void * retbuf)
{
    return (MFDB::QueryEngine::Instance()->Query_Find(ch, collection, z2selector, selectBytes, z2query, lftBytes, qpBytes, retbuf));
}

extern "C" EXPORT_FUNC uint32 MFDBCore_Query_Aggregate(MFDB::Candle ch, const char * collection, void * z2query, uint32 queryBytes, void * retbuf, uint32 uintsort)
{
    return (MFDB::QueryEngine::Instance()->Query_Aggregate(ch, collection, z2query, queryBytes, retbuf, uintsort));
}

