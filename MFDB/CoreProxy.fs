//
// Copyright (c) 2014  Benedetto Proietti
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

module MemFusionDB.CoreProxy

open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices
#nowarn "9"

[<DllImport("MFDBCore.dll",EntryPoint="MFDBCore_AcquireBufferForInsert",CallingConvention=CallingConvention.StdCall)>]
extern void * MFDBCore_AcquireBufferForInsert(uint64 ch, string collection, uint32 size);

[<DllImport("MFDBCore.dll",EntryPoint="MFDBCore_ReleaseBufferForInsert",CallingConvention=CallingConvention.StdCall)>]
extern uint32 MFDBCore_ReleaseBufferForInsert(uint64 ch, string collection, void *);

[<DllImport("MFDBCore.dll",EntryPoint="MFDBCore_Initialize_QueryEngine",CallingConvention=CallingConvention.StdCall)>]
extern void MFDBCore_Initialize_QueryEngine(uint32, uint32, uint32, uint32, string datapath);

[<DllImport("MFDBCore.dll",EntryPoint="MFDBCore_Query_Find",CallingConvention=CallingConvention.StdCall)>]
extern uint32 MFDBCore_Query_Find(uint64 ch, string collection, void * z2selector, uint32 selectBytes, void * z2query, uint32 lftBytes, uint32 qpBytes, void * retbuf);

[<DllImport("MFDBCore.dll",EntryPoint="MFDBCore_Query_Aggregate",CallingConvention=CallingConvention.StdCall)>]
extern uint32 MFDBCore_Query_Aggregate(uint64 ch, string collection, void * z2query, uint32 queryBytes, void * retbuf, uint32 uintsort);

type CoreProxy() =
    static member AcquireInsertBuffer (candle : uint64) (collection : string) (size : uint32) =
        let mutable c_str = collection
        let ret = MFDBCore_AcquireBufferForInsert(candle, c_str, size)
        ret

    static member ReleaseInsertBuffer (candle : uint64) (collection : string) (nint : nativeint) =
        let mutable c_str = collection
        let ret = MFDBCore_ReleaseBufferForInsert(candle, c_str, nint)
        ret

    static member InitializeQueryEngine (maxConcIB, bmaxelems, bmaxsize, maxbins, datapath) =
        MFDBCore_Initialize_QueryEngine(maxConcIB, bmaxelems, bmaxsize, maxbins, datapath)

    static member Query_Find (candle : uint64) (collection : string) (z2selector : nativeint) (selectBytes : uint32) (z2query : nativeint) (lftBytes : uint32) (qpBytes : uint32) (retbuf : nativeint) =
        let mutable c_str = collection
        MFDBCore_Query_Find(candle, c_str, z2selector, selectBytes, z2query, lftBytes, qpBytes, retbuf)

    static member Query_Aggregate (candle : uint64) (collection : string) (z2query : nativeint) (queryBytes : uint32) (retbuf : nativeint) (uintsort : uint32) =
        let mutable c_str = collection
        MFDBCore_Query_Aggregate(candle, c_str, z2query, queryBytes, retbuf, uintsort)
