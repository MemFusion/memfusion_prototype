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

module MemFusionDB.Collection

open MemFusionDB.MongoMessages
open MemFusionDB.MsgUtil
open MemFusionDB.Logging
open MemFusionDB.Z2
open MemFusionDB.IO
open MemFusionDB.CoreProxy
open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices
open MemFusion.native

type Candle = uint64

type Collection(name : string) =
    let name = name
    let MaxDocNum = 0xFFFFFFFFu

    member x.getname() = name

    member x.update_doc header z2doc   = ()
    member x.delete_doc header z2doc   = ()
    member x.get_more header z2doc     = z2doc
    member x.kill_cursors header z2doc = z2doc

    member x.execute_query (candle : Candle) (retbuf : byte []) (z2selector : byte[]) (selectBytes : uint32) (z2query : byte[]) (lftBytes : uint32)  (qpBytes : uint32) =
        let y_retbuf = pin retbuf
        let y_z2query = pin z2query
        let y_filter = pin z2selector
        let numz2 = CoreProxy.Query_Find candle name ~~y_filter selectBytes ~~y_z2query lftBytes qpBytes ~~y_retbuf
        !~y_retbuf
        !~y_filter
        !~y_z2query
        let numbytes = (int numz2) * (int Z2Constants.z2size)
        retbuf.[0 .. numbytes-1]

    member x.execute_aggregation (candle : Candle) (retbuf : byte []) (z2query : byte[]) (queryBytes : uint32) sort =
        let y_retbuf = pin retbuf
        let y_z2query = pin z2query
        let uintsort = if sort then 1u else 0u
        let numz2 = CoreProxy.Query_Aggregate candle name ~~y_z2query queryBytes ~~y_retbuf uintsort
        !~y_retbuf
        !~y_z2query
        let numbytes = (int numz2) * (int Z2Constants.z2size)
        retbuf.[0 .. numbytes-1]

    member x.FS_AcquireInsertBuffer (candle : Candle) (bytesize : uint32) = 
        let ptr = CoreProxy.AcquireInsertBuffer candle name bytesize
        new NativeCustomBW(ptr)

    member x.FS_ReleaseInsertBuffer (candle : Candle) (cbw : NativeCustomBW) =
        CoreProxy.ReleaseInsertBuffer candle name (cbw.GetNativeInt())

type FakeCollection(name : string) =
    let name = name

    static member InitializeQueryEngine(maxConcIB, bmaxelems, bmaxsize, maxbins) =
        ()

    member x.execute_query (candle : Candle) (retbuf : byte []) (z2selector : byte[]) (selectBytes : uint32) (z2query : byte[]) (lftBytes : uint32)  (qpBytes : uint32) = retbuf

    member x.FS_AcquireInsertBuffer (candle : Candle) (bytesize : uint32) =  new DummyBW()

    member x.FS_ReleaseInsertBuffer (candle : Candle) (cbw : NativeCustomBW) = 0u

