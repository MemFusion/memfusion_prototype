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

namespace MemFusionDB.Z2Parser

open System
open System.IO
open System.Collections.Generic
open System.Threading
open System.Security.Cryptography
open System.Runtime.Serialization.Formatters.Binary
open MemFusionDB
open MemFusionDB.BSONAst
open MemFusionDB.BSONUtil
open MemFusion.Util
open MemFusionDB.Collection
open MemFusionDB.Z2
open MemFusionDB.Hybrid
open MemFusionDB.IO
open MemFusionDB.Exceptions

type Consumer<'Payload> = Z2Ctx<'Payload> -> Zstate<'Payload list> -> z2compword -> Z2value -> Zstate<'Payload list>

and Consumers<'Payload> = Consumer<'Payload> * Consumer<'Payload>

and Z2Ctx<'Payload>(bw : ICustomBW, blobs : HybridBlobHash, acquireName: string -> uint32, consumers : Consumers<'Payload>) =
    let _bw = bw
    let _blobs = blobs
    let nextLFTidx = ref 0l
    let _consume_pre = fst consumers
    let _consume_post = snd consumers
    static let md5 = new MD5CryptoServiceProvider()

    member x.get_lft_idx() =
        uint32 (Interlocked.Increment nextLFTidx)

    member x.add_hash (hash : HashType, bytes : byte []) = _blobs.add_hash hash bytes

    member x.acquire_name_idx name = acquireName name

    member x.bw = _bw

    member x.BytesWritten() = _bw.BytesWritten()

    member x.hash (bytes : byte []) =
        lock md5 (fun () -> md5.ComputeHash(bytes))

    member x.consume_pre zs zcw value = _consume_pre x zs zcw value
    member x.consume_post zs zcw value = _consume_post x zs zcw value

    interface System.IDisposable with
        member x.Dispose() = ()


type Z2<'Payload>(z2ctx : Z2Ctx<'Payload>) =
    let z2ctx = z2ctx
    let bw = z2ctx.bw
    let consume_pre = z2ctx.consume_pre
    let consume_post = z2ctx.consume_post

    let MaxValueBytes = 8
    let MaxStringBytes = 8

    let bytesToUint64 (bytes : byte []) : uint64 =
        let len = bytes.Length
        if len > MaxValueBytes then failwith "Internal error: string too long in bytesToUint64"
        let b0 = uint64 bytes.[0]
        let b1 = if len>1 then uint64 bytes.[1] else 0UL
        let b2 = if len>2 then uint64 bytes.[2] else 0UL
        let b3 = if len>3 then uint64 bytes.[3] else 0UL
        let b4 = if len>4 then uint64 bytes.[4] else 0UL
        let b5 = if len>5 then uint64 bytes.[5] else 0UL
        let b6 = if len>6 then uint64 bytes.[6] else 0UL
        let b7 = if len>7 then uint64 bytes.[7] else 0UL
        let low  = b0 ||| (b1 <<< 8) ||| (b2 <<< 16) ||| (b3 <<< 24) ||| (b4 <<< 32) ||| (b5 <<< 40) ||| (b6 <<< 48) ||| (b7 <<< 56)
        low

    let strToUint64 (str : string) : uint64 =
        bytesToUint64 (Text.Encoding.ASCII.GetBytes str)

    // main entry point
    // Convert BSON document into Z2 and serializes the Z2 with the given BW
    // This also returns the Z2 list
    static member z2_doc (doc : BSON_Document) (z2ctx : Z2Ctx<'Payload>) : 'Payload list =
        let f = new Z2<'Payload>(z2ctx)
        Z2<'Payload>.z2_doc_impl f (InitialDocDepth,[],[]) doc
        |> payload
        |> List.rev

    // re-entrant doc folder
    static member private z2_doc_impl (f : Z2<'Payload>) (zs: Zstate<'Payload list>) (doc : BSON_Document) : Zstate<'Payload list> =
        doc |> BSON.fold f zs

    member private x.nested_doc zs =
        (docdepth zs) + 1

    member private x.hash_64 (v : byte []) =
        let ret = z2ctx.hash v
        let ret2 = BitConverter.ToUInt64(ret, 0)
        ret2

    member private x.hash_64 (str : string) = x.hash_64 (System.Text.Encoding.ASCII.GetBytes(str))

    member private x.binary_hash (bytes : byte []) =
        let hash = x.hash_64 bytes
        z2ctx.add_hash(hash, bytes)
        hash

    member private x.string_hash (str : BSON_cstring) =
        let hash = x.hash_64 str
        let bstr = Array.append (System.Text.Encoding.ASCII.GetBytes(str)) [|0uy|]
        z2ctx.add_hash(hash, bstr)
        hash

    member private x.string_hash (str : BSON_string) = x.string_hash (snd str)

    member private x.create_name zs (str : BSON_cstring) (bytes : byte []) =
        let dd = docdepth zs
        if Char.IsDigit(str.[0])
            then
                let intstr = uint32 (Convert.ToInt32(str))
                let result = (uint32 Z2Constants.ArrayNamesBase) + intstr
                match arname zs with
                    | aname :: tail->
                        let (an_dd,name) = aname
                        if (an_dd=dd) then name else result
                    | [] -> result
            else
                match str with
                    | "_id" -> uint32 Z2Constants._ID
                    | _ -> z2ctx.acquire_name_idx str

    member private x.create_value (v : BSON_string) =
        let str : string = snd v
        let strlen = 1 + (fst v)
        let strlen2 = String.length str
        let isshort = strlen2 <= MaxValueBytes
        let value = if isshort then strToUint64 str else x.string_hash v
        (value, isshort, strlen2)

    member private x.create_value (v : double) =
        let value = BitConverter.ToUInt64(BitConverter.GetBytes v, 0)
        (value, true)

    member private x.create_value (v : BSON_binary) =
        let len = lengthOf v
        let isshort = len <= MaxValueBytes
        let bytesShort =  Array.concat [ [|byte (fst v)|]; snd v]
        let bytesLong =  Array.concat [  BitConverter.GetBytes(len); [|byte (fst v)|]; snd v]
        let value = if isshort then bytesToUint64 bytesShort else x.binary_hash bytesLong
        (value, isshort)

    member private x.create_value (bytes : byte []) =
        let len = bytes.Length
        let isshort = len <= MaxValueBytes
        let value = if isshort then bytesToUint64 bytes else x.binary_hash bytes
        (value, isshort)

    member private x.create_value (v : bool) =
        ((if v then 1UL else 0UL), true)

    member private x.create_value (v : int64) =
        ((uint64 v), true)

    member private x.create_value (s1 : string, s2 : string) =
        let bytes = Array.concat [BitConverter.GetBytes s1.Length;
                                  Text.Encoding.ASCII.GetBytes s1;
                                  BitConverter.GetBytes s2.Length;
                                  Text.Encoding.ASCII.GetBytes s2]
        let value = x.binary_hash bytes
        (value, false)

    member private x.fold_InnerDoc zs nameidx (doc : BSON_Document) t =
        let zcw = z2cw_create(nameidx, t, false, 0uy)
        let newdocdepth =
            match t with
                | BSONtype.ArrayDoc -> (docdepth zs)
                | _ -> (docdepth zs) + 1
        let numkids = (snd doc).Length
        let zs2 = consume_pre zs zcw (uint64 numkids)
        let zs3 = (newdocdepth, payload zs2, arname zs2)
        let (newdocdepth, outdoc, _) = Z2.z2_doc_impl x zs3 doc
        //consume_post zs zcw (uint64 numkids) |> ignore
        (docdepth zs2, outdoc, arname zs2)

    interface BSON.IBSONFolder<Zstate<'Payload list>> with
        member x.fold_Floatnum zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let (value, isshort) = x.create_value(snd v)
            let zcw = z2cw_create(nameidx, BSONtype.Floatnum, isshort, 8uy)
            consume_pre zs zcw value

        member x.fold_UTF8String zs v =
            let str = snd (snd v)
            // vlen includes trailing binary zero
            let nameidx = x.create_name zs (fst v) (System.Text.Encoding.ASCII.GetBytes(str))
            let (value, isshort, strlen) = x.create_value(snd v)
            let vlen = if isshort then (byte)strlen else 0uy
            if int32 vlen>MaxValueBytes then raise(InternalError("vlen bigger than 8 while z2ing UTF8String"))
            let zcw = z2cw_create(nameidx, BSONtype.UTF8String, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_EmbeddedDoc zs v =
            let nameidx = x.create_name zs (fst v) [||]
            x.fold_InnerDoc zs nameidx (snd v) BSONtype.EmbeddedDoc

        member x.fold_ArrayDoc zs v =
            let nameidx = x.create_name zs (fst v) [||]
            let curdepth = docdepth zs
            let zs2 = (curdepth, payload zs, (curdepth, nameidx) :: (arname zs))
            x.fold_InnerDoc zs2 nameidx (snd v) BSONtype.ArrayDoc

        member x.fold_BinaryData zs v =
            let nameidx = x.create_name zs (fst v) (BinaryDataMerged(snd v))
            let (value, isshort) = x.create_value(snd v)
            let vlen = if isshort then (byte) (lengthOf (snd v)) else 0uy
            let zcw = z2cw_create(nameidx, BSONtype.BinaryData, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_Undefined zs v =
            let nameidx = x.create_name zs v [||]
            let zcw = z2cw_create(nameidx, BSONtype.Undefined, true, 0uy)
            consume_pre zs zcw 0UL

        member x.fold_ObjectID zs v =
            let nameidx = x.create_name zs (fst v) (snd v)
            let (value, isshort) = x.create_value(snd v)
            let vlen = if isshort then (byte) (snd v).Length else 0uy
            let zcw = z2cw_create(nameidx, BSONtype.ObjectID, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_Bool zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let (value, isshort) = x.create_value(snd v)
            let zcw = z2cw_create(nameidx, BSONtype.Bool, isshort, 1uy)
            consume_pre zs zcw value

        member x.fold_UTCDateTime zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let (value, isshort) = x.create_value(snd v)
            let zcw = z2cw_create(nameidx, BSONtype.UTCDateTime, isshort, 8uy)
            consume_pre zs zcw value

        member x.fold_Null zs v =
            let nameidx = x.create_name zs v [||]
            let zcw = z2cw_create(nameidx, BSONtype.Null, true, 0uy)
            consume_pre zs zcw 0UL

        member x.fold_RegEx zs v =
            let str = (snd3 v) + (thd3 v)
            let nameidx = x.create_name zs (fst3 v) (System.Text.Encoding.ASCII.GetBytes(str))
            let (value, _) = x.create_value(snd3 v, thd3 v)
            let zcw = z2cw_create(nameidx, BSONtype.RegEx, false, 0uy)
            consume_pre zs zcw value

        member x.fold_DBPointer zs v =
            let one = System.Text.Encoding.ASCII.GetBytes(snd (snd3 v))
            let merged = Array.concat [one; thd3 v]
            let nameidx = x.create_name zs (fst3 v) merged
            let one = Text.Encoding.ASCII.GetBytes (snd (snd3 v))
            let two = thd3 v
            let bytes = Array.concat [one; [|0uy|]; two ]
            let (value, isshort) = x.create_value(bytes)
            let vlen = if isshort then (byte) bytes.Length else 0uy
            let zcw = z2cw_create(nameidx, BSONtype.DBPointer, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_JScode zs v =
            let nameidx = x.create_name zs (fst v) (System.Text.Encoding.ASCII.GetBytes(snd (snd v)))
            let (value, isshort, strlen) = x.create_value(snd v)
            let vlen = if isshort then (byte) strlen else 0uy
            let zcw = z2cw_create(nameidx, BSONtype.JScode, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_Symbol zs v =
            let nameidx = x.create_name zs (fst v) (System.Text.Encoding.ASCII.GetBytes(snd (snd v)))
            let (value, isshort, strlen) = x.create_value(snd v)
            let vlen = if isshort then (byte) strlen else 0uy
            let zcw = z2cw_create(nameidx, BSONtype.Symbol, isshort, vlen)
            consume_pre zs zcw value

        member x.fold_JSCodeWScope zs v =
            let nameidx = x.create_name zs (fst v) (System.Text.Encoding.ASCII.GetBytes(snd (fst (snd v))))
            let str = (fst (snd v))
            let doc = (snd (snd v))
            let (value, isshort, strlen) = x.create_value(str)
            let zcw = z2cw_create(nameidx, BSONtype.JSCodeWScope, false, 0uy)
            let sz = consume_pre zs zcw value
            let ret = x.fold_InnerDoc zs nameidx doc BSONtype.JSCodeWScope
            consume_post zs zcw value |> ignore
            ret

        member x.fold_Int32 zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let zcw = z2cw_create(nameidx, BSONtype.Int32, true, 4uy)
            consume_pre zs zcw (uint64 (snd v))

        member x.fold_Int64 zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let zcw = z2cw_create(nameidx, BSONtype.Int64, true, 8uy)
            consume_pre zs zcw (uint64 (snd v))

        member x.fold_TimeStamp zs v =
            let nameidx = x.create_name zs (fst v) (BitConverter.GetBytes(snd v))
            let zcw = z2cw_create(nameidx, BSONtype.TimeStamp, true, 8uy)
            consume_pre zs zcw (uint64 (snd v))

        member x.fold_MinKey zs v =
            let nameidx = x.create_name zs v [||]
            let zcw = z2cw_create(nameidx, BSONtype.MinKey, true, 8uy)
            consume_pre zs zcw 0UL

        member x.fold_MaxKey zs v =
            let nameidx = x.create_name zs v [||]
            let zcw = z2cw_create(nameidx, BSONtype.MaxKey, true, 8uy)
            consume_pre zs zcw 0UL

