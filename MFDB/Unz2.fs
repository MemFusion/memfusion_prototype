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

namespace MemFusionDB.UnZ2

open System
open System.IO
open System.Collections.Generic
open System.Threading
open System.Runtime.Serialization.Formatters.Binary
open MemFusionDB
open MemFusionDB.BSONAst
open MemFusionDB.BSONUtil
open MemFusion.Util
open MemFusionDB.Collection
open MemFusionDB.Z2
open MemFusionDB.BitConverter
open MemFusionDB.Hybrid
open MemFusionDB.Exceptions
open MemFusionDB.IO


type z2reader(z2list : Z2list) =
    let z2array = z2list |> List.toArray
    let size = z2array.Length
    let mutable index = 0

    member x.eos() = index >= size

    member x.get() =
        if x.eos() then raise(InternalError("index out-of-bounds in z2reader"))
        let ret = z2array.[index]
        index <- index + 1
        ret

    member x.peek() =
        if x.eos() then raise(InternalError("index out-of-bounds in z2reader"))
        z2array.[index]


type Z2RCtx(blobs : HybridBlobHash, decodeNameIdx) =
    let _blobs = blobs

    member x.decode_binary_hash (hash : HashType) : byte [] = _blobs.decode_hash hash
    member x.decode_nameidx (nameidx : uint32) : string = decodeNameIdx nameidx

    interface System.IDisposable with
        member x.Dispose() = ()


type Unz2(z2rctx : Z2RCtx) =
    let z2rctx = z2rctx

    let decode_name (name : Z2name) = z2rctx.decode_nameidx name

    let decode_binary_value (cw : z2compword) (value : Z2value) =
        let allbytes = match z2cw_shortvalue cw with
                        | true -> Array.concat [ BitConverter.GetBytes(value) ]
                        | false -> z2rctx.decode_binary_hash value
        if z2cw_shortvalue cw
            then
                let len = int32 (z2cw_vlen cw)
                // onthe left: minus one because we encode subtype also in len
                (len-1, allbytes.[0], allbytes.[1 .. len-1])
            else
                let len = System.BitConverter.ToInt32(allbytes,0)
                (len, allbytes.[4], allbytes.[5 .. len-1])

    let decode_string_value (cw : z2compword) (value : Z2value) =
        try
            match z2cw_shortvalue cw with
                    | true ->
                        let vlen = z2cw_vlen cw
                        let arr = Array.concat [ BitConverter.GetBytes value ]
                        arr.[0 .. (int)vlen-1]
                    | false ->
                        z2rctx.decode_binary_hash value
            |> MFUtil.StringFromByteArray
        with
            | :? System.Collections.Generic.KeyNotFoundException as ex ->
                raise(InternalError(sprintf "KeyNotFoundException in decode_string_value: %s" ex.Message))
            | _ -> raise(InternalError("Unknwon exception in decode_string_value"))

    let rec myunz2_elem (z2r : z2reader) =
        let z = z2r.get()
        let dd = z2docdepth z
        let z2t = z2type z
        let realname = decode_name (z2name z)
        let value = z2value z
        let zcw = z2cw z

        match z2t with
            | BSONtype.Floatnum     ->
                Floatnum(realname, BitConverter.ToDouble(BitConverter.GetBytes(value),0))
            | BSONtype.Int64     ->
                Int64(realname, int64(value))
            | BSONtype.Int32     ->
                Int32(realname, int32(value))
            | BSONtype.UTF8String   ->
                let str = decode_string_value (z2cw z) value
                UTF8String(realname, (str.Length, str))
            | BSONtype.EmbeddedDoc  ->
                let lambda (z2r : z2reader) = (not (z2r.eos()) && (dd < z2docdepth (z2r.peek())))
                let newdoc = myunz2 z2r lambda
                EmbeddedDoc(realname, newdoc)
            | BSONtype.ArrayDoc     ->
                let kids = uint32 value
                let lambda (z2r : z2reader) = (not (z2r.eos()) && (dd < z2docdepth (z2r.peek())))
                let newdoc =
                    seq {
                        for idx = 0u to kids-1u do
                        let elem = myunz2_elem z2r
                        let elem = BSONChangeName elem (Convert.ToString idx)
                        yield elem
                    } |> Seq.toList
                ArrayDoc(realname, (0, newdoc))  // rev ??
            | BSONtype.BinaryData   ->
                let (size, subt, bytes) = decode_binary_value zcw value
                if bytes.Length <> size then raise(InternalError("Wrong size unz2ing BinaryData."))
                BinaryData(realname, (subt, bytes))
            | BSONtype.Undefined    -> Undefined(realname)
            | BSONtype.ObjectID     ->
                let bytes = z2rctx.decode_binary_hash value
                ObjectID(realname, bytes)
            | BSONtype.Bool         ->
                let bval = match value with
                                | 0UL -> false
                                | 1UL -> true
                                | _ -> raise(UnZ2InternalError("Bool"))
                Bool(realname, bval)
            | BSONtype.UTCDateTime  -> UTCDateTime(realname, (int64 value))
            | BSONtype.Null         -> Null(realname)
            | BSONtype.RegEx        ->
                let bytes = z2rctx.decode_binary_hash value
                let s1len = BitConverter.ToInt32(bytes, 0)
                let s2len = BitConverter.ToInt32(bytes, 4+s1len)
                let s1 = System.Text.Encoding.ASCII.GetString(bytes.[4 .. 4+s1len])
                let s2 = System.Text.Encoding.ASCII.GetString(bytes.[4+s1len+4 .. ])
                RegEx(realname, s1, s2)
            | BSONtype.DBPointer    ->
                let allbytes = z2rctx.decode_binary_hash value
                let str = System.Text.Encoding.ASCII.GetString(allbytes.[ 0 .. allbytes.Length-12 ])
                let bytes = allbytes.[ allbytes.Length-12 .. ]
                DBPointer(realname, (str.Length, str), bytes)
            | BSONtype.JScode       ->
                let str = decode_string_value zcw value
                JScode(realname, (str.Length, str))
            | BSONtype.Symbol       ->
                let str = decode_string_value zcw value
                Symbol(realname, (str.Length, str))
            | BSONtype.JSCodeWScope ->
                let str = decode_string_value zcw value
                let bson_str : BSON_string = (str.Length, str)
                let lambda (z2r : z2reader) = (not (z2r.eos()) && (dd < z2docdepth (z2r.peek())))
                let newdoc = myunz2 z2r lambda
                JSCodeWScope(realname, (bson_str, newdoc))
            | BSONtype.TimeStamp    -> TimeStamp(realname, int64(value))
            | BSONtype.MinKey       -> MinKey(realname)
            | BSONtype.MaxKey       -> MaxKey(realname)
            | _ -> raise(InternalError(sprintf "Unknown BSON type %A during unz2" z2t))

    and myunz2 (z2r : z2reader) (lambda : z2reader -> bool) : BSON_Document =
        let xx = seq {  // not (z2r.eos())
            while (lambda z2r) do
                let temp = myunz2_elem z2r
                yield temp
        }
        (0, xx |> Seq.toList)

    member private x.z2_to_BSON_Impl (z2r : z2reader) : BSON_Document =
        let one = myunz2 z2r (fun z2r -> not (z2r.eos()))
        one

    static member z2_to_BSON (z2rctx : Z2RCtx) (z2list : Z2list) : BSON_Document =
        if z2list.Length = 0 then emptyDoc
            else
                let f = new Unz2(z2rctx)
                f.z2_to_BSON_Impl (new z2reader(z2list))

    static member z2bytes_to_BSON (z2rctx : Z2RCtx) (z2bytes : byte []) : BSON_Document list =
        if z2bytes.Length=0 then [emptyDoc]
        else
            let z2size = int Z2Constants.z2size
            let numDocuments = z2value (BitConverter.ToZ2(z2bytes, 0))

            let z2seq = seq {  // read from binary buffer
                                for idx in 1 .. (z2bytes.Length/z2size)-1 do
                                    let z2__ = BitConverter.ToZ2(z2bytes, idx*z2size)
                                    yield z2__
                             }

            let z2seqseq = z2seq |> Seq.groupWhen (fun z -> z2docdepth z = -1)

            let z2listlist =
                z2seqseq
                |> Seq.map (fun z2seq -> z2seq |> Seq.filter (fun z -> z2docdepth z <> -1) |> Seq.toList)
                |> Seq.toList

//            printf "\nz2seq0 = %A" z2seq
//            printf "\nz2listlist  = %A" z2listlist

            let f = new Unz2(z2rctx)
            z2listlist
            |> List.map (fun z2list -> f.z2_to_BSON_Impl (new z2reader(z2list)))

