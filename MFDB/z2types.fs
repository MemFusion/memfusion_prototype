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

module MemFusionDB.Z2

open MemFusionDB.BSONAst
open MemFusion.Util
open MemFusionDB.Exceptions

type Z2DocDepth = int32
type HashType = uint64
type Z2name = uint32  // 24 really
type Z2value = uint64
type Z2vlen = byte

type BSONtypeCompressed =
    | CFloatnum     = 1uy
    | CUTF8String   = 2uy
    | CEmbeddedDoc  = 3uy
    | CArrayDoc     = 4uy
    | CBinaryData   = 5uy
    | CUndefined    = 6uy
    | CObjectID     = 7uy
    | CBool         = 8uy
    | CUTCDateTime  = 9uy
    | CNull         = 10uy
    | CRegEx        = 11uy
    | CDBPointer    = 12uy
    | CJScode       = 13uy
    | CSymbol       = 14uy
    | CJSCodeWScope = 15uy
    | CInt32        = 16uy
    | CTimeStamp    = 17uy
    | CInt64        = 18uy
    | CMaxKey       = 19uy
    | CMinKey       = 20uy

type z2compword = Z2name * BSONtype * bool * Z2vlen

let CompressBSONtype t =
    match t with
        | BSONtype.Floatnum     -> BSONtypeCompressed.CFloatnum
        | BSONtype.UTF8String   -> BSONtypeCompressed.CUTF8String
        | BSONtype.EmbeddedDoc  -> BSONtypeCompressed.CEmbeddedDoc
        | BSONtype.ArrayDoc     -> BSONtypeCompressed.CArrayDoc
        | BSONtype.BinaryData   -> BSONtypeCompressed.CBinaryData
        | BSONtype.Undefined    -> BSONtypeCompressed.CUndefined
        | BSONtype.ObjectID     -> BSONtypeCompressed.CObjectID
        | BSONtype.Bool         -> BSONtypeCompressed.CBool
        | BSONtype.UTCDateTime  -> BSONtypeCompressed.CUTCDateTime
        | BSONtype.Null         -> BSONtypeCompressed.CNull
        | BSONtype.RegEx        -> BSONtypeCompressed.CRegEx
        | BSONtype.DBPointer    -> BSONtypeCompressed.CDBPointer
        | BSONtype.JScode       -> BSONtypeCompressed.CJScode
        | BSONtype.Symbol       -> BSONtypeCompressed.CSymbol
        | BSONtype.JSCodeWScope -> BSONtypeCompressed.CJSCodeWScope
        | BSONtype.Int32        -> BSONtypeCompressed.CInt32
        | BSONtype.TimeStamp    -> BSONtypeCompressed.CTimeStamp
        | BSONtype.Int64        -> BSONtypeCompressed.CInt64
        | BSONtype.MinKey       -> BSONtypeCompressed.CMinKey
        | BSONtype.MaxKey       -> BSONtypeCompressed.CMaxKey
        | _ -> failwith "wrong BSONtype in z2info"

let DecompressBSONtype t =
    match t with
        | BSONtypeCompressed.CFloatnum     -> BSONtype.Floatnum
        | BSONtypeCompressed.CUTF8String   -> BSONtype.UTF8String
        | BSONtypeCompressed.CEmbeddedDoc  -> BSONtype.EmbeddedDoc
        | BSONtypeCompressed.CArrayDoc     -> BSONtype.ArrayDoc
        | BSONtypeCompressed.CBinaryData   -> BSONtype.BinaryData
        | BSONtypeCompressed.CUndefined    -> BSONtype.Undefined
        | BSONtypeCompressed.CObjectID     -> BSONtype.ObjectID
        | BSONtypeCompressed.CBool         -> BSONtype.Bool
        | BSONtypeCompressed.CUTCDateTime  -> BSONtype.UTCDateTime
        | BSONtypeCompressed.CNull         -> BSONtype.Null
        | BSONtypeCompressed.CRegEx        -> BSONtype.RegEx
        | BSONtypeCompressed.CDBPointer    -> BSONtype.DBPointer
        | BSONtypeCompressed.CJScode       -> BSONtype.JScode
        | BSONtypeCompressed.CSymbol       -> BSONtype.Symbol
        | BSONtypeCompressed.CJSCodeWScope -> BSONtype.JSCodeWScope
        | BSONtypeCompressed.CInt32        -> BSONtype.Int32
        | BSONtypeCompressed.CTimeStamp    -> BSONtype.TimeStamp
        | BSONtypeCompressed.CInt64        -> BSONtype.Int64
        | BSONtypeCompressed.CMinKey       -> BSONtype.MinKey
        | BSONtypeCompressed.CMaxKey       -> BSONtype.MaxKey
        | _ -> failwith "wrong BSONtypeCompressed in z2info"

let DecompressBSONtype_byte (t : byte) =
    match t with
        | 1uy -> BSONtype.Floatnum
        | 2uy -> BSONtype.UTF8String
        | 3uy -> BSONtype.EmbeddedDoc
        | 4uy -> BSONtype.ArrayDoc
        | 5uy -> BSONtype.BinaryData
        | 6uy -> BSONtype.Undefined
        | 7uy -> BSONtype.ObjectID
        | 8uy -> BSONtype.Bool
        | 9uy -> BSONtype.UTCDateTime
        | 10uy -> BSONtype.Null
        | 11uy -> BSONtype.RegEx
        | 12uy -> BSONtype.DBPointer
        | 13uy -> BSONtype.JScode
        | 14uy -> BSONtype.Symbol
        | 15uy -> BSONtype.JSCodeWScope
        | 16uy -> BSONtype.Int32
        | 17uy -> BSONtype.TimeStamp
        | 18uy -> BSONtype.Int64
        | 19uy -> BSONtype.MinKey
        | 20uy -> BSONtype.MaxKey
        | _ -> failwith "wrong BSONtypeCompressed in z2info"

let z2cw_serialize(z2cw : z2compword) =
    let vlen = fth4 z2cw
    if vlen>8uy then failwith "Z2vlen should not be greater than 8 (serializing)"
    let ret = (((uint32 vlen) <<< 28) ||| ((uint32 (snd4 z2cw)) <<< 23) ||| (fst4 z2cw))
    ret

let z2cw_create(zn : Z2name, t : BSONtype, v : bool, vl : Z2vlen) : z2compword = (zn, t, v, vl)

let z2cw_deserialize(serialized : uint32) : z2compword =
    let zn = serialized &&& 0x7FFFFFu
    let tb = byte ((serialized &&& 0x0F800000u) >>> 23)
    let t = DecompressBSONtype_byte(tb)
    let vlen : byte = (byte)(serialized >>> 28)
    if vlen > 8uy then failwith "Z2vlen should not be greater than 8 (deserializing)"
    let svb = vlen > 0uy
    (zn, t, svb, vlen)

type z2cwserial = uint32

//          4B           4B          8B
type z2 = Z2DocDepth * z2compword * Z2value

let z2cw_name (z2cw : z2compword) = fst4 z2cw
let z2cw_type (z2cw : z2compword) = snd4 z2cw
let z2cw_shortvalue (z2cw : z2compword) = thd4 z2cw
let z2cw_replace_name (z2cw : z2compword) (zname : Z2name) =
    z2cw_create(zname, z2cw_type z2cw, thd4 z2cw, fth4 z2cw)
let z2cw_vlen (z2cw : z2compword) = fth4 z2cw

let z2name (a : z2) = z2cw_name(snd3 a)
let z2type (a : z2) = z2cw_type(snd3 a)
let z2docdepth (a : z2) = fst3 a
let z2cw (a : z2) = snd3 a
let z2value (a : z2) = thd3 a


type Z2Constants =
    | _ID    = 1u
    | ArrayNamesBase = 1024u
    | UserNamesBase = 2048u
    | z2size = 16u

//   z2 format

//    QWORD      DWORD       Bits         Description
//    0          0           all          DocDepth  (32bit)
//    
//    0          1            0-22        Z2name
//    0          1           23-27        Z2type  (5bit)
//    0          1           28-31        short value: 0 means hashed; 1-8 is the length; should not be more than 8
//    
//    1          2            all         Z2value low
//    1          3            all         Z2value high


type Z2list  = z2 list
type Z2rlist = z2 list

type Zstate<'Payload> = Z2DocDepth * 'Payload * (Z2DocDepth * Z2name) list

//Z2rlist

let docdepth (zs : Zstate<'Payload>) = fst3 zs
let payload  (zs : Zstate<'Payload>) = snd3 zs
let arname   (zs : Zstate<'Payload>) = thd3 zs

let InitialDocDepth = 0

