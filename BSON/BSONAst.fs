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

module MemFusionDB.BSONAst
open MemFusion.Util

type BSON_Document = int32 * BSON_e_list

and BSON_e_name = BSON_cstring

and BSON_e_list = BSON_element list

and Floatnum     = BSON_e_name * double 
and UTF8String   = BSON_e_name * BSON_string
and EmbeddedDoc  = BSON_e_name * BSON_Document
and ArrayDoc     = BSON_e_name * BSON_Document
and BinaryData   = BSON_e_name * BSON_binary
and Undefined    = BSON_e_name
and ObjectID     = BSON_e_name * Byte12
and Bool         = BSON_e_name * bool
and UTCDateTime  = BSON_e_name * int64
and Null         = BSON_e_name
and RegEx        = BSON_e_name * BSON_cstring * BSON_cstring
and DBPointer    = BSON_e_name * BSON_string * Byte12
and JScode       = BSON_e_name * BSON_string
and Symbol       = BSON_e_name * BSON_string
and JSCodeWScope = BSON_e_name * BSON_code_w_s
and Int32        = BSON_e_name * int32
and Int64        = BSON_e_name * int64
and TimeStamp    = BSON_e_name * int64
and MinKey       = BSON_e_name
and MaxKey       = BSON_e_name

and
    //[<CustomEquality; CustomComparison>]
    BSON_element =
    | Floatnum     of Floatnum
    | UTF8String   of UTF8String
    | EmbeddedDoc  of EmbeddedDoc
    | ArrayDoc     of ArrayDoc
    | BinaryData   of BinaryData
    | Undefined    of Undefined
    | ObjectID     of ObjectID
    | Bool         of Bool
    | UTCDateTime  of UTCDateTime
    | Null         of Null
    | RegEx        of RegEx
    | DBPointer    of DBPointer
    | JScode       of JScode
    | Symbol       of Symbol
    | JSCodeWScope of JSCodeWScope
    | Int32        of Int32
    | Int64        of Int64
    | TimeStamp    of TimeStamp
    | MinKey       of MinKey
    | MaxKey       of MaxKey

and BSON_string =  int32 * BSON_cstring

and BSON_cstring = string

and BSON_code_w_s = BSON_string * BSON_Document

and BSON_binary = BSON_subtype * byte array

and BSON_subtype = byte

and Byte12 = byte array


type BSONtype =
    | Floatnum     = 1uy
    | UTF8String   = 2uy
    | EmbeddedDoc  = 3uy
    | ArrayDoc     = 4uy
    | BinaryData   = 5uy
    | Undefined    = 6uy
    | ObjectID     = 7uy
    | Bool         = 8uy
    | UTCDateTime  = 9uy
    | Null         = 10uy
    | RegEx        = 11uy
    | DBPointer    = 12uy
    | JScode       = 13uy
    | Symbol       = 14uy
    | JSCodeWScope = 15uy
    | Int32        = 16uy
    | TimeStamp    = 17uy
    | Int64        = 18uy
    | MaxKey       = 0x7Fuy
    | MinKey       = 0xFFuy

let emptyDoc = (0, [])

let CompareBSONElements (elem1 : BSON_element) (elem2 : BSON_element) =
    match (elem1, elem2) with
        | (Floatnum v1, Floatnum v2) ->  (fst v1) = (fst v2)
        | (UTF8String   v1, UTF8String v2) ->  (fst v1) = (fst v2)
        | (EmbeddedDoc  v1, EmbeddedDoc v2) ->  (fst v1) = (fst v2)
        | (ArrayDoc     v1, ArrayDoc v2) ->  (fst v1) = (fst v2)
        | (BinaryData   v1, BinaryData v2) ->  (fst v1) = (fst v2)
        | (Undefined    v1, Undefined v2) ->  v1 = v2
        | (ObjectID     v1, ObjectID v2) ->  (fst v1) = (fst v2)
        | (Bool         v1, Bool v2) ->  (fst v1) = (fst v2)
        | (UTCDateTime  v1, UTCDateTime v2) ->  (fst v1) = (fst v2)
        | (Null         v1, Null v2) ->  v1 = v2
        | (RegEx        v1, RegEx v2) ->  (fst3 v1) = (fst3 v2)
        | (DBPointer    v1, DBPointer v2) ->  (fst3 v1) = (fst3 v2)
        | (JScode       v1, JScode v2) ->  (fst v1) = (fst v2)
        | (Symbol       v1, Symbol v2) ->  (fst v1) = (fst v2)
        | (JSCodeWScope v1, JSCodeWScope v2) ->  (fst v1) = (fst v2)
        | (Int32        v1, Int32 v2) ->  (fst v1) = (fst v2)
        | (Int64        v1, Int64 v2) ->  (fst v1) = (fst v2)
        | (TimeStamp    v1, TimeStamp v2) ->  (fst v1) = (fst v2)
        | (MinKey       v1, MinKey v2) -> v1 = v2
        | (MaxKey       v1, MaxKey v2) -> v1 = v2
        | _ -> false

let strOf (str : string) = (str.Length, str)

let BSONChangeName (elem : BSON_element) (name : BSON_e_name) =
    match elem with
    | Floatnum     v -> Floatnum(name, snd v)
    | UTF8String   v -> UTF8String(name, snd v)
    | EmbeddedDoc  v -> EmbeddedDoc(name, snd v)
    | ArrayDoc     v -> ArrayDoc(name, snd v)
    | BinaryData   v -> BinaryData(name, snd v)
    | Undefined    v -> Undefined(name)
    | ObjectID     v -> ObjectID(name, snd v)
    | Bool         v -> Bool(name, snd v)
    | UTCDateTime  v -> UTCDateTime(name, snd v)
    | Null         v -> Null(name)
    | RegEx        v -> RegEx(name, snd3 v, thd3 v)
    | DBPointer    v -> DBPointer(name, snd3 v, thd3 v)
    | JScode       v -> JScode(name, snd v)
    | Symbol       v -> Symbol(name, snd v)
    | JSCodeWScope v -> JSCodeWScope(name, snd v)
    | Int32        v -> Int32(name, snd v)
    | Int64        v -> Int64(name, snd v)
    | TimeStamp    v -> TimeStamp(name, snd v)
    | MinKey       v -> MinKey(name)
    | MaxKey       v -> MaxKey(name)

