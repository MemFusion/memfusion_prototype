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

module MemFusionDB.BSON

open System.IO
open MemFusionDB.BSONAst
open MemFusionDB.BSONUtil
open MemFusion.Util

type IBSONFolder<'State> =
    abstract member fold_Floatnum: 'State -> Floatnum -> 'State
    abstract member fold_UTF8String: 'State -> UTF8String -> 'State
    abstract member fold_EmbeddedDoc: 'State -> EmbeddedDoc -> 'State
    abstract member fold_ArrayDoc: 'State -> ArrayDoc -> 'State
    abstract member fold_BinaryData: 'State -> BinaryData -> 'State
    abstract member fold_Undefined: 'State -> Undefined -> 'State
    abstract member fold_ObjectID: 'State -> ObjectID -> 'State
    abstract member fold_Bool: 'State -> Bool -> 'State
    abstract member fold_UTCDateTime: 'State -> UTCDateTime -> 'State
    abstract member fold_Null: 'State -> Null -> 'State
    abstract member fold_RegEx: 'State -> RegEx -> 'State
    abstract member fold_DBPointer: 'State -> DBPointer -> 'State
    abstract member fold_JScode: 'State -> JScode -> 'State
    abstract member fold_Symbol: 'State -> Symbol -> 'State
    abstract member fold_JSCodeWScope: 'State -> JSCodeWScope -> 'State
    abstract member fold_Int32: 'State -> Int32 -> 'State
    abstract member fold_Int64: 'State -> Int64 -> 'State
    abstract member fold_TimeStamp: 'State -> TimeStamp -> 'State
    abstract member fold_MinKey: 'State -> MinKey -> 'State
    abstract member fold_MaxKey: 'State -> MaxKey -> 'State

let private foldElem<'State> (f: IBSONFolder<'State>) (state: 'State) (x: BSON_element) =
    match x with
        | Floatnum n -> f.fold_Floatnum state n
        | UTF8String n -> f.fold_UTF8String state n
        | EmbeddedDoc n -> f.fold_EmbeddedDoc state n
        | ArrayDoc n -> f.fold_ArrayDoc state n
        | BinaryData n -> f.fold_BinaryData state n
        | Undefined n -> f.fold_Undefined state n
        | ObjectID n -> f.fold_ObjectID state n
        | Bool n -> f.fold_Bool state n
        | UTCDateTime n -> f.fold_UTCDateTime state n
        | Null n -> f.fold_Null state n
        | RegEx n -> f.fold_RegEx state n
        | DBPointer n -> f.fold_DBPointer state n
        | JScode n -> f.fold_JScode state n
        | Symbol n -> f.fold_Symbol state n
        | JSCodeWScope n -> f.fold_JSCodeWScope state n
        | Int32 n -> f.fold_Int32 state n
        | Int64 n -> f.fold_Int64 state n
        | MinKey n -> f.fold_MinKey state n
        | MaxKey n -> f.fold_MaxKey state n
        | _ -> failwith "fold what?"

let fold<'State> (f: IBSONFolder<'State>) (state: 'State) (source: BSON_Document) =
    (snd source) |> List.fold (foldElem f) state

type BSONSizer() =
    let size_str v = sizeof<int32> + (fst v) + 1
    let size_cstr (cstr : string) = cstr.Length + 1
    let XX (state : (string * int) list * int) (str : string) (adv : int) =
        ((str, adv) :: (fst state), (snd state) + adv)

    static member private size_doc_impl f (doc : BSON_Document) =
        let fixedsize = sizeof<int32> + sizeof<byte>
        let listsize = doc |> fold f ([],0)
        fixedsize + (snd listsize)

    static member size_doc (doc : BSON_Document) =
        let f = new BSONSizer()
        BSONSizer.size_doc_impl f doc

    static member BSON_size_code_w_s (v : BSON_code_w_s) =
        sizeof<int32> + sizeof<int32> + (snd (fst v)).Length + BSONSizer.size_doc (snd v)

    interface IBSONFolder< (string * int) list * int> with
        member x.fold_Floatnum state v = XX state "floatnum" (1 + size_cstr(fst v) + sizeof<double>)
        member x.fold_UTF8String state v =
            let str = snd (snd v)
            let str_has_trailing_zero = str.Length <> (fst (snd v))
            let plus_one = if str_has_trailing_zero then 0 else 1
            XX state "uf8" (plus_one + size_cstr(fst v) + size_str (snd v))

        member x.fold_EmbeddedDoc state v =
            XX state "embedded" (1 + size_cstr(fst v) + BSONSizer.size_doc_impl x (snd v))

        member x.fold_ArrayDoc state v =
            XX state "array" (1 + size_cstr(fst v) + BSONSizer.size_doc_impl x (snd v))

        member x.fold_BinaryData state v =
            let (name,v) = v
            let (st, ba) = v
            XX state "bindata" (1 + size_cstr(name) + sizeof<int32> + sizeof<byte> + ba.Length)

        member x.fold_Undefined state v = XX state "undef" (1 + size_cstr v)
        member x.fold_ObjectID state v = XX state "objectID" (1 + size_cstr(fst v) + 12*sizeof<byte>)
        member x.fold_Bool state v = XX state "bool" (1 + size_cstr(fst v) + sizeof<byte>)
        member x.fold_UTCDateTime state v = XX state "utc" (1 + size_cstr(fst v) + sizeof<int64>)
        member x.fold_Null state v = XX state "null" (1 + size_cstr v)
        member x.fold_RegEx state v = XX state "regex" (1 + size_cstr(fst3 v) + (snd3 v).Length + (thd3 v).Length)
        member x.fold_DBPointer state v = XX state "dbptr" (1 + size_cstr(fst3 v) + size_str (snd3 v) + 12*sizeof<byte>)
        member x.fold_JScode state v = XX state "jscode" (1 + size_cstr(fst v) + size_str (snd v))
        member x.fold_Symbol state v = XX state "symbol" (1 + size_cstr(fst v) + size_str (snd v))
        member x.fold_JSCodeWScope state v = XX state "jscodews" (1 + size_cstr(fst v) + BSONSizer.BSON_size_code_w_s (snd v))
        member x.fold_Int32 state v = XX state "int32" (1 + size_cstr(fst v) + sizeof<int32>)
        member x.fold_TimeStamp state v = XX state "timestamp" (1 + size_cstr(fst v) + sizeof<int64>)
        member x.fold_Int64 state v = XX state "int64" (1 + size_cstr(fst v) + sizeof<int64>)
        member x.fold_MinKey state v = XX state "minkey" (1 + size_cstr v)
        member x.fold_MaxKey state v = XX state "maxkey" (1 + size_cstr v)


type BSONSerializer() =

    static member private SerializeDoc0 f (bw : BinaryWriter) (doc : BSON_Document) =
        let docsize : int32 = BSONSizer.size_doc doc
        bw.Write docsize
        doc |> fold f bw |> ignore
        bw.Write 0uy

    static member serialize_doc (bw : BinaryWriter) (doc : BSON_Document) =
        let f = new BSONSerializer()
        BSONSerializer.SerializeDoc0 f bw doc

    static member serialize_cstring (bw : BinaryWriter) (cstr : BSON_cstring) =
        bw.Write (System.Text.Encoding.ASCII.GetBytes(cstr))
        bw.Write 0uy

    member private x.serialize_string (bw : BinaryWriter) (str : BSON_string) =
        //let len1 : int32 = 1 + (snd str).Length
        let len2 : int32 = fst str
        //if len1<>len2 then failwith "lenght error in serializing BSON string"
        bw.Write len2
        BSONSerializer.serialize_cstring bw (snd str)

    member private x.serialize_name (bw : BinaryWriter) (name : BSON_e_name) =
        BSONSerializer.serialize_cstring bw name

    interface IBSONFolder<BinaryWriter> with
        member x.fold_Floatnum bw v =
            bw.Write 1uy
            x.serialize_name bw (fst v)
            bw.Write (snd v)
            bw

        member x.fold_UTF8String bw v =
            bw.Write 2uy
            x.serialize_name bw (fst v)
            x.serialize_string bw (snd v)
            bw

        member x.fold_EmbeddedDoc bw v =
            bw.Write 3uy
            x.serialize_name bw (fst v)
            BSONSerializer.SerializeDoc0 x bw (snd v)
            bw

        member x.fold_ArrayDoc bw v =
            bw.Write 4uy
            x.serialize_name bw (fst v)
            BSONSerializer.SerializeDoc0 x bw (snd v)
            bw

        member x.fold_BinaryData bw v =
            bw.Write 5uy
            x.serialize_name bw (fst v)
            let binlen : int32 = (snd (snd v)).Length
            let subtype : byte = (fst (snd v))
            let barray = (snd (snd v))
            bw.Write binlen
            bw.Write subtype
            bw.Write barray.[0 .. binlen-1]
            bw

        member x.fold_Undefined bw v =
            bw.Write 6uy
            x.serialize_name bw v
            bw

        member x.fold_ObjectID bw v =
            bw.Write 7uy
            x.serialize_name bw (fst v)
            bw.Write (snd v).[0 .. 11]
            bw

        member x.fold_Bool bw v =
            bw.Write 8uy
            x.serialize_name bw (fst v)
            bw.Write (if (snd v) then 1uy else 0uy)
            bw

        member x.fold_UTCDateTime bw v =
            bw.Write 9uy
            x.serialize_name bw (fst v)
            bw.Write (snd v)
            bw

        member x.fold_Null bw v =
            bw.Write 10uy
            x.serialize_name bw v
            bw

        member x.fold_RegEx bw v =
            bw.Write 11uy
            x.serialize_name bw (fst3 v)
            BSONSerializer.serialize_cstring bw (snd3 v)
            BSONSerializer.serialize_cstring bw (thd3 v)
            bw

        member x.fold_DBPointer bw v =
            bw.Write 12uy
            x.serialize_name bw (fst3 v)
            x.serialize_string bw (snd3 v)
            bw.Write (thd3 v).[0 .. 11]
            bw

        member x.fold_JScode bw v =
            bw.Write 13uy
            x.serialize_name bw (fst v)
            x.serialize_string bw (snd v)
            bw

        member x.fold_Symbol bw v =
            bw.Write 14uy
            x.serialize_name bw (fst v)
            x.serialize_string bw (snd v)
            bw

        member x.fold_JSCodeWScope bw v =
            bw.Write 15uy
            x.serialize_name bw (fst v)
            let len : int32 = sizeof<int32> + BSONSizer.BSON_size_code_w_s (snd v)
            bw.Write len
            let str = (fst (snd v))
            x.serialize_string bw str
            let doc = (snd (snd v))
            BSONSerializer.SerializeDoc0 x bw doc
            bw

        member x.fold_Int32 bw v =
            bw.Write 16uy
            x.serialize_name bw (fst v)
            bw.Write (snd v)
            bw

        member x.fold_Int64 bw v =
            bw.Write 18uy
            x.serialize_name bw (fst v)
            bw.Write (snd v)
            bw

        member x.fold_TimeStamp bw v =
            bw.Write 17uy
            x.serialize_name bw (fst v)
            bw.Write (snd v)
            bw

        member x.fold_MinKey bw v =
            bw.Write 0xFFuy
            x.serialize_name bw v
            bw

        member x.fold_MaxKey bw v =
            bw.Write 0x7Fuy
            x.serialize_name bw v
            bw

type BSONUtil =
    static member NameOf (elem : BSON_element) : string =
        match elem with
            | Floatnum(n,v) -> n
            | Int32(n,v) -> n
            | _ -> ""

type BSONDeserializer() = 
    let TryParseEElement (br : BinaryReader) =
        let first = br.PeekChar()
        if first < 1 || ((first > 0x12) && (first <> 0x7F) && (first <> 0xFF)) then false else true

    let ParseBoundedString (br : BinaryReader)(len : int) =
        let bytes = br.ReadBytes(len)
        let ret = System.Text.Encoding.ASCII.GetString bytes
        ret

    let ParseString (br : BinaryReader) =
        let len = br.ReadInt32()
        let s = ParseBoundedString br len
        (len, s)

    let ParseEName = MFUtil.ParseCString

    let ParseDouble (br : BinaryReader) = br.ReadDouble()

    let ParseBytes12 (br : BinaryReader) = br.ReadBytes(12)

    let ParseBool (br : BinaryReader) =
        let value = br.ReadByte()
        if (value <> 0uy) && (value <> 1uy) then failwith "error parsing bool"
        value=1uy

    let ParseBinary (br : BinaryReader) =
        let len = br.ReadInt32()
        let subtype = br.ReadByte()
        let arr = br.ReadBytes(len)
        (subtype, arr)

    let ParseFixedByte (br : BinaryReader) (b : byte) =
        if br.ReadByte() <> b then failwith "Error parsing fixed byte"

    let rec ParseEList (br : BinaryReader) (bl : BSON_e_list) : BSON_e_list =
        match TryParseEElement br with
            | true ->
                let elem = ParseEElement br
                let tail = ParseEList br bl
                elem :: tail
            | false -> bl

    and ParseDocument (br : BinaryReader) : BSON_Document =
        let docsize = br.ReadInt32()
        let elist = ParseEList br []
        let lastbyte = br.ReadByte()
        if 0uy <> lastbyte then failwith "missing trailing zero in document."
        (int32 br.BaseStream.Position, elist)

    and ParseCodeWS (br : BinaryReader) =
        let num = br.ReadInt32()
        let s = ParseString br
        let doc = ParseDocument br
        (s, doc)

    and ParseEElement (br : BinaryReader) =
        if not (TryParseEElement br) then failwith "Error parsing e_element"
        let first = br.ReadByte()
        let name = ParseEName br
        match first with
            | 1uy -> Floatnum(name, ParseDouble(br))
            | 2uy -> UTF8String(name, ParseString(br))
            | 3uy -> EmbeddedDoc(name, ParseDocument(br))
            | 4uy -> ArrayDoc(name, ParseDocument(br))
            | 5uy -> BinaryData(name, ParseBinary(br))
            | 6uy -> Undefined(name)
            | 7uy -> ObjectID(name, ParseBytes12(br))
            | 8uy -> Bool(name, ParseBool(br))
            | 9uy -> UTCDateTime(name, br.ReadInt64())
            | 0xAuy -> Null(name)
            | 0xBuy -> RegEx(name, MFUtil.ParseCString(br), MFUtil.ParseCString(br))
            | 0xCuy -> DBPointer(name, ParseString(br), ParseBytes12(br))
            | 0xDuy -> JScode(name, ParseString(br))
            | 0xEuy -> Symbol(name, ParseString(br))
            | 0xFuy -> JSCodeWScope(name, ParseCodeWS(br))
            | 0x10uy -> Int32(name, br.ReadInt32())
            | 0x11uy -> TimeStamp(name, br.ReadInt64())
            | 0x12uy -> Int64(name, br.ReadInt64())
            | 0x7Fuy -> MinKey(name)
            | 0xFFuy -> MaxKey(name)
            | _ -> failwith "internal error in ParseEElement"

    member private x.parse_doc (br : BinaryReader) : BSON_Document =
        ParseDocument br

    static member deserialize (br : BinaryReader) =
        if br.PeekChar() = -1 then emptyDoc
        else
            let f = new BSONDeserializer()
            f.parse_doc br
