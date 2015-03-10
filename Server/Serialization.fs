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

module Serialization

open System
open System.IO
open MemFusionDB.BSON
open MemFusionDB.MongoMessages
open System.Runtime.Serialization.Formatters.Binary
open MemFusion.Util
open MemFusionDB.Logging
open MemFusionDB.BSONAst

let SerializeHeader (bw : BinaryWriter) (header : MsgHeader) =
    bw.Write header.messageLength
    bw.Write header.requestID
    bw.Write header.responseTo
    bw.Write header.opCode

let SerializeBody (bw : BinaryWriter) (msg : MsgBody) =
    match msg with
        | OP_REPLY msg ->
            bw.Write msg.responseFlags
            bw.Write msg.cursorID
            bw.Write msg.startingFrom
            bw.Write msg.numberReturned
            msg.documents |> Seq.iter (fun doc -> BSONSerializer.serialize_doc bw doc)
        | OP_MSG msg -> bw.Write msg.message
        | OP_UPDATE msg ->
            bw.Write 0
            bw.Write msg.fullCollectionName
            bw.Write msg.flags
            BSONSerializer.serialize_doc bw msg.selector
            BSONSerializer.serialize_doc bw msg.update
        | OP_INSERT msg ->
            bw.Write msg.flags
            bw.Write msg.fullCollectionName
            msg.documents |> Seq.iter (fun doc -> BSONSerializer.serialize_doc bw doc)
        | _ -> failwith "SerializedBody: What are you trying to serialize??"

let SerializeMsg (buffer : byte[]) (header : MsgHeader) (msg : MsgBody) =
    let st = new MemoryStream(buffer)
    let bw = new BinaryWriter(st)
    SerializeHeader bw header
    SerializeBody bw msg
    bw.BaseStream.Position

let Deserialize_OP_MSG (header : MsgHeader) (br : BinaryReader) =
    {
        message = MFUtil.ParseCString br;
    }

let Deserialize_OP_UPDATE (header : MsgHeader) (br : BinaryReader) =
    {   ZERO  = br.ReadInt32();
        fullCollectionName = MFUtil.ParseCString br;
        flags = br.ReadInt32();
        selector = BSONDeserializer.deserialize br;
        update = BSONDeserializer.deserialize br;
    }

let Deserialize_OP_DELETE (header : MsgHeader) (br : BinaryReader) =
    {   DZERO  = br.ReadInt32();
        fullCollectionName = MFUtil.ParseCString br;
        flags = br.ReadInt32();
        selector = BSONDeserializer.deserialize br;
    }

let Deserialize_OP_GET_MORE (header : MsgHeader) (br : BinaryReader) =
    {   ZERO  = br.ReadInt32();
        fullCollectionName = MFUtil.ParseCString br;
        numberToReturn = br.ReadInt32();
        cursorID = br.ReadInt64();
    }

let Deserialize_OP_KILL_CURSORS (header : MsgHeader) (br : BinaryReader) =
    let sdsd = [|0 .. 12-1|]
    let ZERO  = br.ReadInt32()
    let numberOfCursorIDs = br.ReadInt32()

    {   ZERO  = ZERO;
        numberOfCursorIDs = numberOfCursorIDs;
        cursorIDs = [|0 .. numberOfCursorIDs-1|]
                    |> Array.map (fun x -> br.ReadInt64())
                    |> Array.toList
    }

let Deserialize_OP_QUERY (header : MsgHeader) (br : BinaryReader) =
    {   flags = br.ReadInt32();
        fullCollectionName = MFUtil.ParseCString br;
        numberToSkip = br.ReadInt32();
        numberToReturn = br.ReadInt32();
        query = BSONDeserializer.deserialize br
        returnFieldsSelector =
            let x = BSONDeserializer.deserialize br
            if x = emptyDoc then None else Some(x)
    }

let Deserialize_OP_INSERT (header : MsgHeader) (br : BinaryReader) =
    //logInfo (sprintf "Incoming OP_INSERT %u bytes from Thread %A" header.messageLength System.Threading.Thread.CurrentThread.ManagedThreadId)

    let flags = br.ReadInt32();
    let fullname = MFUtil.ParseCString br
    let bwPositionThreshold = int64 header.messageLength - int64 MsgSizes.HeaderSize
    let docs = seq {
                    while (br.BaseStream.Position < bwPositionThreshold) && (br.PeekChar() <> -1) do
                        yield BSONDeserializer.deserialize br
                        } |> Seq.toList
    {   flags = flags;
        fullCollectionName = fullname;
        documents = docs
    }

