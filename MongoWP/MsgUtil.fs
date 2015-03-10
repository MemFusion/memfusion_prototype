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

module MemFusionDB.MsgUtil
open MemFusionDB.MongoMessages
open MemFusionDB.BSON
open MemFusionDB.BSONAst

let Format_OP_REPLY header (docs : BSON_Document list) =
    let msgLen = int MsgSizes.HeaderSize + int MsgSizes.OP_REPLY_BaseSize + 
                    (docs
                    |> List.fold (fun state doc -> state + BSONSizer.size_doc doc) 0)
    let msg = {   responseFlags = 0;
        cursorID = 0L;
        startingFrom = 0;
        numberReturned = docs.Length;
        documents = docs
    }
    msg

let Format_OP_REPLY_Cursor1 header cursorID (doc : BSON_Document) =
    let ret = Format_OP_REPLY header [doc]
    { ret with cursorID = cursorID }

let Format_OP_REPLY_Cursor header cursorID startIdx (docs : BSON_Document list) =
    let ret = Format_OP_REPLY header docs
    { ret with cursorID = cursorID; startingFrom = startIdx }

let Format_OP_REPLY_Body1 doc =
    {   responseFlags = 0;
        cursorID = 0L;
        startingFrom = 0;
        numberReturned = 1;
        documents = [ doc ]
    }

let estimateReplySize (body : OP_REPLY) =
    let ret = 100 + (body.documents |> List.fold (fun state doc -> state + (fst doc)) 0)
    ret

let MaxMongoMsgSize = 16u*1024u*1024u

