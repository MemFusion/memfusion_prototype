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

module MemFusionDB.MongoMessages

open System
open System.IO
open System.Runtime.Serialization.Formatters.Binary
open MemFusionDB.BSONAst


type MsgHeader =
    {
        messageLength : int32
        requestID     : int32
        responseTo    : int32
        opCode        : int32
    }

and OP_UPDATE =
    {
        ZERO                : int32
        fullCollectionName  : BSON_cstring
        flags               : int32
        selector            : BSON_Document
        update              : BSON_Document
    }

and OP_INSERT =
    {
        flags               : int32
        fullCollectionName  : BSON_cstring
        documents           : BSON_Document list
    }

and OP_QUERY =
    {
        flags               : int32
        fullCollectionName  : BSON_cstring
        numberToSkip        : int32
        numberToReturn      : int32
        query               : BSON_Document
        returnFieldsSelector : BSON_Document option
    }

and OP_GET_MORE =
    {
        ZERO                : int32
        fullCollectionName  : BSON_cstring
        numberToReturn      : int32
        cursorID            : int64
    }

and OP_DELETE =
    {
        DZERO               : int32
        fullCollectionName  : string
        flags               : int32
        selector            : BSON_Document
    }

and OP_KILL_CURSORS =
    {
        ZERO                : int32
        numberOfCursorIDs   : int32
        cursorIDs           : int64 list
    }

and OP_MSG =
    {
        message             : BSON_cstring
    }

and OP_REPLY =
    {
        responseFlags       : int32
        cursorID            : int64
        startingFrom        : int32
        numberReturned      : int32
        documents           : BSON_Document list
    }

and MsgBody =
    | OP_UPDATE of OP_UPDATE
    | OP_INSERT of OP_INSERT
    | OP_QUERY of OP_QUERY
    | OP_GET_MORE of OP_GET_MORE
    | OP_DELETE of OP_DELETE
    | OP_KILL_CURSORS of OP_KILL_CURSORS
    | OP_MSG of OP_MSG
    | OP_REPLY of OP_REPLY

type OpCode =
    | OP_REPLY  = 1
    | OP_MSG    = 1000
    | OP_UPDATE = 2001
    | OP_INSERT = 2002
    | RESERVED  = 2003
    | OP_QUERY  = 2004
    | OP_GET_MORE = 2005
    | OP_DELETE  = 2006
    | OP_KILL_CURSORS = 2007


type MsgSizes =
    | HeaderSize        = 16
    | OP_REPLY_BaseSize = 20
    | MaxMongoMsgSize   = 20000000
    | MinMongoMsgSize   = 40

// type Msg = MsgHeader * MsgBody

let DeserializeHeader(bheader : byte []) =
    let h = { messageLength = BitConverter.ToInt32(bheader, 0);
              requestID = BitConverter.ToInt32(bheader, 4);
              responseTo = BitConverter.ToInt32(bheader, 8);
              opCode = BitConverter.ToInt32(bheader, 12) }
    h

