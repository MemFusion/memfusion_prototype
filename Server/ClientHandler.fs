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

module ClientHandler

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open FSharp.Control
open Microsoft.FSharp.Collections
open Socket
open MemFusionDB.Logging
open Serialization
open MemFusionDB.MongoMessages
open MemFusionDB.BSONAst
open MemFusionDB.BSON
open MemFusion.DB
open MemFusionDB.BSONUtil
open MemFusion.Util
open MemFusionDB.MsgUtil

exception MsgFormatError of string
exception WrongMsgError of string

let startupWarnings = "startupWarnings\u0000"
let no_replica = "not running with --replSet"

let strOf (str : string) = (str.Length, str)

let presetResponses =
    [ (Int32("ping", 1),       ( [Floatnum("ok", 1.0)] ) );
      (Int32("whatsmyuri", 1), ( [UTF8String("you", (15,"localhost:55443")); Floatnum("ok", 1.0)]) );
      (Int32("ismaster", 1),   ( [Bool("ismaster", true); Int64("maxBsonObjectSize", 16L*1024L*1024L); Int64("maxMessageSizeBytes", 48000000L); UTCDateTime("localTime", 0L); Floatnum("ok", 1.0)]) );
      (Int32("buildinfo", 1),
        ( [ UTF8String("version", (5,"0.0.1"));
            UTF8String("gitVersion", (41, "552fe0d21959e32a5bdbecdc62057db386e4e029c"));
            UTF8String("flavor", (2,"Z1"));
            UTF8String("sysInfo", (125, "windows sys.getwindowsversion(major=6, minor=1, build=7601, platform=2, service_pack='Service Pack 1') BOOST_LIB_VERSION=1_49"));
            Int64("bits", 64L);
            Floatnum("ok", 1.0);
          ]
         )
       );
        (UTF8String("getLog", strOf startupWarnings),
         ([  Int64("totalLinesWritten", 0L);
            ArrayDoc("log", emptyDoc);
            Floatnum("ok", 1.0);
            ]));
        (Floatnum("replSetGetStatus", 1.0),
         ([ Int64("ok", 0L);
            UTF8String("errmsg", strOf no_replica)
         ]));
        (Floatnum("getlasterror", 1.0),
          ([ Int64("n", 1L);
             Int64("connectionId", 200L);
             Int64("wtime", 0L);
             Null("err");
             Int64("ok", 0L);
         ]));

     ]

type Failure =
    | MessageTooShort | MessageHasBadOpcode | SomeError

type SuccessFailure<'T> =
    | Failure of Failure
    | Success of 'T

let ValidateBinaryMsg (binarymsg : byte []) =
    let ValidateOpcode hdr =
        let opcodes = Enum.GetValues(typeof<OpCode>) :?> int array
        match Array.tryFind (fun x -> x = hdr.opCode) opcodes with
            | Some x -> true
            | None -> false
    if binarymsg.Length < int MsgSizes.HeaderSize then Failure(MessageTooShort)
    else
        let header = DeserializeHeader binarymsg.[0 .. int MsgSizes.HeaderSize-1]
        let body = binarymsg.[int MsgSizes.HeaderSize .. ]
        let result = 
            [ ((fun (hdr: MsgHeader, msg : byte[]) -> msg.Length >= int MsgSizes.MinMongoMsgSize), MessageTooShort);
              ((fun (hdr: MsgHeader, msg : byte[]) -> ValidateOpcode hdr), MessageHasBadOpcode)
            ]
            |> List.tryFind (fun f -> not ((fst f) (header, binarymsg)))
        match result with
            | Some tup -> Failure(snd tup)
            | None -> Success(header, body)

let OpcodeOf (header : MsgHeader) = enum<OpCode> header.opCode

let reqIDcount = ref 0

let private acquireNewReqID() =
    Interlocked.Increment reqIDcount

type ClientHandler(socket : Socket, closecb, db : DB) =
    let socket = socket
    let closecb = closecb
    let cts = new CancellationTokenSource()
    let clientport = (socket.RemoteEndPoint :?> IPEndPoint).Port

    let Whatsmyuri_reply header =
        let str = "localhost:" + clientport.ToString()
        let strlen = str.Length
        let doc = (0, [UTF8String("you", (strlen, str)); Floatnum("ok", 1.0)])
        Format_OP_REPLY_Body1 doc

    let CraftReplyHeader (header : MsgHeader) doclist =
        let totsize = doclist |> List.fold (fun state doc -> state + BSONSizer.size_doc doc) 0
        { messageLength = int32 MsgSizes.HeaderSize + int32 MsgSizes.OP_REPLY_BaseSize + totsize;
          requestID = acquireNewReqID();
          responseTo = header.requestID;
          opCode = int32 OpCode.OP_REPLY;
        }

    let numReplyBuffers = 10
    let replyBufferFreeList = new BlockingQueueAgent<int>(numReplyBuffers)
    let replyBuffers = [|0 .. numReplyBuffers-1|] |> Array.map (fun _ -> Array.zeroCreate<byte> (int MsgSizes.MaxMongoMsgSize))

    let SendBackResponse hdr body =
        async {
            let! freeidx = replyBufferFreeList.AsyncGet()  //blocking if empty
            let replyBuffer = replyBuffers.[freeidx]
            logDebug (sprintf "Sending back response using freeidx %d. Header = %A\n  body= %A\n " freeidx hdr body)
            let bytes = SerializeMsg replyBuffer hdr (OP_REPLY(body))
            let written = socket.Send(replyBuffer.[0 .. int bytes-1])
            replyBufferFreeList.SyncAdd freeidx
            if int64 written <> bytes then failwith "Error sending reply."
        } |> Async.RunSynchronously

    let Handle_OP_QUERY header (br : BinaryReader) =
        let queryMsg = Deserialize_OP_QUERY header br
        //logDebug (sprintf "OP_QUERY: %A %A" header queryMsg)
        if ((snd queryMsg.query).Length>0) && ((snd queryMsg.query).Head = Int32("whatsmyuri", 1))
            then Whatsmyuri_reply header |> Some
            else
                OP_QUERY(queryMsg)
                |> db.PostAndReply header

    let Handle_OP_INSERT header (br: BinaryReader) =
        let queryMsg = Deserialize_OP_INSERT header br
        //logDebug (sprintf "OP_INSERT: %A %A" header queryMsg)
        OP_INSERT(queryMsg) |> db.PostAndReply header

    let rec CoreHandleMessage (db : DB) (state : int) (buffer : byte []) =
        match ValidateBinaryMsg buffer with
            | Failure err -> raise (MsgFormatError("Message validation failed with error " + err.ToString()))
            | Success (header, body) ->
                //logDebug (sprintf "CH started handling %u" header.requestID)
                use st = new MemoryStream(body)
                use br = new BinaryReader(st, Text.Encoding.ASCII)

                let maybereply : OP_REPLY option =
                    match OpcodeOf header with
                        | OpCode.OP_REPLY -> raise (WrongMsgError("OP_REPLY msg got from client."))
                        | OpCode.OP_MSG -> raise (WrongMsgError("OP_MSG msg got from client."))
                        | OpCode.OP_UPDATE -> OP_UPDATE(Deserialize_OP_UPDATE header br) |> db.PostAndReply header
                        | OpCode.OP_INSERT -> Handle_OP_INSERT header br
                        | OpCode.OP_GET_MORE -> OP_GET_MORE(Deserialize_OP_GET_MORE header br) |> db.PostAndReply header
                        | OpCode.OP_KILL_CURSORS -> OP_KILL_CURSORS(Deserialize_OP_KILL_CURSORS header br) |> db.PostAndReply header
                        | OpCode.OP_QUERY -> Handle_OP_QUERY header br
                        | OpCode.OP_DELETE -> OP_DELETE(Deserialize_OP_DELETE header br) |> db.PostAndReply header
                        | OpCode.RESERVED -> raise (WrongMsgError("OP_RESERVED msg got from client."))
                        | _ -> raise (WrongMsgError("Unknown msg got from client."))
                match maybereply with
                    | Some(replybody) -> SendBackResponse (CraftReplyHeader header replybody.documents) replybody
                    | None -> ()

    let buffer = Array.zeroCreate<byte> (int MsgSizes.MaxMongoMsgSize)

    let rec loop state = async {
        let bytesRead = socket.Receive(buffer, 0, int MsgSizes.HeaderSize, SocketFlags.None)
        let totalBytes = BitConverter.ToInt32(buffer, 0)
        if bytesRead<>int MsgSizes.HeaderSize then raise(SocketException())
        if totalBytes<0 then raise(SocketException())
        let expected = totalBytes - int MsgSizes.HeaderSize
        let bytesRead2 = socket.Receive(buffer, int MsgSizes.HeaderSize, expected, SocketFlags.None)
        CoreHandleMessage db state buffer.[0 .. totalBytes-1]

        return! loop (state+1)
        }

    let startme = async {
        try
            try
                return! loop 0
            with
                | :? MsgFormatError as ex -> logError ex.Message
                | :? WrongMsgError as ex -> logError ex.Message
                | :? ArgumentNullException as ex -> logError ex.Message
                | :? SocketException as ex -> logError ex.Message
                | :? ObjectDisposedException as ex -> logError ex.Message
                | :? Security.SecurityException as ex -> logError ex.Message
                | e -> logError (sprintf "Unknown exception for client on port %d" clientport)
        finally
            logInfo (sprintf "Closing client on port %d" clientport)
            closecb()
            }

    do
        logTrace "ClientHandler created."
        socket.DontFragment <- true
        [|0 .. numReplyBuffers-1|] |> Array.iter (fun x -> replyBufferFreeList.SyncAdd x)
        ()

    member x.Start() =
        logTrace "ClientHandler.Start"
        Async.Start(startme, cancellationToken = cts.Token)
        { new IDisposable with member x.Dispose() = cts.Cancel(); socket.Close() }

