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

namespace MemFusion.DB
open System.IO
open System.Runtime.Serialization.Formatters.Binary
open System.Collections.Generic
open System.Threading
open System.Runtime.InteropServices
open FSharp.Control

open MemFusionDB.BSONAst
open MemFusionDB.BSON
open MemFusionDB.BSONUtil
open MemFusionDB.Z2
open MemFusionDB.UnZ2
open MemFusionDB.Z2Parser
open MemFusionDB.IO
open MemFusionDB.MongoMessages
open MemFusionDB.MsgUtil
open MemFusionDB.Logging
open MemFusionDB.Collection
open MemFusionDB.Hybrid
open MemFusion.Util
open MemFusionDB.Exceptions

type Agent<'T> = MailboxProcessor<'T>

type DBOP =
    | WithReply of MsgHeader * MsgBody * AsyncReplyChannel<OP_REPLY option>
    //| Noreply of MsgHeader * MsgBody


type InsertBuffers(queuesize, buffersizes) =
    let queuesize = queuesize
    let freebufferslist = new BlockingQueueAgent<byte []>(queuesize)
    let givenaway = new Dictionary<BinaryWriter, byte []>()

type QO =
    | EQ = 1u
    | GT = 2u
    | GTE = 3u
    | IN = 4u
    | LT = 5u
    | LTE = 6u
    | NE = 7u
    | NIN = 8u
    | OR = 9u
    | AND = 10u
    | NOT = 11u
    | NOR = 12u
    | EXISTS = 13u
    | TYPE = 14u
    | MOD = 15u
    | REGEX = 16u
    | WHERE = 17u
    | GEOWITHIN = 18u
    | GEOINTERSECTS = 19u
    | NEAR = 20u
    | NEARSPHERE = 21u
    | ALL = 22u
    | ELEMMATCH = 23u
    | SIZE = 24u
    | DOLLAR = 25u
    | SLICE = 26u
    | GROUP = 27u
    | SUM = 28u
    | MULTIPLY = 29u
    | ADD = 30u
    | AVG = 31u
    | COUNT = 32u

    | START = 9999u
    | END = 9998u
    | AND_ALL = 9997u

//   filter idx,  qpaction,   kids, z2
//type QPfull = uint32 * string * (uint32 * z2)
type QPfull = uint32 * QO * (uint32 * z2)
type QP = QO * uint32

//           idx   instr   z2
type LFT = uint32 * QO * z2

type QueryContext =
    {
        z2ctx : Z2Ctx<QPfull>;
        z2fctx : Z2Ctx<z2>;
        z2rctx : Z2RCtx;
    }
type QUERY = | INSERT | DELETE | SEARCH | AGGREGATE

type DB(dbname : string) =
    let dbname = dbname
    let strOf (str : string) = (str.Length, str)
    let strOf1 (str : string) = (str.Length+1, str)
    let dollarcmd = ".$cmd"
    let queryOperatorsPairs =
        [("$gt", QO.GT); ("$gte", QO.GTE); ("$in", QO.IN); ("$lt", QO.LT); ("$lte", QO.LTE);
         ("$ne", QO.NE); ("$nin", QO.NIN);
         ("$or", QO.OR); ("$and", QO.AND); ("$not", QO.NOT); ("$nor", QO.NOR);
         ("$exists", QO.EXISTS); ("$type", QO.TYPE);
         ("$mod", QO.MOD); ("$regex", QO.REGEX); ("$where", QO.WHERE);
         ("$geoWithin", QO.GEOWITHIN); ("$geoIntersects", QO.GEOINTERSECTS); ("$near", QO.NEAR);
         ("$nearSphere", QO.NEARSPHERE);
         ("$all", QO.ALL); ("$elemMatch", QO.ELEMMATCH); ("$size", QO.SIZE);
         ("$", QO.DOLLAR); ("$slice", QO.SLICE); ("$group", QO.GROUP); ("$sum", QO.SUM);
         ("$multiply", QO.MULTIPLY); ("$add", QO.ADD); ("$avg", QO.AVG);
        ]

    let MaxMongoMsgSize = 16u*1024u*1024u
    let MaxNameNum = 0xFFFFFFu
    let BSON_TO_Z2_ESTIMATE_FACTOR = 1.2
    let insertBuffers = new InsertBuffers(10, MaxMongoMsgSize)
    let cursorMap = new Dictionary<int64, (int * string * BSON_Document [])>()

    let collectionblobs = new HybridBlobHash(null, "collBlobs", 8*MEGABYTE, GIGABYTE)
    let collectionMap = new Dictionary<string, Collection>()

    let names1Map = Array.init< Dictionary<string, uint32> > 256 (fun row -> new Dictionary<string, uint32>())
    let names2Map = Array2D.init< Dictionary<string, uint32> > 256 256 (fun row col -> new Dictionary<string, uint32>())
    let names3PMap = Array2D.init< Dictionary<string, uint32> > 256 256 (fun row col -> new Dictionary<string, uint32>())
    let namesIdxMap = new Dictionary<uint32, string>()

    let RealCollectionName (dbname : string) (collname : string) =
        if collname.StartsWith(dbname) && (collname.Length > dbname.Length) then collname.Substring(dbname.Length+1) else collname

    let querybuffer = Array.zeroCreate (int MaxMongoMsgSize)
    let retbuffer   = Array.zeroCreate (int MaxMongoMsgSize)
    let selectbuffer = Array.zeroCreate (int MaxMongoMsgSize)

    let queryOperators = queryOperatorsPairs |> Map.ofList
    let queryOperatorsInv =
        queryOperatorsPairs
        |> List.map (fun (a,b) -> (b,a))
        |> Map.ofList

    let nameNum = ref (int Z2Constants.UserNamesBase)
    let z2sizeEstimateMisses = ref 0l
    let wastedSpaceZ2 = ref 0l
    let mutable lostSpaceZ2 = 0l

    let decodeQueryOperator name =
        match queryOperators.TryFind name with
            | Some(value) -> value
            | None -> raise(UnknownQueryOperatorName(name))

    let userNameExists (name : string) =
        lock namesIdxMap (fun () -> namesIdxMap.ContainsValue name)

    let acquireName (name : string) : Z2name =
        if name = "_id" then (uint32 Z2Constants._ID) else
            if name.[0]='$' then uint32 (decodeQueryOperator name) else
                let d =     match name.Length with
                            | 1 -> names1Map.[int name.[0]]
                            | 2 -> names2Map.[int name.[0], int name.[1]]
                            | _ -> names3PMap.[int name.[0], int name.[1]]
                lock d (fun () ->
                                if d.ContainsKey(name)
                                    then d.[name]
                                    else
                                        let newnamenum = (uint32 (Interlocked.Increment nameNum))
                                        if newnamenum > MaxNameNum then raise(RanOutOfNameNums(name))
                                        d.Add(name, newnamenum)
                                        lock namesIdxMap (fun () -> namesIdxMap.Add(newnamenum, name))
                                        newnamenum
                                        )

    let decodeNameIdx (nameidx : uint32) =
        let nameidxOD = LanguagePrimitives.EnumOfValue nameidx
        if nameidx=(uint32 Z2Constants._ID) then "_id"
            else if nameidx < (uint32 Z2Constants.ArrayNamesBase) then
                match queryOperatorsInv.TryFind nameidxOD with
                    | Some(value) -> value
                    | None -> raise(UnknownZ2QueryOperatorIndex(nameidx))
            else if nameidx < (uint32 Z2Constants.UserNamesBase) then
                // return this for now, in reality we should return parent's name
                (sprintf "%u" (nameidx - (uint32 Z2Constants.ArrayNamesBase)))
            else    
                if not (namesIdxMap.ContainsKey nameidx) then raise(UnknownNameIdx(nameidx))
                namesIdxMap.[nameidx]

    let WriteZ2Name (zn : Z2name) (cbw : ICustomBW) =
        cbw.Write (uint32 zn)

    let writeQO (qo : QO) (cbw : ICustomBW) =
        cbw.Write (uint32 qo)

    let writeZ2 (z : z2) (cbw : ICustomBW) =
        let dd = z2docdepth z
        let cw = z2cw_serialize (z2cw z)
        cbw.Write (uint32 dd)
        cbw.Write (uint32 cw)
        cbw.Write (z2value z)

    let noaction (z2c : Z2Ctx<z2>) (zs : Zstate<Z2list>) (zcw : z2compword) (v : Z2value) : Zstate<Z2list> =
        let dd = docdepth zs
        (dd, (dd,zcw,v) :: (payload zs), arname zs)

    let append (z2c : Z2Ctx<z2>) (zs : Zstate<Z2list>) (zcw : z2compword) (v : Z2value) : Zstate<Z2list> =
        let dd = docdepth zs
        let cw = z2cw_serialize zcw
        z2c.bw.Write (uint32 dd)
        z2c.bw.Write (uint32 cw)
        z2c.bw.Write v
        (dd, (dd,zcw,v) :: (payload zs), arname zs)

    let DecideQPAction (zname : uint32) =
        let qo : QO = LanguagePrimitives.EnumOfValue zname
        if not (queryOperatorsInv.ContainsKey(qo)) then raise(InternalError("Wrong zname in DecideQPAction"))
        queryOperatorsInv.[qo]

    let QP_noaction (z2c : Z2Ctx<QPfull>) (zs : Zstate<QPfull list>) (zcw : z2compword) (v : Z2value) : Zstate<QPfull list> = zs

    let QPappend (z2c : Z2Ctx<QPfull>) (zs : Zstate<QPfull list>) (zcw : z2compword) (v : Z2value) : Zstate<QPfull list> =
        let dd = docdepth zs
        let cw = z2cw_serialize zcw
        let (LFTidx, qpaction, k, name) =
            match z2cw_type zcw with
                | BSONtype.EmbeddedDoc -> (0u, QO.AND, uint32 v, z2cw_name zcw)
                | BSONtype.ArrayDoc    ->
                    let qop = LanguagePrimitives.EnumOfValue (z2cw_name zcw)
                    let qpinstr =
                        match qop with
                            | QO.IN -> QO.OR
                            | QO.NIN -> QO.NOR
                            | _ -> qop
                    (0u, qpinstr, uint32 v, z2cw_name zcw)
                | _                    ->
                    let LFTidx = z2c.get_lft_idx()
                    let (name, qpinstr) =
                        let name = z2cw_name zcw
                        if (name > uint32 Z2Constants._ID) && (name < (uint32 Z2Constants.ArrayNamesBase))
                            then
                                let parentname = z2cw_name (snd3 (snd (thd3 (List.head (snd3 zs)))))
                                let qop = LanguagePrimitives.EnumOfValue (z2cw_name zcw)
                                let qop = match qop with
                                            | QO.IN -> QO.EQ
                                            | QO.NIN -> QO.EQ
                                            | _ -> qop
                                (parentname, qop)
                            else (name, QO.EQ)
                    (LFTidx, qpinstr, 0u, name)
                    // in case of LFT change query opeator to name
        let zcw = z2cw_replace_name zcw name
        let qp : QPfull = (LFTidx, qpaction, (k, (dd,zcw,v)))
        (dd, qp :: (payload zs), arname zs)

    let OptimizeQP (zlist : QPfull list) =
        let qp_start : QPfull = (0u, QO.START, (0u, (InitialDocDepth,(0u,BSONtype.Undefined,true,0uy),0UL)))
        let qp_end   : QPfull = (0u, QO.END,   (0u, (InitialDocDepth,(0u,BSONtype.Undefined,true,0uy),0UL)))
        qp_end :: zlist
        |> List.rev
        |> List.scan (fun (prev : QPfull option, tf) cur ->
            let tf = match prev with
                        | None -> true
                        | Some(vprev) ->
                            let (a1,b1,(c1,d1)) = vprev
                            let (a2,b2,(c2,d2)) = cur
                            let named1 = z2cw_name (z2cw d1)
                            let named2 = z2cw_name (z2cw d2)
                            let should_keep = not ((named1 = named2) && (b1 = b2))
                            match (b2,c2) with
                                | (QO.AND, 1u) -> false
                                | (QO.OR, 1u) -> false
                                | _ -> true
            (Some(cur), tf)
                      ) (Option<QPfull>.None, false)
        |> List.filter (fun (a,b) -> b)
        |> List.filter (fun (a,b) -> match a with | Some(aa) -> true | None -> false)
        |> List.map (fun (a,b) ->
            match a with
                | Some(aa) -> aa
                | None -> raise(InternalError("Expected value in option QPfull.")))
        |> List.append [qp_start]

    let extract_ltf (zlist : QPfull list) : LFT list =
        zlist
        |> List.filter (fun (a,b,(c,d)) -> a<>0u)
        |> List.map (fun (a,b,(c,d)) -> (a,b,d))

    let extract_qp (zlist : QPfull list) : QP list =
        let qplist = zlist
                        |> List.filter (fun (a,b,(c,d)) -> a=0u)
                        |> List.map (fun (a,b,(c,d)) -> (b,c))
        if qplist.Length = 2
            then qplist.Head :: ((QO.AND_ALL, 0u) :: qplist.Tail)
            else qplist

    let emptyReply = Format_OP_REPLY_Body1 (0, [])
    let specialQueryNames = ["insert"; "delete"; "aggregate"]

    let specialQueryImpl (query : OP_QUERY) =
        let collname = query.fullCollectionName.Substring(0, query.fullCollectionName.Length - dollarcmd.Length)+ "\u0000"
        let doc = snd query.query

        if doc.Length=0 then None
            else
                specialQueryNames
                |> List.map (fun str -> UTF8String(str, strOf (collname)))
                |> List.tryFind (fun elem -> CompareBSONElements doc.Head elem)

    let isSpecialQuery (query : OP_QUERY) =
        let ret = specialQueryImpl query
        match ret with
            | Some(elem) -> true
            | None -> false

    let whichSpecialQuery (query : OP_QUERY) =
        let ret = specialQueryImpl query
        match ret with
        | None -> QUERY.SEARCH
        | Some(value) ->
            match value with
            | UTF8String(name,value) ->
                match name with
                | "insert" -> QUERY.INSERT
                | "delete" -> QUERY.DELETE
                | "aggregate" -> QUERY.AGGREGATE
                | _ -> QUERY.SEARCH
            | _ ->  raise(InternalError("TBD ??? 2"))

    let isCmd (query : OP_QUERY) =
        if query.fullCollectionName.EndsWith(dollarcmd)
            then not (isSpecialQuery query)
            else false

    let decodeDollarName (name : string) =
        if not (name.StartsWith("$")) then raise(UnexpectedInput("missing '$' in group field name."))
        let name = RemoveTrailingZeros name.[1 ..]
        if not (userNameExists name) then raise(DollarNameNotFound)
        acquireName name

    // [UTF8String ("_id", (7, "$state "));
    //
    let ExpectIdStringElem (bsonelem : BSON_element) : Z2name =
        match bsonelem with
        | UTF8String (name,value) ->
            if acquireName name <> (uint32 Z2Constants._ID) then raise(UnexpectedInput("Exepcted _id in first group element."))
            let str = CStringOfBsonString value
            decodeDollarName str
        | _ -> raise(UnexpectedInput("Expected UTF8String in $group first element."))

    //      EmbeddedDoc ("totalPop", (125, [UTF8String ("$sum", (5, "$pop "))]));
    //
    //  Should return:
    //     tgtname: totalPop  (new or existing name)
    //     accname: pop       (existing name)
    //     op: $sum
    //
    let ParseGroupTarget (ed : EmbeddedDoc) (groupName : Z2name) =
        let tgtname = acquireName (fst ed)
        let innerdoc = snd ed
        if (snd innerdoc).Length <> 1 then raise(UnexpectedInput("$group only supports simple accumulators for now."))
        let accdoc = (snd innerdoc).Head
        let (accop, accname) =
            match accdoc with
            | Floatnum(name,value) ->
                let accop = decodeQueryOperator name
                if (accop = QO.SUM) && (value=1.0) then (QO.COUNT,groupName)
                    else raise(UnexpectedInput("$group accumulator should be like: {$sum: \"$pop\"} or {$sum: 1}"))
            | UTF8String (name,value) ->
                let accop = decodeQueryOperator name
                let accname = decodeDollarName (CStringOfBsonString value)
                (accop, accname)
            | _ -> raise(UnexpectedInput("$group accumulator should be like: {$sum: \"$pop\"} or {$sum: 1}"))

        (tgtname, accname, accop)

    let agent (db : DB) = Agent<DBOP>.Start(fun inbox ->
        let rec loop (db : DB) = async {
            let! incoming = inbox.Receive()
            match incoming with
                | WithReply (header,body,replych) ->
                    try
                        if MFLogLevel >= LogLevel.Trace then logTrace (sprintf "DB received %A: %A" header.opCode body)
                        match body with
                            | OP_QUERY q  -> db.Handle_OP_QUERY header q
                            | OP_INSERT q -> db.Handle_OP_INSERT header q false
                            | OP_DELETE q  -> db.Handle_OP_DELETE header q
                            | OP_GET_MORE q  -> db.Handle_OP_GET_MORE header q
                            | _ -> raise(InternalError(sprintf "Message %A to be implemented yet" header.opCode))
                        |> replych.Reply
                    with
                        | UnknownQueryOperatorName(name) -> logError (sprintf "DB: Unknown query operator name %A" name)
                        | BlobHashMapCollision(name,b,c) -> logError (sprintf "DB: BlobHashMapCollision %A" name)
                        | BlobHashNotFound(name, hash)   -> logError (sprintf "DB: BlobHashNotFound %A" name)
                        | InternalError(msg) -> logError (sprintf "DB: internal error '%s'" msg)
                        | UnZ2InternalError(msg) -> logError (sprintf "DB: unz2 internal error '%s'" msg)
                        | RanOutOfNameNums(name) ->  logError (sprintf "DB: RanOutOfNameNums for '%s'" name)
                        | UnknownNameIdx(nameidx) ->  logError (sprintf "DB: UnknownNameIdx %u" nameidx)
                        | UnexpectedInput(msg) -> logError (sprintf "DB: UnexpectedInput exception '%s'" msg)
                        | :? System.OutOfMemoryException as ex -> logError (sprintf "DB: out of memory exception. %s" (ex.ToString()))
                        | _ -> logError "DB: general exception"
                    replych.Reply (Some(Format_OP_REPLY header [(0, [Floatnum("ok", 1.0)] )]))
            return! loop db
        }

        loop db)

    do
//        namesIdxMap.Add(2053u, "name2053")
//        namesIdxMap.Add(2054u, "name2054")
        ()

    member private x.estimate_z2size (doc : BSON_Document) =
            let bsonsize = uint32 ((float (fst doc)) * BSON_TO_Z2_ESTIMATE_FACTOR)
            let ret = bsonsize + (uint32 Z2Constants.z2size - (bsonsize % uint32 Z2Constants.z2size))
            if ret = 0u then raise(InternalError("Z2 size estimated to zero"))
            ret

    member x.HandleAdminCmd header (query : OP_QUERY) =
        let pingReply = ( [Floatnum("ok", 1.0)] )
        let masterReply = (
            [Bool("ismaster", true);
            Int32("maxBsonObjectSize", 16*1024*1024);
            Int32("maxMessageSizeBytes", 48000000);
            Int32("MaxWriteBatchSize", 1000);
            Int32("maxWireVersion", 2);
            Int32("minWireVersion", 2);
            UTCDateTime("localTime", 0L);
            Floatnum("ok", 1.0)
            ])
        let append0 (str : string) =
            let temp = Array.concat [str.ToCharArray(); [| (char) 0y|] ]
            let ret = new string(temp)
            ret

        let strOf0 (str : string) = strOf (append0 str)

        let buildinfoReply = 
                ( [ UTF8String("version", (strOf1 "0.1.0"));
                    UTF8String("gitVersion", (strOf1 "552fe0d21959e32a5bdbecdc62057db386e4e029c"));
                    UTF8String("flavor", (strOf1 "MF"));
                    UTF8String("sysInfo", (strOf1 "windows sys.getwindowsversion(major=6, minor=1, build=7601, platform=2, service_pack='Service Pack 1') BOOST_LIB_VERSION=1_49"));
                    Int64("bits", 64L); Floatnum("ok", 1.0); ] )
        let getLogReply = ([  Int64("totalLinesWritten", 0L); ArrayDoc("log", emptyDoc); Floatnum("ok", 1.0); ])
        let replicaReply = ([ Int64("ok", 0L); UTF8String("errmsg", strOf "not running with --replSet") ])
        //let getLastErrorReply = ([ Int64("n", 1L); Int64("connectionId", 200L); Int64("wtime", 0L); Null("err"); Int64("ok", 0L); ])
        let getLastErrorReply = ([ Int64("ok", 0L); ])

        // replies to Insert is { ok:1, n:1 }   // Int32

        let questions = [ (Int32("ping", 1),       pingReply );
                          (Int32("ismaster", 1), masterReply );
                          (Floatnum("isMaster", 1.0), masterReply );
                          (Int32("buildinfo", 1), buildinfoReply );
                          (UTF8String("getLog", strOf "startupWarnings\u0000"), getLogReply );
                          (Floatnum("replSetGetStatus", 1.0), replicaReply );
                          (Floatnum("getlasterror", 1.0), getLastErrorReply );
                          (Int32("getlasterror", 1), getLastErrorReply );
                        ]
        let answer =
            try
                match List.tryFind (
                                    fun (elem,response) ->
                                        elem = (snd query.query).Head) questions with
                                            | None ->
                                                logError (sprintf "Don't know how to reply to header=%A  body=%A" header query)
                                                emptyReply
                                            | Some(elem, response) -> Format_OP_REPLY_Body1 (0, response)
            with
            | ex -> logError (sprintf "exception: %A" ex.Message); emptyReply

        if MFLogLevel >= LogLevel.Trace then logTrace (sprintf "Canned response is %A" answer)
        answer

    member private x.serialize_lft_qp (lft_list : LFT list) (qp_list : QP list) (bw : ICustomBW) =
        let startBytes = bw.BytesWritten()
        lft_list
        |> List.iter (fun lft ->
            bw.Write (fst3 lft) // uint32
            bw.Write (uint32 (snd3 lft)) // uint32
            bw.Write 0UL // uint64 pad
            bw.Write (fst3 (thd3 lft))
            bw.Write (z2cw_serialize ((snd3 (thd3 lft))))
            bw.Write (thd3 (thd3 lft)))
        let lftBytes = bw.BytesWritten() - startBytes
        qp_list
        |> List.iter (fun (a,b) -> bw.Write (uint32 a); bw.Write b)
        let qpBytes = bw.BytesWritten() - lftBytes
        (lftBytes, qpBytes)

    member private x.GetQueryContext(filterPresent, collection : Collection) =
        let querybw = new CustomBW2(querybuffer)
        let selectbw = new CustomBW2(selectbuffer)
        let z2ctx = new Z2Ctx<QPfull>(querybw, collectionblobs, acquireName, (QPappend,QP_noaction))
        let z2fctx = new Z2Ctx<z2>(selectbw, collectionblobs, acquireName, (append,noaction))
        let z2rctx = new Z2RCtx(collectionblobs, decodeNameIdx)
        { z2ctx = z2ctx; z2fctx = z2fctx ; z2rctx = z2rctx }

    member x.Handle_MemFusion_Query header (query : OP_INSERT) =
        Format_OP_REPLY header [(0, [Floatnum("ok", 1.0)] )]
        |> Some

    member x.Handle_OP_QUERY_User header (query : OP_QUERY) =
        let realname = RealCollectionName dbname query.fullCollectionName
        let contains = lock collectionMap (fun () -> collectionMap.ContainsKey(realname))
        match contains with
            | false ->
                logInfo (sprintf "Collection '%s' not found" query.fullCollectionName)
                Format_OP_REPLY header [(0, [Floatnum("ok", 1.0)] )]
                |> Some
            | true ->
                let collection = lock collectionMap (fun () -> collectionMap.[realname])
                let queryCtx = x.GetQueryContext(query.returnFieldsSelector.IsSome, collection)
                let z2selector = match query.returnFieldsSelector with
                                    | Some selector -> Z2<z2>.z2_doc selector queryCtx.z2fctx
                                    | None -> []
                let z2list = Z2<QPfull>.z2_doc query.query queryCtx.z2ctx |> OptimizeQP
                let lft_list = extract_ltf z2list
                let qp_list = extract_qp z2list
                let selectBytes = queryCtx.z2fctx.BytesWritten()
                let (ltfBytes, qpBytes) = x.serialize_lft_qp lft_list qp_list queryCtx.z2ctx.bw
                let queryBytes = queryCtx.z2ctx.BytesWritten()
                collection.execute_query 0UL retbuffer selectbuffer selectBytes querybuffer ltfBytes qpBytes
                |> Unz2.z2bytes_to_BSON queryCtx.z2rctx
                |> Format_OP_REPLY header
                |> Some

    member x.Handle_OP_DELETE header (query : OP_DELETE) =
        Format_OP_REPLY header [(0, [Floatnum("ok", 0.0)] )]
        |> Some

    member x.Handle_OP_GET_MORE header (query : OP_GET_MORE) =
        logInfo (sprintf "OP_GET_MORE: \n %A \n %A \n" header query)
//        Format_OP_REPLY header []
//        |> Some
        lock cursorMap (fun () ->
            match cursorMap.ContainsKey(query.cursorID) with
                | false -> logInfo (sprintf "Cursor %u not found in OP_GET_MORE" query.cursorID); None
                | true ->
                    let (idx,colname,data) = cursorMap.[query.cursorID]
                    match idx<data.Length with
                        | false -> logInfo (sprintf "Cursor %u out of bounds" query.cursorID); None
                        | true ->
                            let isneg = query.numberToReturn < 0
                            let numResults = abs(query.numberToReturn)
                            let xx = min (data.Length) (idx+numResults)
                            cursorMap.[query.cursorID] <- (xx,colname,data)
                            //data.[idx+1 .. ]
                            //|> Array.toList
                            [data.[idx+1]]
                            |> Format_OP_REPLY_Cursor header query.cursorID (idx+1)
                            |> Some
            )

    member x.Handle_OP_QUERY header (query : OP_QUERY) =
        if isCmd query then x.HandleAdminCmd header query |> Some
            else
                if (snd query.query).Length = 0
                then
                    // TBD: better to return an empty response?
                    Format_OP_REPLY header [(0, [Floatnum("ok", 1.0)] )]
                    |> Some
                else
                    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
                    let special = whichSpecialQuery query 
                    let ret = match special with
            //                    | QUERY.INSERT -> x.Handle_OP_QUERY_INSERT header query
            //                    | QUERY.DELETE -> x.Handle_OP_QUERY_REMOVE header query
                                | QUERY.SEARCH -> x.Handle_OP_QUERY_User header query
                                | _ -> x.Handle_SPECIAL_QUERY header query special
                    stopWatch.Stop()
                    logInfo (sprintf "Query took %u ms." stopWatch.ElapsedMilliseconds)
                    ret

    member x.Handle_OP_QUERY_REMOVE header (query : OP_QUERY) =
        raise(InternalError("TBD"))

    member x.Handle_SPECIAL_QUERY header (query : OP_QUERY) special =
        let docarray = (snd query.query) |> List.toArray
        let collname : string =
            match docarray.[0] with
                | UTF8String (n,v) ->
                    //if n <> "insert" then raise(InternalError("expected 'insert' in first elem of doc list for query-insert."))
                    (snd v).Substring(0, (fst v)-1)
                | _ -> raise(InternalError("expected string in first elem of doc list for query-insert."))
        let ad = docarray.[1]
        let doclist = match ad with
                        | ArrayDoc arraydoc -> snd (snd arraydoc)
                        | _ -> raise(InternalError("Unexpected type in special query,"))
        match special with
            | QUERY.AGGREGATE -> x.Handle_Aggregate header query collname doclist
            | QUERY.INSERT -> x.Handle_Batch_Insert header query collname doclist
            | QUERY.SEARCH -> raise(InternalError("Unexpected search query in special query."))
            | QUERY.DELETE -> raise(InternalError("Not implemented: delete special query."))

    member x.Handle_Aggregate header query collname doclist =
//        use fs = new System.IO.FileStream("C:\\temp\\ciaoblobs.bin", FileMode.Create)
//        collectionblobs.Persist(fs)
//        collectionblobs.Clear()
//        fs.Seek(0L, SeekOrigin.Begin) |> ignore
//        collectionblobs.Reload(fs)

        let contains = lock collectionMap (fun () -> collectionMap.ContainsKey(collname))
        match contains with
            | false -> logInfo (sprintf "Collection '%s' not found" collname); None
            | true ->
                if doclist.Length > 2 then raise(InternalError("$group with optional $sort only are currently supported in 'aggreate' commands."))
                let collection = lock collectionMap (fun () -> collectionMap.[collname])
                let sort = doclist.Length = 2
                match doclist.Head with
                | EmbeddedDoc doc ->
                    let doclist = snd (snd doc)
                    if doclist.Length <> 1 then raise(InternalError("Only one $group clause is currently supported in 'aggreate' commands.(2)"))
                    match doclist.Head with
                    | EmbeddedDoc doc ->
                        if (fst doc) <> "$group" then raise(InternalError("Only $group is currently supported in 'aggreate' commands."))
                        x.Handle_Aggregate_Group header collection (snd doc) sort
                    | _ -> raise(InternalError("Unknown bson type in 'aggreate' commands."))
                | _ -> raise(InternalError("Unknown bson type in 'aggreate' commands."))

//                Format_OP_REPLY header [(0, [Int32("ok", 1) ] )]
//                |> Some

    // for now expect something like:
    // [UTF8String ("_id", (7, "$state "));
    //      EmbeddedDoc ("totalPop", (125, [UTF8String ("$sum", (5, "$pop "))]));
    //      EmbeddedDoc ("numCities", (155, [Floatnum ("$sum", 1.0)]))])
    //
    //  group:  $state
    //  tgtname: totalPop
    //  accname: pop
    //  op: $sum
    //
    member x.Handle_Aggregate_Group header collection doc sort =
        try
            logInfo (sprintf "Aggregate: \n\n %A \n\n" doc)
            let elist = BsonElemList doc
            if elist.Length < 2 then raise(InternalError("Aggregate $group expects at least _id and one accumulator."))
            let id_name = ExpectIdStringElem (elist.Head)
            let accs = NoHead elist
            let querybw = new CustomBW2(querybuffer)

            WriteZ2Name id_name querybw

            //  ... ->  groupName, [(targetName, AccName, AccOperation)]
            accs   // every element is expected to be an EmbeddedDoc like above
            |> List.map (fun ed ->
                            match ed with
                                | EmbeddedDoc ed -> ParseGroupTarget ed id_name
                                | _ -> raise(UnexpectedInput("Unrecognized element in $grop.")))
            |> List.iter (fun (tgtname, accname, accop) ->
                            WriteZ2Name tgtname querybw
                            WriteZ2Name accname querybw
                            writeQO accop querybw
                          )

            let z2rctx = new Z2RCtx(collectionblobs, decodeNameIdx)
            let queryBytes = (querybw :> ICustomBW).BytesWritten()
            collection.execute_aggregation 0UL retbuffer querybuffer queryBytes sort 
            |> Unz2.z2bytes_to_BSON z2rctx
            |> x.store_to_cursor header 0L (collection.getname())
            |> Format_OP_REPLY_Cursor1 header 0L
            //|> Format_OP_REPLY header
            |> Some
        with
            | DollarNameNotFound -> None

    member x.store_to_cursor header cursorID collname (doclist : BSON_Document list) : BSON_Document =
        lock cursorMap (fun () ->
                            cursorMap.Clear()
                            cursorMap.Add(cursorID, (1, collname, doclist |> List.toArray))
                       )
        let elemlist =
            doclist
            |> List.mapi (fun idx doc -> EmbeddedDoc(idx.ToString(), doc))

        (0, [
            EmbeddedDoc("Cursor",
                (0, [
                    Int64("id", cursorID);
                    UTF8String("ns", strOf collname);
                    ArrayDoc("firstBatch", (0, elemlist))
                ])
            );
            Floatnum("ok", 1.0)
        ])
//        ArrayDoc("firstBatch",
//            EmbeddedDoc(0,
//                [
//                EmbeddedDoc(0,
//                    [  UTF8String("_id", (2, "wv"));
//                       Int32("totalPop", 1793477)
//                    ]
//                );
//                EmbeddedDoc(0,
//                    [  UTF8String("_id", (2, "wv"));
//                       Int32("totalPop", 1793477)
//                    ]
//                )
//                ]
//             )
//        )

    member x.Handle_Batch_Insert header query collname doclist =
        doclist
        |> List.iter (fun somedoc ->
                match somedoc with
                    | EmbeddedDoc embdoc -> x.Handle_OP_INSERT_Impl header collname (snd embdoc)
                    | _ -> logError (sprintf "Expected EmbeddedDoc in query insert"))

        Format_OP_REPLY header [(0, [Int32("ok", 1); Int32("n", doclist.Length)] )]
        |> Some


    member x.Handle_OP_INSERT header query needsreply =
        let collname = query.fullCollectionName
        let realname = RealCollectionName dbname collname
        if realname = "memfusion" then x.Handle_MemFusion_Query header query
        else
            query.documents
            |> List.iter (fun doc -> x.Handle_OP_INSERT_Impl header collname doc)

            //let docs = [(0, [ Int32("ok", 1) ] )]
            let ret = Format_OP_REPLY header [(0, [Int32("ok", 1); Int32("n", query.documents.Length)] )]
            if needsreply then Some(ret) else None

    member x.Handle_OP_INSERT_Impl header collname doc =
        let realname = RealCollectionName dbname collname
        let collection =
            lock collectionMap
                (fun () ->
                    match collectionMap.ContainsKey(collname) with
                        | false -> if not (collectionMap.ContainsKey(realname)) then
                                    logInfo (sprintf "Collection '%s' not found. Creating it." collname)
                                    collectionMap.Add(realname, new Collection(collname))
                        | true -> ()
                    collectionMap.[realname])

        let z2ctx = new Z2Ctx<z2>(new DummyBW(), collectionblobs, acquireName, (noaction, noaction))
        let z2doclist = Z2<z2>.z2_doc doc z2ctx
        let z2docsize = (uint32 z2doclist.Length) * (uint32 Z2Constants.z2size)
        let cbw = collection.FS_AcquireInsertBuffer 0UL (z2docsize)

        z2doclist |> List.iter (fun z2elem -> writeZ2 z2elem cbw)

        collection.FS_ReleaseInsertBuffer 0UL cbw |> ignore

    member x.PostAndReply (header : MsgHeader) (query : MsgBody) =
        (agent x).PostAndReply(fun reply -> DBOP.WithReply(header, query, reply))

    member x.z2encode (doc :BSON_Document) : Z2list =
        let z2ctx = new Z2Ctx<z2>(new DummyBW(), collectionblobs, acquireName, (noaction, noaction))
        let z2doclist = Z2<z2>.z2_doc doc z2ctx
        z2doclist

    member x.z2decode (z2list : Z2list) : BSON_Document =
        let z2rctx = new Z2RCtx(collectionblobs, decodeNameIdx)
        Unz2.z2_to_BSON z2rctx z2list
