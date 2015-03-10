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

module MemFusion.DBCore

#if USE_THIS_OR_NOT_DONT_KNOW___YET

open System.IO
open System.Runtime.Serialization.Formatters.Binary
open System.Collections.Generic
open System.Threading
open FSharp.Control

open MemFusionDB.BSONAst
open MemFusionDB.BSON
open MemFusionDB.Z2
open MemFusionDB.UnZ2
open MemFusionDB.Z2Parser
open MemFusionDB.IO
open MemFusionDB.MongoMessages
open MemFusionDB.MsgUtil
open MemFusionDB.Logging
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
    | START = 999u
    | END = 998u
    | AND_ALL = 997u

//   filter idx,  qpaction,   kids, z2
//type QPfull = uint32 * string * (uint32 * z2)
type QPfull = uint32 * QO * (uint32 * z2)
type QP = QO * uint32

//           idx   instr   z2
type LFT = uint32 *  QO * z2

type QueryContext =
    {
        z2ctx : Z2Ctx<QPfull>;
        z2fctx : Z2Ctx<z2>;
        z2rctx : Z2RCtx;
    }

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
        ("$", QO.DOLLAR); ("$slice", QO.SLICE)
    ]

let MaxNameNum = 0xFFFFFFu
let BSON_TO_Z2_ESTIMATE_FACTOR = 1.2

let collectionblobs = new HybridBlobHash(null, "collBlobs", 8*MEGABYTE, GIGABYTE)

let names1Map = Array.init< Dictionary<string, uint32> > 256 (fun row -> new Dictionary<string, uint32>())
let names2Map = Array2D.init< Dictionary<string, uint32> > 256 256 (fun row col -> new Dictionary<string, uint32>())
let names3PMap = Array2D.init< Dictionary<string, uint32> > 256 256 (fun row col -> new Dictionary<string, uint32>())
let namesIdxMap = new Dictionary<uint32, string>()

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

let acquireName (name : string) =
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

let isInsertInQuery (query : OP_QUERY) =
    let collname = query.fullCollectionName.Substring(0, query.fullCollectionName.Length - dollarcmd.Length)+ "\u0000"
    let doc = snd query.query
    let dh = doc.Head
    let expected = UTF8String("insert", strOf (collname))
    if doc.Length=0 then false
        else
            let comp = CompareBSONElements doc.Head expected
            comp

let isCmd (query : OP_QUERY) =
    if query.fullCollectionName.EndsWith(dollarcmd)
        then not (isInsertInQuery query)
        else false


    member x.z2encode (doc :BSON_Document) : Z2list =
        let z2ctx = new Z2Ctx<z2>(new DummyBW(), collectionblobs, acquireName, (noaction, noaction))
        let z2doclist = Z2<z2>.z2_doc doc z2ctx
        z2doclist

    member x.z2decode (z2list : Z2list) : BSON_Document list =
        let z2rctx = new Z2RCtx(collectionblobs, decodeNameIdx)
        Unz2.z2_to_BSON z2rctx z2list

#endif


