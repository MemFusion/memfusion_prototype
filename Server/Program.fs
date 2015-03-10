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


open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open FSharp.Control
open Microsoft.FSharp.Collections
open MemFusionDB.MongoMessages
open System.Runtime.Serialization.Formatters.Binary
open Socket
open ClientHandler
open MemFusionDB.Logging
open MemFusion.DB
open MemFusionDB.CoreProxy


type Server() =
    static member Start(db, maxSockets, hostname:string, ?port) =
        let ipAddress = Dns.GetHostEntry(hostname).AddressList.[0]
        Server.Start(db, maxSockets, ipAddress, ?port = port)

    static member Start(db, maxSockets, ?ipAddress, ?port) =
        let ipAddress = defaultArg ipAddress IPAddress.Any
        let port = defaultArg port 80
        let endpoint = IPEndPoint(ipAddress, port)
        let cts = new CancellationTokenSource()
        let freelist = new BlockingQueueAgent<int>(maxSockets)
        let listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        let activeClients = Map.empty<int, ClientHandler>

        let binded =
            try
                listener.Bind(endpoint)
                listener.Listen(int SocketOptionName.MaxConnections)
                true
            with
                | :? System.Net.Sockets.SocketException as ex -> logError (sprintf "Exception: %A." ex.Message) ; false
                | _ -> logError "Unknown exception opening socket." ; false
        match binded with
            | false -> logFatal "Terminating." ; { new IDisposable with member x.Dispose() = listener.Close() }
            | true ->
                logInfo (sprintf "Started listening on port %d" port)
                [|0 .. maxSockets-1|] |> Array.iter (fun x -> freelist.SyncAdd x)

                let rec loop(activeClients : Map<int, ClientHandler>) = async {
                    logInfo "Waiting for request ..."
                    let! socket = listener.AsyncAccept()
                    let! freeIdx = freelist.AsyncGet()   // blocking if empty
                    let FreeMe() = logInfo (sprintf "Socket %d released" freeIdx)
                                   freelist.SyncAdd freeIdx
                    let client = new ClientHandler(socket, FreeMe, db)

                    let clientaddr = (socket.RemoteEndPoint :?> IPEndPoint).Address.ToString()
                    let clientport = (socket.RemoteEndPoint :?> IPEndPoint).Port
                    logInfo (sprintf "Socket %A connected from %s:%u" freeIdx clientaddr clientport)
                    client.Start() |> ignore
                    return! loop(activeClients.Add(freeIdx, client))
                }

                Async.Start(loop(activeClients), cancellationToken = cts.Token)
                { new IDisposable with member x.Dispose() = cts.Cancel(); listener.Close() }

// set up a type to represent the options
[<EntryPoint>]
let main argv =
    logInfo (sprintf "MemFusion (c) 2014-2015 by Benedetto Proietti")
    logInfo "MemFusion is a fast aggregation engine binary compatible with MongoDB"
    logInfo "For questions, bugs, and licensing information: info@memfusion.com\n\n"

    if argv.Length>0 && 0=String.Compare(argv.[0], "/trace") then MFLogLevel <- LogLevel.Trace
    if argv.Length>0 && 0=String.Compare(argv.[0], "/debug") then MFLogLevel <- LogLevel.Debug

    logInfo (sprintf "LogLevel = %s" (MFLogLevel.ToString()))
    let datapath = "."
    CoreProxy.InitializeQueryEngine(64u, 1000u*1000u, 100u*1024u*1024u, 5u*1000u, datapath)
    let db = new DB("test")
    let disposable = Server.Start(db, maxSockets = 5, port = 27017)

    logInfo "Press return to exit..."
    Console.ReadLine()
        |> ignore

    disposable.Dispose()

    0 // return an integer exit code
