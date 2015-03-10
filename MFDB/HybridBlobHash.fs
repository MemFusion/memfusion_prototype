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

namespace MemFusionDB.Hybrid
open System
open System.Threading
open MemFusionDB.Z2
open MemFusionDB.Exceptions
open MemFusionDB.Persistence

exception BlobHashMapCollision of string * byte[] * byte[]
exception BlobHashNotFound of string * HashType


type HybridBlobHash(device, name : string, maxNumHashes, maxSize) =
    let data = new System.Collections.Generic.Dictionary<HashType, byte []>()
    let name = name

    member x.contains_hash (hash : HashType) =
        data.ContainsKey hash

    member x.add_hash (hash : HashType) (bytes : byte []) =
        lock data (fun () ->
                        if data.ContainsKey hash
                        then
                            let there = data.[hash]
                            if there <> bytes then raise(BlobHashMapCollision(name, there, bytes))
                        else data.Add(hash, bytes)
                        )

    member x.decode_hash hash =
        lock data (fun () ->
                    if data.ContainsKey hash
                        then data.[hash]
                        else raise(BlobHashNotFound(name, hash))
                    )

    member x.Clear() = lock data (fun () -> data.Clear())

//    interface IPersistence with
    member x.Persist (filestream : System.IO.FileStream) =
        lock data (fun () ->
            let bytes = BitConverter.GetBytes(data.Count)
            filestream.Write(bytes, 0, sizeof<int>)

            for KeyValue(hash,blob) in data do
                let bytes = BitConverter.GetBytes(hash)
                filestream.Write(bytes, 0, sizeof<HashType>)
                let bytes = BitConverter.GetBytes(blob.Length)
                filestream.Write(bytes, 0, sizeof<int>)
                filestream.Write(blob, 0, blob.Length)
        )

    member x.Reload (filestream : System.IO.FileStream) =
        lock data (fun () ->
            let br = new System.IO.BinaryReader(filestream)
            let count = br.ReadInt32()
            [| 0 .. count-1|]
            |> Array.iter (fun _ ->
                let hash = br.ReadUInt64()
                let bloblen = br.ReadInt32()
                let blob = br.ReadBytes(bloblen)
                data.Add(hash, blob)
            )
        )