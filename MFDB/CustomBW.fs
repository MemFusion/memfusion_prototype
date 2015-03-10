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

namespace MemFusionDB.IO
open System.IO
open Microsoft.FSharp.NativeInterop
open System.Runtime.InteropServices
#nowarn "9"

type ICustomBW =
    abstract member Write: uint32 -> unit
    abstract member Write: uint64 -> unit
    abstract member Write: int32  -> unit
    abstract member BytesWritten: unit -> uint32

type NativeCustomBW(nint : nativeint) =
    let nint = nint
    let mutable offset = 0u

    do
        if nint = nativeint 0 then failwith "booooooooooooooooh"

    interface ICustomBW with
        member x.Write (value : int32) =
            let intaddr = nint + nativeint(offset)
            let nptr = NativePtr.ofNativeInt intaddr
            NativePtr.write<int32> nptr value
            offset <- offset + 4u

        member x.Write (value : uint32) =
            let intaddr = nint + nativeint(offset)
            let nptr = NativePtr.ofNativeInt intaddr
            NativePtr.write<uint32> nptr value
            offset <- offset + 4u

        member x.Write (value : uint64) =
            let intaddr = nint + nativeint(offset)
            let nptr = NativePtr.ofNativeInt intaddr
            NativePtr.write<uint64> nptr value
            offset <- offset + 8u

        member x.BytesWritten () = offset

    member x.GetNativeInt() = nint
    member x.GetOffset() = offset


type CustomBW(bytes : uint32) =
    let buffer = Array.zeroCreate (int bytes)
    let st = new MemoryStream(buffer)
    let bw = new BinaryWriter(st)

    interface ICustomBW with
        member x.Write (value : uint32) = bw.Write value
        member x.Write (value : int32) = bw.Write value
        member x.Write (value : uint64) = bw.Write value
        member x.BytesWritten () = uint32 bw.BaseStream.Position

    interface System.IDisposable with
        member x.Dispose() =
            bw.Dispose()
            st.Dispose()

    member x.GetBuffer() = buffer

type CustomBW2(buffer : byte []) =
    let buffer = buffer
    let st = new MemoryStream(buffer)
    let bw = new BinaryWriter(st)

    interface ICustomBW with
        member x.Write (value : int32) = bw.Write value
        member x.Write (value : uint32) = bw.Write value
        member x.Write (value : uint64) = bw.Write value
        member x.BytesWritten () = uint32 bw.BaseStream.Position

    interface System.IDisposable with
        member x.Dispose() =
            bw.Dispose()
            st.Dispose()

    member x.GetBuffer() = buffer

type DummyBW() =
    let mutable offset = 0u
    interface ICustomBW with
        member x.Write (value : int32) =
            offset <- offset + 4u
        member x.Write (value : uint32) =
            offset <- offset + 4u
        member x.Write (value : uint64) =
            offset <- offset + 8u
        member x.BytesWritten () = offset


