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

module MemFusion.Util

open System
open System.IO


let fst3 (a,_,_) = a
let snd3 (_,a,_) = a
let thd3 (_,_,a) = a

let fst4 (a,_,_,_) = a
let snd4 (_,a,_,_) = a
let thd4 (_,_,a,_) = a
let fth4 (_,_,_,a) = a


let compare_byte_array (a : byte []) (b : byte []) =
    if a.Length <> b.Length then false
        else 0=Array.fold2 (fun state x y -> state + if y=x then 0 else 1) 0 a b

let KILOBYTE = 1024
let MEGABYTE = 1024 * KILOBYTE
let GIGABYTE = 1024 * MEGABYTE

let NoHead somelist =
    match somelist with
        | [] -> []
        | head :: tail -> tail

let FuncName (mi : System.Reflection.MemberInfo) =
    let infos = mi.DeclaringType.GetMember(mi.Name)
    let att = infos.[0].GetCustomAttributes(true)
    (att.[1] :?> CompilationSourceNameAttribute).SourceName 

let rec RemoveTrailingZeros (str : string) =
    let len = str.Length
    if len <= 1
        then str
        else
            match str.[len-1] with
                | '\000' -> RemoveTrailingZeros str.[0 .. len-2]
                | _ -> str

type MFUtil =
    static member ParseCString (br : BinaryReader) =
        let zz = seq {
                    let b = ref 0uy
                    b := br.ReadByte()
                    while !b <> 0uy do
                        yield !b
                        b := br.ReadByte()
                    }
                    |> Seq.toArray
        let ret = System.Text.Encoding.ASCII.GetString(zz)
        ret

    static member StringFromByteArray (bytes : byte []) =
        let idx = Array.findIndex (fun b -> b = 0uy) bytes
        System.Text.Encoding.ASCII.GetString(bytes.[0..idx])


module MFList =
    open System.Diagnostics
    open Microsoft.FSharp.Core
    open Microsoft.FSharp.Core.Operators
    open Microsoft.FSharp.Core.LanguagePrimitives
    open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicOperators
    open Microsoft.FSharp.Collections
    open Microsoft.FSharp.Primitives.Basics
    open System.Collections.Generic

    let foldWhile<'T, 'State, 'Pred> f (s : 'State) (list : 'T list) (pred : 'T -> bool) =
        match list with 
        | [] -> s
        | _ -> 
            let f = OptimizedClosures.FSharpFunc<_,_,_>.Adapt(f)
            let rec loop s xs = 
                match xs with 
                | [] -> s
                | h::t ->
                    match pred h with
                        | true -> loop (f.Invoke(s,h)) t
                        | false -> s
            loop s list
