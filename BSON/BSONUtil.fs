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

module MemFusionDB.BSONUtil

open MemFusionDB.BSONAst

let print_BSON_elem (elem : BSON_element) (str : string) =
    match elem with
        | Floatnum n -> str + ", " + (fst n) + "=( Floatnum: " + (snd n).ToString() + ")"
        | UTF8String n ->
            str + ", " + (fst n) + "=( UTF8String: " + (snd (snd n)) + ")"
        | EmbeddedDoc n -> failwith ",to be implemented."
        | ArrayDoc n -> failwith ",to be implemented."
        | BinaryData n -> failwith ",to be implemented."
        | Undefined n -> str + ", " + n + "=(Undefined)"
        | ObjectID n -> failwith ",to be implemented."
        | Bool n -> str + ", " + (fst n) + "=(Bool:" + (snd n).ToString() + ")"
        | UTCDateTime n -> failwith ",to be implemented."
        | Null n -> str + ", " + n + "=(Null)"
        | RegEx n -> failwith ",to be implemented."
        | DBPointer n -> failwith ",to be implemented."
        | JScode n -> failwith ",to be implemented."
        | Symbol n -> failwith ",to be implemented."
        | JSCodeWScope n -> failwith ",to be implemented."
        | Int32 n -> str + ", " + (fst n) + "=(Int32:" + (snd n).ToString() + ")"
        | Int64 n -> str + ", " + (fst n) + "=(Int64:" + (snd n).ToString() + ")"
        | MinKey n -> str + ", " + n + "=(MinKey)"
        | MaxKey n -> str + ", " +  n + "=(MaxKey)"
        | _ -> failwith "????"


let revBdoc rdoc = (fst rdoc, (snd rdoc) |> List.rev)

let BinaryDataMerged (bd : BSON_binary) =
    Array.concat [ [|byte (fst bd)|]; (snd bd)]


let lengthOf (v : BSON_binary) = 1 + (snd v).Length

let BsonElemList (doc : BSON_Document) = snd doc

let CStringOfBsonString (str : BSON_string) = snd str

