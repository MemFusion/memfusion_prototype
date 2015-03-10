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

module MemFusionDB.Logging

type LogLevel = | Info = 0 | Debug = 2 | Trace = 1

let mutable MFLogLevel = LogLevel.Info
let private logLock = ref 0
let logFatal msg = lock logLock (fun () -> printfn "FATAL error: %s" msg)
let logTrace msg = lock logLock (fun () -> if MFLogLevel>=LogLevel.Trace then printfn "\nTrace: %s"  msg)
let logError msg = lock logLock (fun () -> printfn "Error: %s" msg)
let logWarning msg = lock logLock (fun () -> printfn "Warning: %s" msg)
let logDebug msg = lock logLock (fun () -> if MFLogLevel>=LogLevel.Debug then printfn "Debug: %s" msg)
let logInfo msg = lock logLock (fun () -> printfn "%s" msg)


let maybeLog loglevel lambda =  if MFLogLevel >= loglevel then lambda

