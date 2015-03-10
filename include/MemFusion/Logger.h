//
// Copyright (c) 2014-2015 Benedetto Proietti
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

#pragma once


#include <sstream>
#include <string>
#include <mutex>
#include <atomic>
#include <thread>
#include <iostream>
#include <deque>

#include "MemFusion/LF/spinlock.h"

#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__)
#include <windows.h>
#else
#include <sys/time.h>
#endif

namespace MemFusion
{

class Logger
{
    static Logger * instance;
    std::thread this_thread;
    LF::spinlock queuelock;
    std::atomic<bool> s_shuttingDown;
    typedef std::pair<std::thread::id, std::string *> queuetype;
    std::deque<queuetype> queue;

    // TBD: from configuration
    static const int LOGGER_SLEEP_MS = 20;

    std::string prefix;

    Logger(std::string pre);

    void run();

    void Process(queuetype apair);

public:
    static Logger & Instance() { return (*instance); }

    static void Initialize(std::string prefix)
    {
        instance = new Logger(prefix);
    }

    void Log(std::string * str)
    {
        auto threadId = std::this_thread::get_id();
        std::lock_guard<LF::spinlock> guard(queuelock);
        queue.push_front(std::make_pair(threadId, str));
    }

    void Terminate()
    {
        s_shuttingDown.store(true);
    }
};

}

#define LOG(X)    ::MemFusion::Logger::Instance().Log(new std::string(X))

