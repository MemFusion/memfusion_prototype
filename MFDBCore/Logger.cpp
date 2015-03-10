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

#include "stdafx.h"
#include <ctime>

#include "MemFusion/Logger.h"
#include "MemFusion/types.h"
#include "MemFusion/Utils.h"

namespace MemFusion
{
Logger * Logger::instance = nullptr;

#pragma warning(disable: 4996)
void Logger::Process(queuetype apair)
{
    std::string *str = apair.second;

    if (!str)
        return;

    std::time_t result = std::time(NULL);
    char buffer[256];
    std::strftime(buffer, sizeof(buffer), "%A %c", std::localtime(&result));
    std::cout << buffer << " " << prefix << ": " << apair.first << " " << *str << std::endl;

    delete (str);
}

void Logger::run()
{
    Utils::SetThreadName("Logger");

    while (!s_shuttingDown)
    {
        uint32 qsize = 0;
        {
            std::lock_guard<LF::spinlock> guard(queuelock);
            qsize = (uint32) queue.size();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(LOGGER_SLEEP_MS));

        if (qsize > 0)
        {
            queuetype apair;
            {
                std::lock_guard<LF::spinlock> guard(queuelock);
                apair = queue.back();
                queue.pop_back();
            }

            Process(apair);
        }
    }
}

Logger::Logger(std::string pre)
    : s_shuttingDown(false),
    prefix(pre)
{
    this_thread = std::thread([this]() { run();  });
}

}

