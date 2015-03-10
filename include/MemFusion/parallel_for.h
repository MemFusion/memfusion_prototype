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

#include <assert.h>
#include <vector>
#include <thread>
#include <future>
#include <functional>
#include "types.h"
#include "cancellation_token.h"

namespace MemFusion
{
template <typename Function>
void parallel_for(uint startidx, uint endidx, const Function & function, cancellation_token & token)
{
    assert(startidx <= endidx);

    std::vector<std::future<void>> futures;

    for (uint idx = startidx; idx != endidx; ++idx)
    {
        auto fut = std::async([function, idx, &token]
        {
            function(idx, token);
        });
        futures.push_back(std::move(fut));
    }

    std::string message = "";
    bool got_exception = false;

    for (uint idx = startidx; idx != endidx; ++idx)
    {
        try {
            futures[idx].get();
        }
        catch (std::exception & ex)
        {
            token.cancel();
            got_exception = true;
            std::stringstream ss;
            ss << "std::exception in parallel_for for index " << idx << ", message='" << ex.what() << "'\n";
            message += ss.str();
        }
        catch (...)
        {
            token.cancel();
            got_exception = true;
            std::stringstream ss;
            ss << "unknown exception in parallel_for for index " << idx << "\n";
            message += ss.str();
        }
    }

    if (got_exception)
    {
        throw (std::exception(message.c_str()));
    }
}
}
