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

#include <stdexcept>
#include <atomic>
#include "MemFusion/LF/bvec.h"
#include "MemFusion/opt.h"

namespace MemFusion
{
namespace LF
{
template <typename Key, typename Value>
class smallmap
{
    bvec<std::tuple<bool, Key, Value>> mymap;

    std::atomic<bool> s_inserting;
public:
    smallmap(uint32 size)
        : mymap(size)
    {}

    opt<Value> find(Key key)
    {
        for (auto iter = mymap.begin(); iter != mymap.end(); ++iter)
        {
            if (std::get<1>(*iter) == key)
            {
                return (opt<Value>(std::get<2>(*iter)));
            }
        }
        return (opt<Value>());
    }

    template <typename NotFoundAction>
    Value findinsert(Key key, NotFoundAction action)
    {
        for (;;)
        {
            for (auto iter = mymap.begin(); iter != mymap.end(); ++iter)
            {
                if (std::get<1>(*iter) == key)
                {
                    return (std::get<2>(*iter));
                }
            }

            bool newvalue = true;
            bool expected = false;
            if (s_inserting.compare_exchange_strong(expected, newvalue))
            {
                mymap.add(std::make_tuple(true, key, action()));
                s_inserting.store(false);
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }
};

}
}

