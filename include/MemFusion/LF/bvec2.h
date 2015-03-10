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
#include <array>
#include <vector>
#include <thread>
#include "MemFusion/Cache.h"
#include "MemFusion/non_copyable.h"

namespace MemFusion
{
namespace LF
{

struct bvec2Full //: public std::runtime_error
{
    // using std::runtime_error::runtime_error;
    bvec2Full() {};
};
struct bvec2OOB //: public std::runtime_error
{
    // using std::runtime_error::runtime_error;
    bvec2OOB() {};
};


#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

template <typename T>
class bvec2   // : public MemFusion::non_copyable
{
    std::array<std::vector<T>, 2> data;

    uint32 m_size;
    bool empty;
    CACHE_ALIGN std::atomic<uint32> s_idx[2];
    CACHE_ALIGN std::atomic<uint32> s_pingpong;

    bvec2(const bvec2 &);
    void operator = (const bvec2 &);
public:
    static const int alignment_needed = CACHE_LINE;

    bvec2(uint32 maxsize)
        : m_size(maxsize),
        empty(true),
        s_pingpong(0)
    {
        s_idx[0] = 0;
        s_idx[1] = 0;
        data[0].resize(maxsize, 0);
        data[1].resize(maxsize, 0);
        //actives[0].resize(maxsize, 0);
        //actives[1].resize(maxsize, 0);
    }
    ~bvec2()
    {
    }

    //tuple<uint32,uint32> sizes() const { return s_idx.load(); }

    bool add(T val)
    {
        auto curPP = s_pingpong.load() % 2;
        long lastIdx = s_idx[curPP].load();
        if (lastIdx >= (long)m_size)
            return (false);
        lastIdx = s_idx[curPP]++;
        data[curPP][lastIdx] = val;
        empty = false;
        return (true);
    }

    // create another class that specialized bvec2 with this?
    std::vector<T> reset()
    {
        auto prevPP = s_pingpong++ % 2;
        // Note: some assumptions here
        // Most notable is that # of threads ~ # of cores
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        // And the desired effect is that all consumers should
        // have picked the new pingpong by now
        long lastIdx = s_idx[prevPP].load();

        std::vector<T> ret(&data[prevPP][0], &data[prevPP][lastIdx]);
        s_idx[prevPP].store(0);

        return (std::move(ret));
    }
};

#pragma warning(pop)

}

}
