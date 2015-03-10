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

#include <vector>
#include <stack>

#include "MemFusion/types.h"
#include "LFT/LFT.h"
#include "LFT/AT.h"
#include "LFT/QueryOperators.h"
#include "MemFusion/Logger.h"
#include "retail_assert.h"

namespace MFDB
{
namespace Core
{
using namespace MemFusion;

template <typename T1>
class Z2Query
{
    Z2Query(const Z2Query &);
    void operator = (const Z2Query &);
public:
    typedef std::pair<uint32, uint32> Stage1Payload;

    Z2Query() {}
    virtual uint32 lft_size() const = 0;

    virtual const IZ2LFT<T1> * get_lft(uint32 idx) const = 0;
};

#pragma pack(push)
#pragma pack(1)
struct Aggr1
{
    Z2name targetName;
    Z2name accName;
    QO    accop;
};
#pragma pack(pop)

class Z2AggrQuery : public Z2Query<NV>
{
public:
    typedef NV T1;
    typedef std::pair<uint32, uint32> Stage1Payload;
private:
    std::vector<const IZ2LFT<T1> *> folders;
    Z2name groupname;
    std::vector<Aggr1> aggrlist;
    uint32 uintsort;

    Z2AggrQuery(const Z2AggrQuery &);
    void operator = (const Z2AggrQuery &);
public:
    Z2AggrQuery(Z2name groupname_, const std::vector<Aggr1> & al, uint32 sort)
        : aggrlist(al),
        groupname(groupname_),
        uintsort(sort)
    {
        for (uint idx = 0; idx < al.size(); ++idx)
        {
            switch (al[idx].accop)
            {
            case QO::COUNT:
                folders.push_back(new Z2LAT<COUNTExtractPolicy>(groupname, al[idx].accName, al[idx].targetName, al[idx].accop, idx));
                break;
            case QO::SUM:
                folders.push_back(new Z2LAT<SUMExtractPolicy>(groupname, al[idx].accName, al[idx].targetName, al[idx].accop, idx));
                break;
            default:
                throw std::exception("Unknown QO in Z2AggrQuery ctor.");
            }
        }
    }

    uint32 GetSort() const
    {
        return uintsort;
    }

    uint32 lft_size() const
    {
        return (static_cast<uint32>(folders.size()));
    }

    const IZ2LFT<T1> * get_lft(uint32 idx) const
    {
        return (folders[idx]);
    }
};

class Z2FindQuery : public Z2Query<uint32>
{
public:
    typedef uint32 T1;
    typedef std::pair<uint32, uint32> Stage1Payload;
private:
    std::vector<const IZ2LFT<uint32> *> lfts;
    const std::vector<QPraw> qps;

    void CreateLFTs(const std::vector<LFTraw> & lft_raws)
    {
        std::vector<LFTraw>::const_iterator iter = lft_raws.cbegin();

        for (; iter != lft_raws.cend(); ++iter)
        {
            Z2 z2(iter->z2raw);
            uint32 LFTidx = static_cast<uint32>(lfts.size());

            switch (iter->qo)
            {
            case QO::GT:
                if (z2.z2type() == BSONtypeCompressed::CFloatnum) {
                    lfts.push_back(new Z2LFT<LFT::GT_float>(z2, LFTidx));
                }
                else {
                    lfts.push_back(new Z2LFT<LFT::GT>(z2, LFTidx));
                }
                break;
            case QO::GTE:  // $gte
                if (z2.z2type() == BSONtypeCompressed::CFloatnum) {
                    lfts.push_back(new Z2LFT<LFT::GTE_float>(z2, LFTidx));
                }
                else {
                    lfts.push_back(new Z2LFT<LFT::GTE>(z2, LFTidx));
                }
                break;
            case QO::LT:  // $lt
                if (z2.z2type() == BSONtypeCompressed::CFloatnum) {
                    lfts.push_back(new Z2LFT<LFT::LT_float>(z2, LFTidx));
                }
                else {
                    lfts.push_back(new Z2LFT<LFT::LT>(z2, LFTidx));
                }
                break;
            case QO::LTE:  // $lte
                if (z2.z2type() == BSONtypeCompressed::CFloatnum) {
                    lfts.push_back(new Z2LFT<LFT::LTE_float>(z2, LFTidx));
                }
                else {
                    lfts.push_back(new Z2LFT<LFT::LTE>(z2, LFTidx));
                }
                break;
            case QO::EQ:  // $EQ
                lfts.push_back(new Z2LFT<LFT::EQ>(z2, LFTidx));
                break;
            case QO::NE:  // $ne
                lfts.push_back(new Z2LFT<LFT::NE>(z2, LFTidx));
                break;
#if 0
            case 13:
                lfts.push_back(new Z2LFT_exits(make_z2range(z2begin, z2end));
                break;
            case 14:
                lfts.push_back(new Z2LFT_type(make_z2range(z2begin, z2end));
                break;
            case 15:
                lfts.push_back(new Z2LFT_mod(make_z2range(z2begin, z2end));
                break;
            case 16:
                lfts.push_back(new Z2LFT_regex(make_z2range(z2begin, z2end));
                break;
            case 17:
                lfts.push_back(new Z2LFT_where(make_z2range(z2begin, z2end));
                break;
            case 18:
                lfts.push_back(new Z2LFT_geoWithin(make_z2range(z2begin, z2end));
                break;
            case 19:
                lfts.push_back(new Z2LFT_geoIntersects(make_z2range(z2begin, z2end));
                break;
            case 20:
                lfts.push_back(new Z2LFT_near(make_z2range(z2begin, z2end));
                break;
            case 21:
                lfts.push_back(new Z2LFT_nearSphere(make_z2range(z2begin, z2end));
                break;
            case 22:
                lfts.push_back(new Z2LFT_all(make_z2range(z2begin, z2end));
                break;
            case 23:
                lfts.push_back(new Z2LFT_elemMatch(make_z2range(z2begin, z2end));
                break;
            case 24:
                lfts.push_back(new Z2LFT_size(make_z2range(z2begin, z2end));
                break;
            case 25:
                lfts.push_back(new Z2LFT_dollar(make_z2range(z2begin, z2end));
                break;
            case 26:
                lfts.push_back(new Z2LFT_slice(make_z2range(z2begin, z2end));
                break;
#endif
            default:
                retail_assert(false, "Unknown special operator with zname in query");
            }
        }
    }

    static std::vector<QPraw> remove_ends(const std::vector<QPraw> & qps)
    {
        assert(qps.size() >= 2);
        std::vector<QPraw> ret(&qps[1], &qps[qps.size()-1]);
        return (std::move(ret));
    }

public:
    Z2FindQuery(const std::vector<LFTraw> & lft_raws, const std::vector<QPraw> & qps_)
        : qps(remove_ends(qps_))
    {
        CreateLFTs(lft_raws);
    }

    uint32 lft_size() const
    {
        return (static_cast<uint32>(lfts.size()));
    }

    const IZ2LFT<uint32> * get_lft(uint32 idx) const
    {
        return (idx < lfts.size() ? lfts[idx] : nullptr);
    }

    bool apply_qp(std::vector<bool> & lfts) const
    {
        //FILE_LOG(logDEBUG4) << "QP: Applying " << lfts.size() << " lfts with " << qps.size() << " qps";

        if (qps.size() == 0)
        {
            assert(lfts.size() == 1);
            return (lfts[0]);
        }

        if ((qps.size() == 1) && (qps[0].command == QO::AND_ALL))
        {
            bool ret = lfts[0];
            for (bool v : lfts) { ret &= v; }
            return (ret);
        }

        std::stack<bool> mystack;
        for (bool v : lfts) { mystack.push(v); }

        bool ret = mystack.top();

        std::for_each(qps.begin(), qps.end(),
            [&mystack, &ret](QPraw qp)
        {
            uint32 kids = qp.kids;
            QO cmd = qp.command;

            ret = mystack.top();
            mystack.pop();

            switch (cmd)
            {
            case QO::AND:
                for (uint32 times = 1; times < kids; ++times)
                {
                    ret &= mystack.top();
                    mystack.pop();
                }
                mystack.push(ret);
                break;
            case QO::OR:
                for (uint32 times = 1; times < kids; ++times)
                {
                    ret |= mystack.top();
                    mystack.pop();
                }
                mystack.push(ret);
                break;
            default:
                assert(UNREACHED);
            }
        });

        assert(mystack.size() == 1);
        return (ret);
    }

};

}
}
