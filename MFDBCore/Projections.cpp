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


#include "stdafx.h"
#include <mutex>
#include <algorithm>
#include <assert.h>
#include <sstream>

#include "Collection.h"

namespace MFDB
{
namespace Core
{
using std::vector;

uint32 Collection::FindProjectPhase(vector<LFTStage3> & matchesPerBin, Buffer & retbuf, opt<Projections&> onames)
{
    uint32 ret = 0;
    Z2raw * dstPtr = static_cast<Z2raw*>(retbuf.get());
    Z2raw * pstart = dstPtr++;
    uint32 doccount = 0;

    for (const Bin<Z2raw> * bin : bins)
    {
        cuint32 binIdx = bin->binIdx();
        LFTStage3 & stage3 = matchesPerBin[binIdx];

        doccount += FindProject(stage3, dstPtr, bin, onames);
    }
    if ((byte*) dstPtr > (((byte*) pstart) + MAX_DOCUMENT_SIZE))
        throw - 1;
    *pstart = Z2({ BSONtypeCompressed::CArrayDoc, 0 }, 0, doccount, -1);

    ret = static_cast<uint32>(std::distance(pstart + 1, dstPtr));
    return (ret);
}

// This writes final output for "find" queries
//
uint32 Collection::FindProject(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin, opt<Projections &> onames)
{
    uint32 ret = 0;
    if (!onames.is_initialized() || (onames.get().size()==0)) {
        ret = FindProjectAll(stage3, dstPtr, bin);
    }
    else {
        Projections & names = onames.get();
        auto iter = names.find(Z2name(MFDB::Constants::Id_1));
        bool projectId = (names.end() == iter);
        if (!projectId) { names.erase(iter);  }
        ret = FindProjectSome(stage3, dstPtr, bin, projectId, names);
    }

    return (ret);
}

uint32 Collection::FindProjectAll(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin)
{
    uint32 doccount = 0;

    std::for_each(stage3.begin(), stage3.end(),
        [&dstPtr, &doccount, bin](uint32 idx)
    {
        ++doccount;
        auto range = bin->get_elem_range(idx);
        for (auto srcPtr = range.begin(); srcPtr != range.end(); ++srcPtr)
        {
            if (Z2::invalid(*srcPtr))
                break;
            memcpy(dstPtr, srcPtr, sizeof(*dstPtr));
            ++dstPtr;
        }
        AddDocDelimiter(dstPtr);
    });

    return (doccount);
}

uint32 Collection::FindProjectSome(LFTStage3 & stage3, Z2raw *& dstPtr, const Bin<Z2raw> * bin, bool projectId, Projections & names)
{
    uint32 doccount = 0;

    std::for_each(stage3.begin(), stage3.end(),
        [&dstPtr, &doccount, bin, names, projectId](uint32 idx)
    {
        ++doccount;
        auto range = bin->get_elem_range(idx);
        Projections foundFields = names;
        bool doAll = (names.size() == 0);
        bool idDone = !projectId;

        for (auto srcPtr = range.begin(); srcPtr != range.end(); ++srcPtr)
        {
            Z2 z2(*srcPtr);
            if (Z2::invalid(z2))
                break;
            auto name = z2.z2name();

            bool todo = true;

            if (name == MFDB::Constants::Id_1)
            {
                todo = projectId;
                idDone = true;
            }
            else if (!doAll)
            {
                auto iter = std::find(foundFields.begin(), foundFields.end(), name);
                todo = iter != foundFields.end();
                if (todo) { foundFields.erase(iter); }
            }

            int32 parentDepthToSkip = 0;

            if (z2.z2type() == BSONtypeCompressed::CArrayDoc)
            {
                parentDepthToSkip = (int32)z2.z2value();
            }

            if (todo)
            {
                memcpy(dstPtr, srcPtr, sizeof(*dstPtr));
                ++dstPtr;
                if (z2.HasInnerDoc())
                {
                    Z2DocDepth parentDocNum = z2.z2docdepth();
                    for (++srcPtr; srcPtr != range.end(); ++srcPtr)
                    {
                        Z2 z2(*srcPtr);
                        if (Z2::invalid(z2))
                            break;
                        if ((Z2DocDepth) z2.z2docdepth() == parentDocNum)
                        {
                            if (--parentDepthToSkip < 0)
                                break;
                        }
                        memcpy(dstPtr, srcPtr, sizeof(*dstPtr));
                        ++dstPtr;
                    }
                    //--srcPtr;
                }
                if (!doAll && (foundFields.size() == 0) && idDone)
                    break;
            }
        }
        AddDocDelimiter(dstPtr);
    });

    return (doccount);
}


}
}

