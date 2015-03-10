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
#include "MemFusion/types.h"
#include "MemFusion/Inline.h"
#include <immintrin.h>

namespace MFDB
{

enum BSONtypeCompressed
{
    CFloatnum = 1,
    CUTF8String = 2,
    CEmbeddedDoc = 3,
    CArrayDoc = 4,
    CBinaryData = 5,
    CUndefined = 6,
    CObjectID = 7,
    CBool = 8,
    CUTCDateTime = 9,
    CNull = 10,
    CRegEx = 11,
    CDBPointer = 12,
    CJScode = 13,
    CSymbol = 14,
    CJSCodeWScope = 15,
    CInt32 = 16,
    CTimeStamp = 17,
    CInt64 = 18,
    CMaxKey = 19,
    CMinKey = 20,
};

enum Z2Constants
{
    RootDocnum = 0,
};


typedef unsigned __int8 Z2type;
typedef unsigned __int8 Z2vlen;
typedef __m128i Z2raw;
typedef uint32 Z2name;
typedef int32 Z2DocDepth;
typedef uint64 Z2value;

#pragma pack(push)
#pragma pack(1)
#pragma warning(push)
#pragma warning(disable : 4201)    // nonstandard extension used : nameless struct/union
union Z2typeinfo
{
    struct {
        unsigned __int8 Z2type : 5;
        unsigned __int8 Z2vlen : 3;
    };
    byte value;
};
#pragma pack(pop)
#pragma warning(pop)

//   z2 format

//    QWORD      DWORD       Bits         Description
//    0          0           all          DocDepth  (32bit)
//    
//    0          1            0-22        Z2name
//    0          1           23-27        Z2type  (5bit)
//    0          1           28-31        short value: 0 means hashed; 1-8 is the length; should not be more than 8
//    
//    1          2            all         Z2value low
//    1          3            all         Z2value high

class Z2
{
    Z2raw z2raw;

public:
    static const int Z2_NAME_BITS = 0x7FFFFF;
    static const int LastSpecialNameIdx = 1023;

    Z2(Z2typeinfo zt, Z2name zname, Z2value zval, Z2DocDepth docdepth = (0))
    {
        static_assert(sizeof(Z2typeinfo) == sizeof(byte), "Z2typeinfo should have size 1.");

        z2raw.m128i_u32[0] = docdepth;
        uint32 dw1 = (zname & Z2_NAME_BITS) | (uint32(zt.Z2type) << 23) | (uint32(zt.Z2vlen) << 28);
        z2raw.m128i_u32[1] = dw1;
        z2raw.m128i_u64[1] = zval;
    }

    Z2(uint64 low, uint64 high)
    {
        z2raw.m128i_u64[0] = low;
        z2raw.m128i_u64[1] = high;
    }

    Z2(const Z2raw raw)
        : z2raw(raw)
    {
    }

    static uint32 size() { return sizeof(Z2raw); }

    Z2name z2name() const
    {
        return (z2raw.m128i_u32[1] & Z2_NAME_BITS);
    }

    operator Z2raw() const
    {
        return (z2raw);
    }

    Z2DocDepth z2docdepth() const
    {
        return (Z2DocDepth(z2raw.m128i_u32[0]));
    }

    Z2type z2type() const
    {
        return (Z2type((z2raw.m128i_u16[3] & 0x0F80) >> 7));
    }
    Z2vlen z2vlen() const
    {
        return (z2raw.m128i_u8[7] >> 5);
    }
    Z2typeinfo z2typeinfo() const
    {
        Z2typeinfo ret;
        ret.value = static_cast<byte>(z2raw.m128i_u32[1] >> 23);
        return (ret);
    }

    uint64 z2low() const
    {
        return (z2raw.m128i_u64[0]);
    }
    uint64 z2value() const
    {
        return (z2raw.m128i_u64[1]);
    }

    INLINE static bool invalid(Z2raw z2raw)
    {
        return (z2raw.m128i_u64[0] == 0);
    }

    static bool HasInnerDoc(Z2type type)
    {
        return ((type == BSONtypeCompressed::CEmbeddedDoc) || (type == BSONtypeCompressed::CArrayDoc));
    }

    bool HasInnerDoc() const
    {
        Z2type type = z2type();
        return ((type == BSONtypeCompressed::CEmbeddedDoc) || (type == BSONtypeCompressed::CArrayDoc));
    }

    INLINE bool NameIsSpecial() const
    {
        return (z2name() <= LastSpecialNameIdx);
    }

    INLINE static bool NameIsSpecial(const Z2name & name)
    {
        return (name <= LastSpecialNameIdx);
    }

    INLINE static double double_z2(const Z2 & z2)
    {
        double ret;
        auto val = z2.z2value();
        ret = *(double*) &val;
        return (ret);
    }

    INLINE static Z2raw remove_name(Z2raw z2raw)
    {
        z2raw.m128i_u32[1] &= ~Z2_NAME_BITS;
        return (z2raw);
    }

    INLINE static Z2raw remove_doc(Z2raw z2raw)
    {
        z2raw.m128i_u32[0] = 0;
        return (z2raw);
    }
};

enum Constants
{
    Id_1 = 1,
};


}
