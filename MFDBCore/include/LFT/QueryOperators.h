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

#include "MemFusion/non_copyable.h"
#include "z2types.h"

namespace MFDB
{
namespace LFT
{
#pragma warning(push)
#pragma warning(disable: 4324)  // structure was padded due to __declspec(align())

class GT : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v = _mm_cmpgt_epi64(actualz2, filter);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret += _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64) + bitsof(uint64));
    }
};

class LTE : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v = _mm_cmpgt_epi64(actualz2, filter);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret ^= _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64));
    }
};

class GT_float : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128d reg_v = _mm_cmpgt_pd(*(__m128d*)&actualz2, *(__m128d*)&filter);
        uint64 ret = _mm_popcnt_u64(_mm_cmpeq_epi32(actualz2, filter).m128i_u64[0]);
        ret += _mm_popcnt_u64(*(uint64*)&reg_v.m128d_f64[1]);
        return (ret == bitsof(uint64) + bitsof(uint64));
    }
};

class LTE_float : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128d reg_v = _mm_cmpgt_pd(*(__m128d*)&actualz2, *(__m128d*)&filter);
        uint64 ret = _mm_popcnt_u64(_mm_cmpeq_epi32(actualz2, filter).m128i_u64[0]);
        ret ^= _mm_popcnt_u64(*(uint64*) &reg_v.m128d_f64[1]);
        return (ret == bitsof(uint64));
    }
};

class GTE : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v1 = _mm_cmpgt_epi64(actualz2, filter);
        __m128i reg_v2 = _mm_cmpeq_epi64(actualz2, filter);
        __m128i reg_v = _mm_or_si128(reg_v1, reg_v2);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret += _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64) + bitsof(uint64));
    }
};

class LT : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v1 = _mm_cmpgt_epi64(actualz2, filter);
        __m128i reg_v2 = _mm_cmpeq_epi64(actualz2, filter);
        __m128i reg_v = _mm_or_si128(reg_v1, reg_v2);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret ^= _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64));
    }
};

class GTE_float : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128d *act = (__m128d*)&actualz2;
        __m128d *filt = (__m128d*)&filter;
        __m128d reg_v1 = _mm_cmpgt_pd(*act, *filt);
        __m128d reg_v2 = _mm_cmpeq_pd(*act, *filt);
        __m128i reg_v = _mm_or_si128(*(__m128i*)&reg_v1, *(__m128i*)&reg_v2);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret += _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64) + bitsof(uint64));
    }
};

class EQ : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v = _mm_cmpeq_epi64(actualz2, filter);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret += _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64) + bitsof(uint64));
    }
};

class NE : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128i reg_v = _mm_cmpeq_epi64(actualz2, filter);
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        ret ^= _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64));
    }
};


class LT_float : public MemFusion::non_copyable
{
public:
    INLINE static bool apply(Z2raw filter, Z2raw actualz2)
    {
        __m128i reg_dnt = _mm_cmpeq_epi32(actualz2, filter);
        __m128d *act = (__m128d*)&actualz2;
        __m128d *filt = (__m128d*)&filter;
        __m128d reg_v1 = _mm_cmpgt_pd(*act, *filt);
        __m128d reg_v2 = _mm_cmpeq_pd(*act, *filt);
        __m128i reg_v = _mm_or_si128(*(__m128i*)&reg_v1, *(__m128i*)&reg_v2);
        // same name and doc depth? (64 bits on if so)
        uint64 ret = _mm_popcnt_u64(reg_dnt.m128i_u64[0]);
        // expectd reg_v1 and reg_v2 both false ... all zeros
        ret ^= _mm_popcnt_u64(reg_v.m128i_u64[1]);
        return (ret == bitsof(uint64));
    }
};

#pragma warning(pop)

}
}
