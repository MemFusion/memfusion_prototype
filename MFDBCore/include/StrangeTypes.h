// Copyright (c) 2014-2015 Benedetto Proietti
//

#pragma once

#include "MemFusion\types.h"
#include <tuple>
#include <map>

namespace MFDB
{
namespace Core
{
template <typename ZT>
class Bin;

class Collection;

typedef std::tuple<void *, uint32> BI;
typedef std::tuple<Bin<Z2raw>*, BI> BBI;
typedef std::tuple<Collection*, BBI> CBBI;
typedef std::pair<CBBI, uint32> CBBI2;


static void * BufferOf(const BBI & bbi)
{
    BI bi = std::get<1>(bbi);
    return (std::get<0>(bi));
}

static Core::Collection * CollectionOf(const CBBI & cbbi)
{
    return (std::get<0>(cbbi));
}

static void * BufferOf(const CBBI & cbbi)
{
    BBI bbi = std::get<1>(cbbi);
    BI bi = std::get<1>(bbi);
    return (std::get<0>(bi));
}

static Core::Bin<Z2raw> * BinOf(const CBBI & cbbi)
{
    BBI bbi = std::get<1>(cbbi);
    return (std::get<0>(bbi));
}

static uint32 ElemIdxOf(CBBI cbbi)
{
    BBI bbi = std::get<1>(cbbi);
    BI bi = std::get<1>(bbi);
    return (std::get<1>(bi));
}

static BI BI_of(const CBBI & cbbi)
{
    BBI bbi = std::get<1>(cbbi);
    BI bi = std::get<1>(bbi);
    return (bi);
}

}
}

