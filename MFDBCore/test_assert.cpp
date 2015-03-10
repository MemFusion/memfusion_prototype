// Copyright (c) 2014  Benedetto Proietti
//

#include "MemFusion/test_assert.h"

namespace MemFusion
{

void test_assert_impl(bool condition, const char * msg, const char * func, int linenum)
{
    if (!condition)
    {
        throw TestAssert(msg, func, linenum);
    }
}

}


