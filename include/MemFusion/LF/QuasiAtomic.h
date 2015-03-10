// Copyright (c) 2014  Benedetto Proietti
//

#include "MemFusion\types.h"
#include "MemFusion\Cache.h"
#include "MemFusion\Inline.h"
#include <atomic>

namespace MemFusion
{
namespace LF
{
    namespace UUU {
        class QuasiAtomic
        {
            QuasiAtomic(const QuasiAtomic &);
            void operator = (const QuasiAtomic &);

            CACHE_ALIGN std::atomic<bool> s_qa;
        public:
            QuasiAtomic()
                : s_qa(false)
            {}

            INLINE void Acquire()
            {
                bool expected;
                while (!s_qa.compare_exchange_strong(expected, true, std::memory_order_release, std::memory_order_relaxed))
                {
                    _mm_pause();
                }
            }
            INLINE void Release()
            {
                s_qa.store(false);
            }

            INLINE bool Available()
            {
                return (s_qa == false);
            }
        };
        class QuasiAtomicGuard
        {
            QuasiAtomicGuard();
            QuasiAtomicGuard(const QuasiAtomicGuard &);
            void operator = (const QuasiAtomicGuard &);

            QuasiAtomic & qa;
        public:
            QuasiAtomicGuard(QuasiAtomic & q)
                : qa(q)
            {
                qa.Acquire();
            }

            ~QuasiAtomicGuard()
            {
                qa.Release();
            }
        };
    }




class QuasiAtomic
{
    QuasiAtomic(const QuasiAtomic &);
    void operator = (const QuasiAtomic &);

    CACHE_ALIGN std::atomic<uint64> s_qa;
public:
    QuasiAtomic()
        : s_qa(0)
    {}

    INLINE void Acquire()
    {
        ++s_qa;
    }
    INLINE void Release()
    {
        ++s_qa;
    }

    INLINE bool AcquireIfZero()
    {
        s_qa.compare_exchange_strong(expected, )
    }
};
class QuasiAtomicGuard
{
    QuasiAtomicGuard();
    QuasiAtomicGuard(const QuasiAtomicGuard &);
    void operator = (const QuasiAtomicGuard &);

    QuasiAtomic & qa;
public:
    QuasiAtomicGuard(QuasiAtomic & q)
        : qa(q)
    {
        qa.Acquire();
    }

    ~QuasiAtomicGuard()
    {
        qa.Release();
    }
};
}
}

