#pragma once

#include <chrono>


struct clock
{
    using duration =
            std::chrono::nanoseconds;

    using rep =
            duration::rep;

    using period =
            duration::period;

    using time_point =
            std::chrono::time_point<clock, duration>;

    static constexpr bool is_steady{true};

    static time_point now() noexcept
    {
        timespec tp;

        clock_gettime(CLOCK_MONOTONIC, &tp);

        return time_point(duration(std::chrono::seconds(tp.tv_sec) +
                                   std::chrono::nanoseconds(tp.tv_nsec)));
    }
};


void internal_sleep(int64_t delay)
{
    timespec tq =
    {
        .tv_sec = 0,
        .tv_nsec = delay
    };

    timespec tp;

    while (true)
    {
        if (nanosleep(&tq, &tp) == 0)
        {
            break;
        }

        tq = tp;
    }
}
