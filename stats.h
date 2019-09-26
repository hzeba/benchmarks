#pragma once


#include <tdigest.h>

#include <cstdint>
#include <cmath>
#include <vector>


namespace tests {


class stats
{
public:
    void add(uint64_t sample)
    {
        td_add(m_hist, sample, 1);

        m_min = std::min(m_min, sample);
        m_max = std::max(m_max, sample);
        m_sum += sample;
        m_n++;
    }

    uint64_t n() const
    {
        return m_n;
    }

    uint64_t sum() const
    {
        return m_sum;
    }

    uint64_t min() const
    {
        return m_min;
    }

    uint64_t max() const
    {
        return m_max;
    }

    float avg() const
    {
        return static_cast<float>(m_sum) / m_n;
    }

    float value_at(float p) const
    {
        return td_value_at(m_hist, p);
    }

public:
    stats()
    {
        m_hist = td_new(1000);

        if (m_hist == nullptr)
        {
            throw std::bad_alloc();
        }
    }

    ~stats()
    {
        td_free(m_hist);
        m_hist = nullptr;
    }

private:
    td_histogram* m_hist{nullptr};

    uint64_t m_min{static_cast<uint64_t>(-1)};
    uint64_t m_max{0};
    uint64_t m_sum{0};
    uint32_t m_n{0};
};

}
