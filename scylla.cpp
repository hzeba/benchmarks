#include <random>
#include <set>

#include <cassandra.h>

#include "format.h"
#include "stats.h"


struct reader
{
    std::set<uint64_t>::iterator iterator;
    std::set<uint64_t>::iterator end;
};


void update_thread(const std::string_view& uri,                 // << localhost
                   uint32_t seed,                               // << 0
                   uint32_t iterations,                         // << 40000
                   uint32_t keys,                               // << 5000
                   uint32_t target_rate,                        // << 0
                   FILE* stats_fd)
{
    CassCluster* cluster = cass_cluster_new();
    assert(cluster != nullptr);

    CassSession* session = cass_session_new();
    assert(session != nullptr);

    cass_cluster_set_contact_points(cluster, uri.data());

    CassFuture* connect_future = cass_session_connect(session, cluster);
    assert(connect_future != nullptr);

    CassError rc = cass_future_error_code(connect_future);
    assert(rc == CASS_OK);

    std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(0, static_cast<uint32_t>(-1));

    auto t0 = clock::now();

    uint64_t inserted_keys = 0;

    CassFuture* prepare_future = cass_session_prepare(session, "INSERT INTO test.test (key, value) VALUES (?, ?);");
    assert(prepare_future != nullptr);

    rc = cass_future_error_code(prepare_future);
    assert(rc == CASS_OK);

    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    for (uint32_t i = 0; i < iterations; i++)
    {
        std::set<uint64_t> key_set;

        while (key_set.size() != keys)
        {
            key_set.insert(distribution(generator));
        }

        reader r{.iterator = key_set.begin(), .end = key_set.end()};

        auto t1 = clock::now();

        while (true)
        {
            uint32_t batch_count = 0;

            CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);

            while (batch_count < 10)
            {
                uint64_t key = *r.iterator;

                CassStatement* statement = cass_prepared_bind(prepared);
                assert(statement != nullptr);

                cass_statement_bind_int64(statement, 0, key);
                cass_statement_bind_int64(statement, 1, key);

                rc = cass_batch_add_statement(batch, statement);
                assert(rc == CASS_OK);

                cass_statement_free(statement);

                if (++r.iterator == r.end)
                {
                    break;
                }

                batch_count++;
            }

            CassFuture* query_future = cass_session_execute_batch(session, batch);
            assert(query_future != nullptr);

            cass_batch_free(batch);

            CassError rc = cass_future_error_code(query_future);
            assert(rc == CASS_OK);

            const CassResult* result = cass_future_get_result(query_future);
            assert(result != nullptr);

            cass_future_free(query_future);

            //const CassRow* row = cass_result_first_row(result);
            //assert(row != nullptr);

            cass_result_free(result);

            if (r.iterator == r.end)
            {
                break;
            }
        }

        auto t2 = clock::now();

        inserted_keys += keys;

        uint64_t iter_t = std::chrono::duration_cast<clock::duration>(t2 - t1).count();
        uint64_t total_t = std::chrono::duration_cast<clock::duration>(t2 - t0).count();

        if (stats_fd != nullptr)
        {
            auto ts = std::chrono::duration_cast<std::chrono::microseconds>(t2.time_since_epoch()).count();

            fmt::print(stats_fd,
                       "{},{},{}\n",
                       ts,
                       keys,
                       iter_t);

            fflush(stats_fd);
        }

        if (target_rate != 0)
        {
            uint64_t target_t = inserted_keys * 1000000000 / target_rate;

            if (target_t > total_t)
            {
                uint64_t delay = target_t - total_t;
                internal_sleep(delay);
            }
        }
    }

    cass_prepared_free(prepared);

    cass_future_free(connect_future);

    cass_session_free(session);
    cass_cluster_free(cluster);
}


void fetch_thread(const std::string_view& uri,                  // << localhost
                  uint32_t seed,                                // << 0
                  FILE* stats_fd)
{
    CassCluster* cluster = cass_cluster_new();
    assert(cluster != nullptr);

    CassSession* session = cass_session_new();
    assert(session != nullptr);

    cass_cluster_set_contact_points(cluster, uri.data());

    CassFuture* connect_future = cass_session_connect(session, cluster);
    assert(connect_future != nullptr);

    CassError rc = cass_future_error_code(connect_future);
    assert(rc == CASS_OK);

    CassFuture* prepare_future = cass_session_prepare(session, "SELECT value FROM test.test WHERE key = ?;");
    assert(prepare_future != nullptr);

    rc = cass_future_error_code(prepare_future);
    assert(rc == CASS_OK);

    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(0, static_cast<uint32_t>(-1));

    auto s = std::make_unique<tests::stats>();

    while (true)
    {
        uint64_t key = distribution(generator);

        CassStatement* statement = cass_prepared_bind(prepared);
        assert(statement != nullptr);

        cass_statement_bind_int64(statement, 0, key);

        {
            auto t1 = clock::now();

            CassFuture* query_future = cass_session_execute(session, statement);
            assert(query_future != nullptr);

            cass_statement_free(statement);

            CassError rc = cass_future_error_code(query_future);
            assert(rc == CASS_OK);

            const CassResult* result = cass_future_get_result(query_future);
            assert(result != nullptr);

            cass_future_free(query_future);

            const CassRow* row = cass_result_first_row(result);
            assert(row != nullptr);

            cass_result_free(result);

	    auto t2 = clock::now();

            s->add(std::chrono::duration_cast<clock::duration>(t2 - t1).count());
        }

        if (s->n() == 10000)
        {
            auto ts = std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count();

            if (stats_fd != nullptr)
            {
                fmt::print(stats_fd,
                           "{},{},{},{:.2f},{:.2f},{}\n",
                           ts,
                           s->n(),
                           s->sum(),
                           s->value_at(0.99),
                           s->value_at(0.9999),
                           s->max());

                fflush(stats_fd);
            }

            s = std::make_unique<tests::stats>();
        }
    }

    cass_prepared_free(prepared);

    cass_future_free(connect_future);

    cass_session_free(session);
    cass_cluster_free(cluster);
}
