#include <random>
#include <set>

#include <cassert>

#include <mongoc.h>
#include <bson.h>

#include "format.h"
#include "stats.h"


struct reader
{
    std::set<uint64_t>::iterator iterator;
    std::set<uint64_t>::iterator end;
};


void update_thread(const std::string_view& uri,                 // << mongodb://localhost:27017
                   const std::string_view& collection_name,     // << test
                   const std::string_view& database_name,       // << test
                   uint32_t seed,                               // << 0
                   uint32_t iterations,                         // << 40000
                   uint32_t keys,                               // << 5000
                   uint32_t delay,                              // << 50
                   FILE* stats_fd)
{
    mongoc_client_t* client = mongoc_client_new(uri.data());
    assert(client != nullptr);

    mongoc_database_t* database = mongoc_client_get_database(client, database_name.data());
    assert(database != nullptr);

    mongoc_collection_t* collection = mongoc_client_get_collection(client,
                                                                   collection_name.data(),
                                                                   database_name.data());

    std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(0, static_cast<uint32_t>(-1));

    bson_t* opts = BCON_NEW("upsert", BCON_BOOL(true));
    assert(opts != nullptr);

    for (uint32_t i = 0; i < iterations; i++)
    {
        std::set<uint64_t> key_set;

        while (key_set.size() != keys)
        {
            key_set.insert(distribution(generator));
        }

        reader r{.iterator = key_set.begin(), .end = key_set.end()};

        mongoc_bulk_operation_t* bulk_op = mongoc_collection_create_bulk_operation_with_opts(collection,
                                                                                             nullptr);
        assert(bulk_op != nullptr);

        while (true)
        {
            uint64_t key = *r.iterator;

            bson_t* query = BCON_NEW("_id", BCON_INT64(key));
            assert(query != nullptr);

            bson_t* doc = BCON_NEW("$set", "{", "value", BCON_INT64(key), "}");
            assert(query != nullptr);

            bson_error_t error;

            auto ret = mongoc_bulk_operation_update_one_with_opts(bulk_op,
                                                                  query,
                                                                  doc,
                                                                  opts,
                                                                  &error);
            assert(ret == true);

            bson_destroy(doc);
            bson_destroy(query);

            if (++r.iterator == r.end)
            {
                break;
            }
        }

        auto t1 = clock::now();

        bson_t reply;
        bson_error_t error;

        auto ret = mongoc_bulk_operation_execute(bulk_op, &reply, &error);
        assert(ret == true);

        auto t2 = clock::now();

        bson_destroy(&reply);
        mongoc_bulk_operation_destroy(bulk_op);

        uint64_t t = std::chrono::duration_cast<clock::duration>(t2 - t1).count();

        if (stats_fd != nullptr)
        {
            fmt::print(stats_fd,
                       "{},{},{}\n",
                       std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count(),
                       keys,
                       t);

            fflush(stats_fd);
        }

        if (delay != 0)
        {
            timespec tq =
            {
                .tv_sec = 0,
                .tv_nsec = std::chrono::milliseconds(delay).count() * 1000000
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
    }

    bson_destroy(opts);
}


void fetch_thread(const std::string_view& uri,                  // << mongodb://localhost:27107
                  const std::string_view& database_name,        // << test
                  const std::string_view& collection_name,      // << test
                  uint32_t seed,                                // << 0
                  FILE* stats_fd)
{
    mongoc_client_t* client = mongoc_client_new(uri.data());
    assert(client != nullptr);

    mongoc_collection_t* collection = mongoc_client_get_collection(client,
                                                                   database_name.data(),
                                                                   collection_name.data());
    assert(collection != nullptr);

    std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(0, static_cast<uint32_t>(-1));

    auto s = std::make_unique<tests::stats>();

    while (true)
    {
        uint64_t key = distribution(generator);

        bson_t* query = BCON_NEW("_id", BCON_INT64(key));
        assert(query != nullptr);

        mongoc_cursor_t* cursor = nullptr;

        {
            auto t1 = clock::now();

            cursor = mongoc_collection_find_with_opts(collection,
                                                      query,
                                                      nullptr,
                                                      nullptr);
            assert(cursor != nullptr);

            const bson_t *next_doc;
            bson_error_t error;

            auto ret = mongoc_cursor_error(cursor, &error);
            assert(ret == false);

            uint32_t count = 0;

            while (mongoc_cursor_next(cursor, &next_doc))
            {
                count++;
            }

            if (count != 1)
            {
                generator.seed(seed);
                distribution.reset();
            }

            auto t2 = clock::now();

            s->add(std::chrono::duration_cast<clock::duration>(t2 - t1).count());
        }

        mongoc_cursor_destroy(cursor);
        bson_destroy(query);

        if (s->n() == 10000)
        {
            if (stats_fd != nullptr)
            {
                fmt::print(stats_fd,
                           "{},{},{},{:.2f},{:.2f},{}\n",
                           std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count(),
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
}
