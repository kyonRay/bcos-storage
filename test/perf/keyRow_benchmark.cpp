/**
 *  Copyright (C) 2021 FISCO BCOS.
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * @file keyRow_benchmark.cpp
 * @author: kyonGuo
 * @date 2022/2/17
 */

#include "RocksDBStorage.h"
#include "boost/filesystem.hpp"
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/log/core.hpp>
#include <cstdlib>
#include <functional>
#include <rocksdb/write_batch.h>
#include <bcos-framework/libstorage/StateStorage.h>
#include <future>
#include <optional>

using namespace std;
using namespace bcos;
using namespace bcos::storage;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

boost::program_options::options_description main_options("Main for Table benchmark");

po::variables_map initCommandLine(int argc, const char *argv[]) {
    main_options.add_options()
            ("help,h", "help of Table benchmark")
            ("path,p", po::value<string>()->default_value("benchmark"), "[RocksDB path]")
            ("name,n", po::value<string>()->default_value("tableName"), "[table name]")
            ("keys,k", po::value<int>()->default_value(10000), "the number of different keys")
            ("value,v", po::value<int>()->default_value(256), "the length of value")
            ("mode,m", po::value<int>()->default_value(3), "m=1,only do write;m=2,only do read;m=3,do all test")
            ("random,r", "every test use a new rocksdb");
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, main_options), vm);
        po::notify(vm);
    }
    catch (...) {
        std::cout << "invalid input" << std::endl;
        exit(0);
    }
    if (vm.count("help") || vm.count("h")) {
        std::cout << main_options << std::endl;
        exit(0);
    }
    return vm;
}

int main(int argc, const char *argv[]) {
    boost::log::core::get()->set_logging_enabled(false);
    boost::property_tree::ptree pt;
    auto params = initCommandLine(argc, argv);
    auto storagePath = params["path"].as<string>();
    if (params.count("random")) {
        storagePath += to_string(utcTime());
    }
    if (fs::exists(storagePath)) {
        fs::remove_all(storagePath);
    }
    auto keys = params["keys"].as<int>();
    auto valueLength = params["value"].as<int>();
    string testTableName = params["name"].as<string>() + to_string(utcTime());

    std::vector<std::string> keyVec = {};
    for (int i = 0; i < keys; ++i) {
        std::string key = "key" + boost::lexical_cast<std::string>(i);
        keyVec.emplace_back(std::move(key));
    }

    string value;
    value.resize(valueLength);
    for (int i = 0; i < valueLength; ++i) {
        value[i] = '0' + rand() % 10;
    }

    rocksdb::DB *db;
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well

    // options.IncreaseParallelism();
    // options.OptimizeLevelStyleCompaction();

    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, storagePath, &db);

    cout << "rocksdb path    : " << storagePath << endl;
    cout << "table name      : " << testTableName << endl;
    cout << "keys number     : " << keys << endl;
    cout << "value length(B) : " << valueLength << endl;
    cout << "rocksdb ok      : " << s.ok() << endl;
    auto dbStorage = std::make_shared<RocksDBStorage>(std::unique_ptr<rocksdb::DB>(db));

    auto performance = [&](const string &description,
                           std::function<void()> operation) {
        auto now = std::chrono::system_clock::now();
        cout << "<<<<<<<<<< " << description << endl;
        operation();
        auto elapsed = std::chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now() - now);
        cout << "<<<<<<<<<< " << description
             << "|time used(ms)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
             << elapsed.count() << "|" << endl;
    };

    auto createTable = [&](const string &tableName) {
        string valueFields("v1");
        std::promise<std::optional<Table>> prom;
        dbStorage->asyncCreateTable(tableName, valueFields,
                                    [&prom](Error::UniquePtr, std::optional<Table> &&table) {
                                        prom.set_value(table);
                                    });
        std::optional<Table> table = prom.get_future().get();
    };

    auto write_batch =
            [&](const string &tableName, const vector<string> &_keyVec, const string &value) {
                auto stateStorage = std::make_shared<bcos::storage::StateStorage>(dbStorage);

                auto count = keyVec.size();
                cout << "<<<<<<<<<< Write batch " << "count: " << count << endl;
                auto table = stateStorage->openTable(tableName);
                auto callback_count = make_shared<atomic_int>(0);
                for (const auto &key: _keyVec) {
                    Entry entry;
                    entry.importFields({value});
                    table->setRow(key, entry);
                }
                auto params1 = bcos::storage::TransactionalStorageInterface::TwoPCParams();
                params1.number = 100;
                params1.primaryTableName = tableName;
                params1.primaryTableKey = "key0";
                std::promise<Error::Ptr> e;

                auto now = std::chrono::system_clock::now();
                // pre-write, put to rocksdb buffer
                dbStorage->asyncPrepare(params1, *stateStorage, [&](Error::Ptr, uint64_t ts) {
                    params1.startTS = ts;
                    // commit buffer
                    dbStorage->asyncCommit(params1, [&](Error::Ptr _e) { e.set_value(_e); });
                    // check commit success
                });
                auto err = e.get_future().get();
                auto elapsed = std::chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now() - now);
                cout << "<<<<<<<<<< Write batch finished"
                     << "|time used(ms)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
                     << elapsed.count() << " rounds=" << count << " tps=" << count / elapsed.count() << "|"
                     << endl;
                if (err != nullptr) {
                    cout << "commit error: " << err->errorMessage() << endl;
                }
            };

    auto write_single = [&](const string &tableName, const string &key,
                            const string &value) {
        Entry entry;
        entry.importFields({value});
        std::promise<Error::UniquePtr> p;
        dbStorage->asyncSetRow(tableName, key, entry, [&](Error::UniquePtr _e) {
            p.set_value(std::move(_e));
        });
        std::ignore = p.get_future().get();
    };

    auto read_batch =
            [&](const string &tableName, const gsl::span<std::string const> &_keys) {
                std::promise<std::tuple<Error::UniquePtr, std::vector<std::optional<Entry>>>> p;
                dbStorage->asyncGetRows(tableName, _keys,
                                        [&p](Error::UniquePtr _e, std::vector<std::optional<Entry>> _entries) {
                                            p.set_value(std::make_tuple(std::move(_e), _entries));
                                        });
                auto[e, entries] = p.get_future().get();
                cout << "<<<<<<<<<< read_batch entries size: " << entries.size() << endl;
                if (e != nullptr) {
                    cout << "read_batch error: " << e->errorMessage() << endl;
                }
            };

    auto read_single = [&](const string &tableName, const std::string &_key) {
        std::promise<std::tuple<Error::UniquePtr, std::optional<Entry>>> p;
        dbStorage->asyncGetRow(tableName, _key, [&p](Error::UniquePtr _e, std::optional<Entry> _entry) {
            p.set_value(std::make_tuple(std::move(_e), _entry));
        });
        auto[e, entry] = p.get_future().get();
        if (e != nullptr) {
            cout << "read_single error: " << e->errorMessage() << endl;
        }
    };

    cout << "<<<<<<<<<< " << endl;
    cout << "<<<<<<<<<< " << "Create table" << endl;
    createTable(testTableName);
    write_batch(testTableName, keyVec, value);
    performance("Read batch", [&]() {
        read_batch(testTableName, gsl::make_span(keyVec));
    });
    for (auto &item: keyVec) {
        item += to_string(utcTime());
    }
    performance("Write single", [&]() {
        for (const auto &key: keyVec) {
            write_single(testTableName, key, value);
        }
    });
    performance("Read single", [&]() {
        for (const auto &key: keyVec) {
            read_single(testTableName, key);
        }
    });
    return 0;
}