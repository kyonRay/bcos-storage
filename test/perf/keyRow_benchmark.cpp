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
#include <boost/serialization/vector.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/log/core.hpp>
#include <cstdlib>
#include <functional>
#include <rocksdb/write_batch.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/iostats_context.h>
#include <bcos-framework/libstorage/StateStorage.h>
#include <future>
#include <optional>

using namespace std;
using namespace bcos;
using namespace bcos::storage;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

boost::program_options::options_description main_options("Main for Table benchmark");

static const ssize_t MAX_CACHE_CAPACITY = 8 * 1024 * 1024;

po::variables_map initCommandLine(int argc, const char *argv[]) {
    main_options.add_options()
            ("help,h", "help of Table benchmark")
            ("path,p", po::value<string>()->default_value("benchmark"), "[RocksDB path]")
            ("name,n", po::value<string>()->default_value("tableName"), "[table name]")
            ("keys,k", po::value<int>()->default_value(10000), "the number of different keys")
            ("value,v", po::value<int>()->default_value(256), "the length of value")
            ("mode,m", po::value<int>()->default_value(3), "m=1,only do write;m=2,only do read;m=3,do all test")
            ("batch,b", po::value<bool>()->default_value(true), "do batch test")
            ("static,s", po::value<bool>()->default_value(false))
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

void writeBatch(shared_ptr<RocksDBStorage> &dbStorage, const string &tableName, const vector<string> &_keyVec,
                const string &value) {
    auto stateStorage = std::make_shared<bcos::storage::StateStorage>(dbStorage);

    auto count = _keyVec.size();
    cout << "<<<<<<<<<< Write batch " << "count: " << count << endl;
    auto table = stateStorage->openTable(tableName);
    for (const auto &key: _keyVec) {
        Entry entry;
        entry.importFields({value});
        table->setRow(key, std::move(entry));
    }
    auto params1 = bcos::storage::TransactionalStorageInterface::TwoPCParams();
    params1.primaryTableName = tableName;
    std::promise<Error::Ptr> e;

    auto now = std::chrono::system_clock::now();
    // pre-write, put to rocksdb buffer
    dbStorage->asyncPrepare(params1, *stateStorage, [&](Error::Ptr, uint64_t) {
        // commit buffer
        dbStorage->asyncCommit(params1, [&](Error::Ptr _e) { e.set_value(std::move(_e)); });
        // check commit success
    });
    auto err = e.get_future().get();
    auto elapsed = std::chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now() - now);
    cout << "<<<<<<<<<< Write batch finished"
         << "|time used(ms)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
         << elapsed.count() << "|" << endl;
    if (err != nullptr) {
        cout << "commit error: " << err->errorMessage() << endl;
    }
}

void createTable(shared_ptr<RocksDBStorage> &dbStorage, const string &tableName) {
    std::promise<std::optional<Table>> prom;
    dbStorage->asyncOpenTable(tableName, [&](Error::UniquePtr error, std::optional<Table> table) {
        if (error || !table.has_value()) {
            string valueFields("v1");
            dbStorage->asyncCreateTable(tableName, valueFields,
                                        [&prom](Error::UniquePtr, std::optional<Table> &&table) {
                                            prom.set_value(std::move(table));
                                        });
        } else {
            prom.set_value(std::move(table));
        }
    });
    auto table = prom.get_future().get();
}

void writeSingle(shared_ptr<RocksDBStorage> &dbStorage, const string &tableName, const string &key,
                 const string &value) {
    Entry entry;
    entry.importFields({value});
    std::promise<Error::UniquePtr> p;
    dbStorage->asyncSetRow(tableName, key, entry, [&](Error::UniquePtr _e) {
        p.set_value(std::move(_e));
    });
    std::ignore = p.get_future().get();
};

void
readBatch(shared_ptr<RocksDBStorage> &dbStorage, const string &tableName, const gsl::span<std::string const> &_keys,
          size_t valueLen = 0) {
    auto now = std::chrono::system_clock::now();
    std::promise<std::tuple<Error::UniquePtr, std::vector<std::optional<Entry>>>> p;
    dbStorage->asyncGetRows(tableName, _keys,
                            [&p](Error::UniquePtr _e, std::vector<std::optional<Entry>> _entries) {
                                p.set_value(std::make_tuple(std::move(_e), std::move(_entries)));
                            });
    auto[e, entries] = p.get_future().get();
    auto elapsed = std::chrono::duration_cast<chrono::milliseconds>(std::chrono::system_clock::now() - now);
    cout << "<<<<<<<<<< Read batch finished"
         << "|time used(ms)=" << std::setiosflags(std::ios::fixed) << std::setprecision(3)
         << elapsed.count() << "|" << endl;
    cout << "<<<<<<<<<< read_batch entries size: " << entries.size() << endl;
    if (e != nullptr) {
        cout << "read_batch error: " << e->errorMessage() << endl;
    }
    if (valueLen != 0) {
        size_t errorCount = 0;
        for (const auto &entry: entries) {
            if (entry->get().size() != valueLen) {
                errorCount++;
            }
        }
        if (errorCount) {
            cerr << "error entries: " << errorCount << endl;
        }
    }
};

void readSingle(shared_ptr<RocksDBStorage> &dbStorage, const string &tableName, const std::string &_key) {
    std::promise<std::tuple<Error::UniquePtr, std::optional<Entry>>> p;
    dbStorage->asyncGetRow(tableName, _key, [&p](Error::UniquePtr _e, std::optional<Entry> _entry) {
        p.set_value(std::make_tuple(std::move(_e), _entry));
    });
    auto[e, entry] = p.get_future().get();
    if (e != nullptr) {
        cout << "read_single error: " << e->errorMessage() << endl;
    }
};

void getKeys(vector<string> &keyVec, int keysNum, int mode, const string &storagePath, int groupSize) {
    auto keyInsert = [&]() {
        int prefixNum = 1;
        int pageCount = 0;
        for (int i = 0; i < keysNum; ++i) {
            if (pageCount > groupSize) {
                prefixNum++;
                pageCount = 0;
            } else {
                pageCount++;
            }
            stringstream prefixStream;
            prefixStream << std::left << std::setfill('x') << std::setw(5) << to_string(prefixNum);
            auto key = prefixStream.str() + to_string(random());
            keyVec.emplace_back(std::move(key));
        }
    };

    switch (mode) {
        case 1: {
            // only write, write file
            keyInsert();
            auto path = fs::path("bench_keys_" + storagePath);
            if (fs::exists(path)) {
                fs::remove_all(path);
            }
            fs::ofstream output(path, ios_base::out);
            try {
                boost::archive::binary_oarchive oarchive(output);
                oarchive << keyVec;
            } catch (...) {
                cout << "write file error" << endl;
                output.close();
                exit(-1);
            }
            output.close();
            break;
        }
        case 2: {
            auto path = fs::path("bench_keys_" + storagePath);
            fs::ifstream input(path, ios_base::in);
            try {
                boost::archive::binary_iarchive iarchive(input);
                iarchive >> keyVec;
            } catch (...) {
                cout << "read file error" << endl;
                input.close();
                exit(-1);
            }
            input.close();
            // only read, read file
            break;
        }
        case 3: {
            keyInsert();
            // write and read, use hot data
            break;
        }
        default:
            break;
    }
}

int main(int argc, const char *argv[]) {
    boost::log::core::get()->set_logging_enabled(false);
    boost::property_tree::ptree pt;
    auto params = initCommandLine(argc, argv);
    auto storagePath = params["path"].as<string>();
    if (params.count("random")) {
        storagePath += to_string(utcTime());
    }
    auto keys = params["keys"].as<int>();
    auto valueLength = params["value"].as<int>();
    string testTableName = params["name"].as<string>();
    auto mode = params["mode"].as<int>();
    auto batch = params["batch"].as<bool>();
    auto statics = params["static"].as<bool>();

    std::vector<std::string> keyVec = {};
    getKeys(keyVec, keys, mode, storagePath, ceil(MAX_CACHE_CAPACITY / valueLength));

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
    if (statics) {
        rocksdb::get_perf_context()->Reset();
        rocksdb::get_iostats_context()->Reset();
    }

    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, storagePath, &db);

    cout << "rocksdb path    : " << storagePath << endl;
    cout << "table name      : " << testTableName << endl;
    cout << "keys number     : " << keys << endl;
    cout << "value length(B) : " << valueLength << endl;
    cout << "batch test      : " << batch << endl;
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

    auto staticOP = [&](bool statics, function<void()> op) {
        if (statics) {
            rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
        }
        op();
        if (statics) {
            rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
            cout << "rocksDB static:" << endl << rocksdb::get_perf_context()->ToString(true) << endl;
            cout << "IO static:" << endl << rocksdb::get_iostats_context()->ToString(true) << endl;
            rocksdb::get_perf_context()->Reset();
            rocksdb::get_iostats_context()->Reset();
        }
    };

    cout << "<<<<<<<<<< " << endl;
    cout << "<<<<<<<<<< " << "Check table" << endl;
    createTable(dbStorage, testTableName);
    if (mode & 1 && batch) {
        staticOP(statics, [&]() { writeBatch(dbStorage, testTableName, keyVec, value); });
    }
    if (mode & 2 && batch) {
        staticOP(statics, [&]() { readBatch(dbStorage, testTableName, gsl::make_span(keyVec), valueLength); });
    }
    if (mode & 1 && !batch) {
        performance("Write single", [&]() {
            for (const auto &key: keyVec) {
                writeSingle(dbStorage, testTableName, key, value);
            }
        });
    }
    if (mode & 2 && !batch) {
        performance("Read single", [&]() {
            for (const auto &key: keyVec) {
                readSingle(dbStorage, testTableName, key);
            }
        });
    }
    return 0;
}