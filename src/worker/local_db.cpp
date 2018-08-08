/*
 * local_db.cpp
 *
 *  Created on: 2018年8月6日
 *      Author: qiyingwang
 */
#include "local_db.h"
#include "common/elasticfaiss.h"
#include <gflags/gflags.h>
#include "rocksdb/utilities/convenience.h"

DEFINE_string(rocksdb_conf, "", "Initial configuration of rocksdb.");

namespace elasticfaiss
{
    LocalDB g_local_db;
    LocalDB::LocalDB()
            : _db(NULL)
    {
    }
    int LocalDB::init()
    {
        _options.create_if_missing = true;
//        options.info_log.reset(new RocksDBLogger);
//        if (DEBUG_ENABLED())
//        {
//            options.info_log_level = rocksdb::DEBUG_LEVEL;
//        }
//        else
//        {
//            options.info_log_level = rocksdb::INFO_LEVEL;
//        }

        rocksdb::Status s = rocksdb::GetOptionsFromString(_options, FLAGS_rocksdb_conf, &_options);
        if (!s.ok())
        {
            LOG(ERROR) << "Invalid rocksdb's options " << FLAGS_rocksdb_conf << " with error:" << s.ToString();
            return -1;
        }
//        if (strcasecmp(g_db->GetConf().rocksdb_compaction.c_str(), "OptimizeLevelStyleCompaction") == 0)
//        {
//            m_options.OptimizeLevelStyleCompaction();
//
//        }
//        else if (strcasecmp(g_db->GetConf().rocksdb_compaction.c_str(), "OptimizeUniversalStyleCompaction") == 0)
//        {
//            m_options.OptimizeUniversalStyleCompaction();
//        }

        _options.IncreaseParallelism();
        //options.stats_dump_period_sec = (unsigned int) g_db->GetConf().statistics_log_period;

        std::string db_home = g_home + "/localdb";
        butil::FilePath db_home_path(db_home);
        butil::CreateDirectory(db_home_path, true);
        std::vector<std::string> column_families;
        s = rocksdb::DB::ListColumnFamilies(_options, db_home, &column_families);
        if (column_families.empty())
        {
            s = rocksdb::DB::Open(_options, db_home, &_db);
        }
        else
        {
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families_descs(column_families.size());
            for (size_t i = 0; i < column_families.size(); i++)
            {
                column_families_descs[i] = rocksdb::ColumnFamilyDescriptor(column_families[i],
                        rocksdb::ColumnFamilyOptions(_options));
            }
            std::vector<rocksdb::ColumnFamilyHandle*> handlers;
            s = rocksdb::DB::Open(_options, db_home, column_families_descs, &handlers, &_db);
            if (s.ok())
            {
                for (size_t i = 0; i < handlers.size(); i++)
                {
                    rocksdb::ColumnFamilyHandle* handler = handlers[i];
                    _handlers[column_families_descs[i].name].reset(handler);
                    LOG(INFO) << "RocksDB open column family:" << column_families_descs[i].name << " success.";
                }
            }
        }

        if (s != rocksdb::Status::OK())
        {
            LOG(ERROR) << "Failed to open db: " << db_home << " with error:" << s.ToString();
            return -1;
        }
        LOG(INFO) << "Success to load localdb.";
        return 0;
    }
    ColumnFamilyHandlePtr LocalDB::get_shard_db(const ShardIndexKey& shard, bool create_if_noexist)
    {
        std::lock_guard<bthread::Mutex> guard(_db_mutex);
        auto found = _handlers.find(shard.to_string());
        if (found != _handlers.end())
        {
            return found->second;
        }
        if (create_if_noexist)
        {
            rocksdb::ColumnFamilyOptions cf_options(_options);
            std::string name = shard.to_string();
            rocksdb::ColumnFamilyHandle* cfh = NULL;
            rocksdb::Status s = _db->CreateColumnFamily(cf_options, name, &cfh);
            if (s.ok())
            {
                _handlers[name].reset(cfh);
                LOG(INFO) << "Create ColumnFamilyHandle with name:" << name << " success.";
                return _handlers[name];
            }
            LOG(ERROR) << "Failed to create column family:" << name << " with error:" << s.ToString();
        }
        return NULL;
    }
    int LocalDB::drop_shard_db(ColumnFamilyHandlePtr sdb)
    {
        std::string name = sdb->GetName();
        std::lock_guard<bthread::Mutex> guard(_db_mutex);
        auto found = _handlers.find(name);
        if (found == _handlers.end())
        {
            return -2;
        }
        rocksdb::Status s = _db->DropColumnFamily(sdb.get());
        if (s.ok())
        {
            LOG(INFO) << "Drop ColumnFamilyHandle with name:" << name << " success.";
        }
        else
        {
            LOG(ERROR) << "Failed to drop column family:" << name << " with error:" << s.ToString();
        }
        _handlers.erase(found);
        return 0;
    }
    int LocalDB::backup_shard_db(ColumnFamilyHandlePtr sdb, const std::string& file_path)
    {
        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), _options);
        // Open the file for writing
        rocksdb::Status s = sst_file_writer.Open(file_path);
        if (!s.ok())
        {
            LOG(ERROR) << "Error while opening file " << file_path << " with error: " << s.ToString();
            return 1;
        }

        rocksdb::ReadOptions ropt;
        auto iter = _db->NewIterator(ropt, sdb.get());
        // Insert rows into the SST file, note that inserted keys must be
        // strictly increasing (based on options.comparator)
        int64_t cusrsor = 0;
        while (iter->Valid())
        {
            auto key = iter->key();
            auto value = iter->value();
            s = sst_file_writer.Put(key, value);
            if (!s.ok())
            {
                //printf("Error while adding Key: %s, Error: %s\n", key.c_str(), s.ToString().c_str());
                LOG(ERROR) << "Shard:" << sdb->GetName() << " backup error while adding key/value pair at " << cusrsor
                        << " with error: " << s.ToString();
                delete iter;
                return -1;
            }
            cusrsor++;
            iter->Next();
        }
        delete iter;

        // Close the file
        s = sst_file_writer.Finish();
        if (!s.ok())
        {
            LOG(ERROR) << "Error while finishing file " << file_path << " with error: " << s.ToString();
            return -1;
        }
        return 0;
    }
    int LocalDB::restore_shard_db(ColumnFamilyHandlePtr sdb, const std::string& file)
    {
        rocksdb::IngestExternalFileOptions ifo;
        // Ingest the 2 passed SST files into the DB
        rocksdb::Status s = _db->IngestExternalFile(sdb.get(), { file }, ifo);
        if (!s.ok())
        {
            LOG(ERROR) << "Shard:" << sdb->GetName() << " restore with error: " << s.ToString();
            return 1;
        }
        return 0;
    }
}

