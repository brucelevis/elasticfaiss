/*
 * local_db.h
 *
 *  Created on: 2018年8月6日
 *      Author: qiyingwang
 */

#ifndef SRC_WORKER_LOCAL_DB_H_
#define SRC_WORKER_LOCAL_DB_H_

#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/comparator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/backupable_db.h"
#include "shard_service.h"
#include <butil/threading/simple_thread.h>
#include <butil/synchronization/waitable_event.h>
#include <unordered_map>

namespace elasticfaiss
{
    typedef std::shared_ptr<rocksdb::ColumnFamilyHandle> ColumnFamilyHandlePtr;
    class LocalDB
    {
        private:
            rocksdb::DB* _db;
            bthread::Mutex _db_mutex;
            typedef std::unordered_map<std::string, ColumnFamilyHandlePtr> ColumnFamilyHandleTable;
            ColumnFamilyHandleTable _handlers;
            rocksdb::Options _options;
        public:
            LocalDB();
            int init();
            ColumnFamilyHandlePtr get_shard_db(const ShardIndexKey& shard, bool create_if_noexist);
            int drop_shard_db(ColumnFamilyHandlePtr sdb);
            int backup_shard_db(ColumnFamilyHandlePtr sdb, const std::string& file);
            int restore_shard_db(ColumnFamilyHandlePtr sdb, const std::string& file);
            bool is_inited() const
            {
                return NULL != _db;
            }
    };

    extern LocalDB g_local_db;
}



#endif /* SRC_WORKER_LOCAL_DB_H_ */
