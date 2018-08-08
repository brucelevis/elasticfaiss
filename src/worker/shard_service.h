/*
 * shard_service.h
 *
 *  Created on: 2018年7月15日
 *      Author: wangqiying
 */

#ifndef SRC_WORKER_SHARD_SERVICE_H_
#define SRC_WORKER_SHARD_SERVICE_H_
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include "proto/shard.pb.h"
#include "proto/work_node.pb.h"
#include "proto/master.pb.h"
#include "common/elasticfaiss.h"
#include "rocksdb/db.h"
#include <mutex>

namespace elasticfaiss
{
    struct ShardConfig
    {
            const IndexConf* index_conf;
            int32_t shard_idx;
            braft::Configuration conf;
            std::string leader;
            std::set<std::string> nodes;
            ShardConfig()
                    : index_conf(NULL), shard_idx(0)
            {
            }
    };

    struct ShardNodes
    {
            int64_t state_ms;
            int32_t shard_idx;
            const WorkNode* leader;
            std::set<const WorkNode*> nodes;
            ShardNodes()
                    : state_ms(-1), shard_idx(0), leader(NULL)
            {
            }
            bool has_leader() const
            {
                if (NULL == leader)
                {
                    return false;
                }
                return leader->state() == WNODE_ACTIVE;
            }
            void clear()
            {
                shard_idx = 0;
                leader = NULL;
                nodes.clear();
                state_ms = -1;
            }
    };

    class ShardNode: public braft::StateMachine
    {

        public:
            ShardNode()
                    : _node(NULL), _leader_term(-1), _state(SHARD_LOADING)
            {
            }
            ~ShardNode()
            {
                delete _node;
            }

            const IndexShardConf& get_init_conf()
            {
                return _init_conf;
            }
            // Starts this node
            int start(const IndexShardConf& req);

            bool is_leader() const
            {
                return _leader_term.load(butil::memory_order_acquire) > 0;
            }

            ShardState get_state()
            {
                return _state;
            }

            bool list_peers(std::vector<std::string>& ids);

            // Shut this node down.
            void shutdown()
            {
                if (_node)
                {
                    _node->shutdown(NULL);
                }
            }
            void remove_db();

            // Blocking this thread until the node is eventually down.
            void join()
            {
                if (_node)
                {
                    _node->join();
                }
            }

        private:
            void on_apply(braft::Iterator& iter);
            void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);
            int on_snapshot_load(braft::SnapshotReader* reader);

            void on_leader_start(int64_t term)
            {
                _leader_term.store(term, butil::memory_order_release);
                LOG(INFO) << "Node becomes leader for " << _node->node_id().group_id;
            }
            void on_leader_stop(const butil::Status& status)
            {
                _leader_term.store(-1, butil::memory_order_release);
                LOG(INFO) << "Node stepped down : " << status;
            }

            void on_shutdown()
            {
                LOG(INFO) << "This node is down for " << _node->node_id().group_id;
            }
            void on_error(const ::braft::Error& e)
            {
                LOG(ERROR) << "Meet raft error " << e;
            }
            void on_configuration_committed(const ::braft::Configuration& conf)
            {
                LOG(INFO) << "Configuration of this group is " << conf;
            }
            void on_stop_following(const ::braft::LeaderChangeContext& ctx)
            {
                LOG(INFO) << "Node stops following " << ctx;
            }
            void on_start_following(const ::braft::LeaderChangeContext& ctx)
            {
                LOG(INFO) << "Node start following " << ctx;
            }
            // end of @braft::StateMachine

        private:
            IndexShardConf _init_conf;
            braft::Node* volatile _node;
            butil::atomic<int64_t> _leader_term;
            ShardState _state;
            std::shared_ptr<rocksdb::ColumnFamilyHandle> _db;
    };

    struct ShardIndexKey
    {
            std::string index;
            int32_t shard_idx;
            ShardIndexKey(const std::string& n = "", int32_t idx = -1)
                    : index(n), shard_idx(idx)
            {
            }
            bool operator<(const ShardIndexKey& other) const
            {
                int ret = index.compare(other.index);
                if (0 != ret)
                {
                    return ret < 0 ? true : false;
                }
                return shard_idx < other.shard_idx;
            }
            std::string to_string() const;
    };

    class ShardManager
    {
        private:
            typedef std::map<ShardIndexKey, ShardNode*> ShardNodeTable;
            std::string _init_conf_path;
            bthread::Mutex _shards_mutex;
            ShardNodeTable _shards;
            int load_shards(const BootstrapResponse& conf);
        public:
            int init(const BootstrapResponse& conf);
            int create_shard(const IndexShardConf& req);
            int remove_shard(const DeleteShardRequest& req);
            void fill_heartbeat_request(NodeHeartbeatRequest& req);
    };

    std::string shard_cluster_name(const std::string& index, int32_t shard_idx);

    extern ShardManager g_shards;
}

#endif /* SRC_WORKER_SHARD_SERVICE_H_ */
