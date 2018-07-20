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
#include <mutex>

namespace elasticfaiss
{

    struct ShardNodeId
    {
            std::string name;
            int32_t idx;
            ShardNodeId(const std::string& n = "", int32_t i = 0)
                    : name(n), idx(i)
            {
            }
            bool operator<(const ShardNodeId& other) const
            {
                int ret = name.compare(other.name);
                if (0 != ret)
                {
                    return ret < 0 ? true : false;
                }
                return idx < other.idx;
            }
    };

    struct ShardNodeMeta
    {
            std::string node_peer;
            bool is_leader;
            ShardNodeMeta()
                    : is_leader(false)
            {
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

            const CreateShardRequest& get_init_conf()
            {
                return _init_conf;
            }
            // Starts this node
            int start(const CreateShardRequest& req);

            bool is_leader() const
            {
                return _leader_term.load(butil::memory_order_acquire) > 0;
            }

            ShardState get_state()
            {
                return _state;
            }

            // Shut this node down.
            void shutdown()
            {
                if (_node)
                {
                    _node->shutdown(NULL);
                }
            }

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
                LOG(INFO) << "Node becomes leader";
            }
            void on_leader_stop(const butil::Status& status)
            {
                _leader_term.store(-1, butil::memory_order_release);
                LOG(INFO) << "Node stepped down : " << status;
            }

            void on_shutdown()
            {
                LOG(INFO) << "This node is down";
            }
            void on_error(const ::braft::Error& e)
            {
                LOG(ERROR) << "Met raft error " << e;
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
            CreateShardRequest _init_conf;
            braft::Node* volatile _node;
            butil::atomic<int64_t> _leader_term;
            ShardState _state;
    };

    class ShardManager
    {
        private:
            typedef std::map<ShardNodeId, ShardNode*> ShardNodeTable;
            std::string _init_conf_path;
            std::mutex _shards_mutex;
            ShardNodeTable _shards;
            WorkNodeInitConf _init_conf;
            int sync_shards_info();
            int load_shards();
        public:
            int init();
            int create_shard(const CreateShardRequest& req);
            int remove_shard(const DeleteShardRequest& req);
            void fill_heartbeat_request(NodeHeartbeatRequest& req);
    };

    std::string cluster_group_name(const std::string& cluster, const std::string& index, int32_t shard_idx);

    extern ShardManager g_shards;

    class ShardServiceImpl
    {
        private:
    };
}

#endif /* SRC_WORKER_SHARD_SERVICE_H_ */
