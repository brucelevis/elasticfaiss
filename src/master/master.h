/*
 * master.h
 *
 *  Created on: 2018年7月15日
 *      Author: wangqiying
 */

#ifndef SRC_MASTER_MASTER_H_
#define SRC_MASTER_MASTER_H_

#include <unordered_map>
#include <stdint.h>
#include <mutex>
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <butil/threading/simple_thread.h>
#include <butil/synchronization/waitable_event.h>
#include "proto/master.pb.h"
#include "common/proto_helper.h"
#include "common/elasticfaiss.h"
#include "worker/shard_service.h"

namespace elasticfaiss
{
    class Master;
    template<typename REQ, typename RES>
    class MasterOpClosure: public braft::Closure, public ReqResProtoHolder
    {
        public:
            MasterOpClosure(Master* m, const REQ* request, RES* response, google::protobuf::Closure* done)
                    : _master(m), _request(request), _response(response), _done(done)
            {
            }
            ~MasterOpClosure()
            {
            }
            const ::google::protobuf::Message* get_request()
            {
                return _request;
            }
            ::google::protobuf::Message* get_response()
            {
                return _response;
            }
            const REQ* request() const
            {
                return _request;
            }
            RES* response() const
            {
                return _response;
            }
            void Run();

        private:
            Master* _master;
            const REQ* _request;
            RES* _response;
            google::protobuf::Closure* _done;
    };

    struct ShardIndexKey
    {
            std::string cluster;
            std::string index;
            int32_t idx;
            ShardIndexKey()
                    : idx(0)
            {
            }
            bool operator<(const ShardIndexKey& other) const
            {
                int ret = cluster.compare(other.cluster);
                if (0 != ret)
                {
                    return ret < 0 ? true : false;
                }
                ret = index.compare(other.index);
                if (0 != ret)
                {
                    return ret < 0 ? true : false;
                }
                return idx < other.idx;
            }
    };

    typedef std::deque<ShardNodeMeta> ShardNodeMetaArray;
    typedef std::map<ShardIndexKey, ShardNodeMetaArray> ShardNodeMetaTable;
    typedef std::unordered_map<std::string, WorkNode*> WorkNodeTable;
    typedef std::unordered_map<std::string, IndexConf*> IndexConfTable;
    typedef std::unordered_map<std::string, WorkNodeTable> ClusterWorkNodeTable;
    typedef std::unordered_map<std::string, IndexConfTable> ClusterIndexConfTable;
    typedef std::unordered_map<std::string, ClusterState*> ClusterStateTable;
    typedef std::map<ShardIndexKey, braft::Configuration> ShardConfTable;
    //typedef std::unordered_map<std::string, brpc::Channel*> NodeChannelTable;

    class Master: public braft::StateMachine, public butil::SimpleThread
    {
        public:
            Master()
                    : butil::SimpleThread("master_routine"), _node(NULL), _leader_term(-1), _routine_event(false,
                            false), _check_term(false), _running(false)
            {
            }
            ~Master()
            {
                delete _node;
            }

            // Starts this node
            int start();

            template<typename REQ, typename RES>
            void applyRPC(const REQ* req, int8_t op, RES* res, ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard done_guard(done);
                const int64_t term = _leader_term.load(butil::memory_order_relaxed);
                if (term < 0)
                {
                    return redirect(res);
                }
                butil::IOBuf log;
                log.push_back(op);
                butil::IOBufAsZeroCopyOutputStream wrapper(&log);
                if (!req->SerializeToZeroCopyStream(&wrapper))
                {
                    LOG(ERROR) << "Fail to serialize request";
                    res->set_success(false);
                    return;
                }
                // Apply this log as a braft::Task
                braft::Task task;
                task.data = &log;
                // This callback would be iovoked when the task actually excuted or
                // fail
                task.done = new MasterOpClosure<REQ, RES>(this, req, res, done_guard.release());
                if (_check_term)
                {
                    // ABA problem can be avoid if expected_term is set
                    task.expected_term = term;
                }
                // Now the task is applied to the group, waiting for the result.
                return _node->apply(task);
            }
            // Impelements Service methods
            void bootstrap(const ::elasticfaiss::BootstrapRequest* request, ::elasticfaiss::BootstrapResponse* response,
                    ::google::protobuf::Closure* done);
            void node_heartbeat(const ::elasticfaiss::NodeHeartbeatRequest* request,
                    ::elasticfaiss::NodeHeartbeatResponse* response, ::google::protobuf::Closure* done);
            void get_cluster_state(const ::elasticfaiss::GetClusterStateRequest* request,
                    ::elasticfaiss::GetClusterStateResponse* response);
            void create_index(const ::elasticfaiss::CreateIndexRequest* request,
                    ::elasticfaiss::CreateIndexResponse* response, ::google::protobuf::Closure* done);
            void delete_index(const ::elasticfaiss::DeleteIndexRequest* request,
                    ::elasticfaiss::DeleteIndexResponse* response, ::google::protobuf::Closure* done);
            void update_index(const ::elasticfaiss::UpdateIndexRequest* request,
                    ::elasticfaiss::UpdateIndexResponse* response, ::google::protobuf::Closure* done);

            bool is_leader() const;

            // Shut this node down.
            void shutdown();

            // Blocking this thread until the node is eventually down.
            void join();

            template<typename RES>
            void redirect(RES* response)
            {
                response->set_success(false);
                if (_node)
                {
                    braft::PeerId leader = _node->leader_id();
                    if (!leader.is_empty())
                    {
                        response->set_redirect(leader.to_string());
                    }
                }
            }
        private:
            int transaction_create_index(const std::string& cluster, const IndexConf& conf);
            int rpc_delete_index_shard(const std::string& cluster, const std::string& name, int32_t idx,
                    const std::string& node);
            int rpc_create_index_shard(const std::string& cluster, const IndexConf& conf, int32_t idx,
                    const std::string& node, const std::string& all_nodes);
            int index_shard_add_peer(const std::string& cluster, const IndexConf& conf, int32_t idx,
                    const std::string& node, const std::string& all_nodes);
            int remove_index_shards(const std::string& cluster, IndexConf& conf, int32_t idx, const ShardNodeMetaArray& nodes);
            int add_index_shard(const std::string& cluster, IndexConf& conf, int32_t idx, const ShardNodeMetaArray& nodes);
            int select_nodes4index(const std::string& cluster, int32_t replica_count, const StringSet& current_nodes,
                    std::vector<std::string>& nodes);
            void check_node_timeout(ShardNodeMetaTable& index_nodes);
            void check_index(const ShardNodeMetaTable& index_nodes);
            void routine();
            void Run();
            // @braft::StateMachine
            void on_apply(braft::Iterator& iter);

            void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done);

            int on_snapshot_load(braft::SnapshotReader* reader);

            void on_leader_start(int64_t term);
            void on_leader_stop(const butil::Status& status);

            void on_shutdown();
            void on_error(const ::braft::Error& e);
            void on_configuration_committed(const ::braft::Configuration& conf);
            void on_stop_following(const ::braft::LeaderChangeContext& ctx);
            void on_start_following(const ::braft::LeaderChangeContext& ctx);

            int handle_node_bootstrap(const ::elasticfaiss::BootstrapRequest* request,
                    ::elasticfaiss::BootstrapResponse* response);
            int handle_node_heartbeat(const ::elasticfaiss::NodeHeartbeatRequest* request,
                    ::elasticfaiss::NodeHeartbeatResponse* response);
            int handle_create_index(const ::elasticfaiss::CreateIndexRequest* request,
                    ::elasticfaiss::CreateIndexResponse* response);
            int handle_delete_index(const ::elasticfaiss::DeleteIndexRequest* request,
                    ::elasticfaiss::DeleteIndexResponse* response);
            int handle_update_index(const ::elasticfaiss::UpdateIndexRequest* request,
                    ::elasticfaiss::UpdateIndexResponse* response);

        private:
            braft::Node* volatile _node;
            butil::atomic<int64_t> _leader_term;
            butil::WaitableEvent _routine_event;

            ClusterWorkNodeTable _cluster_workers;
            ClusterStateTable _cluster_states;
            ClusterIndexConfTable _cluster_index_confs;
            ShardConfTable _cluster_shard_confs;
            std::mutex _cluster_mutex;
            std::mutex _index_mutex;

            //NodeChannelTable _node_channels;
            std::mutex _channel_mutex;

            bool _check_term;
            std::atomic<bool> _running;
    };

    template<typename REQ, typename RES>
    void MasterOpClosure<REQ, RES>::Run()
    {
        std::unique_ptr<MasterOpClosure> self_guard(this);
        brpc::ClosureGuard done_guard(_done);
        if (status().ok())
        {
            return;
        }
        // Try redirect if this request failed.
        _master->redirect(_response);
    }

    class MasterServiceImpl: public MasterService
    {
        public:
            explicit MasterServiceImpl(Master* master)
                    : _master(master)
            {
            }
            void bootstrap(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::BootstrapRequest* request, ::elasticfaiss::BootstrapResponse* response,
                    ::google::protobuf::Closure* done)
            {
                return _master->bootstrap(request, response, done);
            }
            void node_heartbeat(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::NodeHeartbeatRequest* request,
                    ::elasticfaiss::NodeHeartbeatResponse* response, ::google::protobuf::Closure* done)
            {
                return _master->node_heartbeat(request, response, done);
            }
            void get_cluster_state(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::GetClusterStateRequest* request,
                    ::elasticfaiss::GetClusterStateResponse* response, ::google::protobuf::Closure* done)
            {
                brpc::ClosureGuard done_guard(done);
                return _master->get_cluster_state(request, response);
            }
            void create_index(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::CreateIndexRequest* request, ::elasticfaiss::CreateIndexResponse* response,
                    ::google::protobuf::Closure* done)
            {
                return _master->create_index(request, response, done);
            }
            void delete_index(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::DeleteIndexRequest* request, ::elasticfaiss::DeleteIndexResponse* response,
                    ::google::protobuf::Closure* done)
            {
                return _master->delete_index(request, response, done);
            }
            void update_index(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::UpdateIndexRequest* request, ::elasticfaiss::UpdateIndexResponse* response,
                    ::google::protobuf::Closure* done)
            {
                return _master->update_index(request, response, done);
            }
        private:
            Master* _master;
    };
}

#endif /* SRC_MASTER_MASTER_H_ */
