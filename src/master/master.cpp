#include <pthread.h>
#include <memory>
#include <map>
#include <utility>
#include <gflags/gflags.h>              // DEFINE_*
//#include <glog/logging.h>
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <brpc/channel.h>
#include <butil/time.h>

#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include <braft/cli.h>
#include "master.h"
#include "proto/work_node.pb.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(node_heartbeat_timeout_ms, 10000,
        "Change node state to timeout after such milliseconds if no heartbeat received");
DEFINE_int32(master_snapshot_interval, 30, "Interval between each snapshot");

namespace elasticfaiss
{
    class Master;
// Define types for different operation
    enum MasterOpType
    {
        OP_UNKNOWN = 0,
        OP_NODE_BOOTSTRAP = 1,
        OP_NODE_HEARTBEAT = 2,
        OP_CREATE_INDEX = 3,
        OP_DELETE_INDEX = 4,
        OP_UPDATE_INDEX = 5,
    };

    int Master::select_nodes4index(const std::string& cluster, int32_t replica_count, const StringSet& current_nodes,
            std::vector<std::string>& nodes)
    {
        std::lock_guard<std::mutex> guard(_cluster_mutex);
        WorkNodeTable wnodes = _cluster_workers[cluster];
        int32_t expected_count = replica_count - current_nodes.size();
        if (wnodes.size() < (size_t) replica_count)
        {
            LOG(ERROR) << "No enough nodes in cluster:" << cluster << " to create index with replica:" << replica_count;
            return -1;
        }
        while (nodes.size() != (size_t) expected_count && nodes.empty())
        {
            std::string min_weight_node;
            int32_t min_shard_count = 0;
            WorkNodeTable::iterator it = wnodes.begin();
            while (it != wnodes.end())
            {
                WorkNode* node = it->second;
                if (node->state() != WNODE_ACTIVE || current_nodes.count(node->peer_id()) > 0)
                {
                    it = wnodes.erase(it);
                    continue;
                }
                if (node->shards_size() == 0)
                {
                    nodes.push_back(node->peer_id());
                    it = wnodes.erase(it);
                    min_weight_node.clear();
                    break;
                }
                if (0 == min_shard_count || min_shard_count < node->shards_size())
                {
                    min_weight_node = node->peer_id();
                    min_shard_count = node->shards_size();
                }
                it++;
            }
            if (!min_weight_node.empty())
            {
                nodes.push_back(min_weight_node);
                wnodes.erase(min_weight_node);
            }
        }
        if (nodes.size() != (size_t) expected_count)
        {
            LOG(ERROR) << "No enough valid nodes in cluster:" << cluster << " to create index with replica:"
                    << replica_count;
            nodes.clear();
            return -1;
        }
        return 0;
    }

    int Master::add_index_shard(const std::string& cluster, IndexConf& conf, int32_t idx,
            const ShardNodeMetaArray& nodes)
    {
        ShardIndexKey key;
        key.cluster = cluster;
        key.index = conf.name();
        key.idx = idx;
        ShardConfTable::iterator found = _cluster_shard_confs.find(key);
        if (found == _cluster_shard_confs.end())
        {
            LOG(ERROR) << "No shard config found for :" << cluster << "_" << key.index << "_" << key.idx;
            return -1;
        }
        braft::Configuration& cfg = found->second;
        std::vector<std::string> new_nodes;
        StringSet current;
        for (const auto& node : nodes)
        {
            current.insert(node.node_peer);
        }
        if (0 == select_nodes4index(cluster, conf.number_of_replicas(), current, new_nodes))
        {
            if (nodes[0].is_leader)
            {

            }
            else
            {

            }
        }
        return -1;
    }

    int Master::remove_index_shards(const std::string& cluster, IndexConf& conf, int32_t idx,
            const ShardNodeMetaArray& nodes)
    {
        ShardIndexKey key;
        key.cluster = cluster;
        key.index = conf.name();
        key.idx = idx;
        ShardConfTable::iterator found = _cluster_shard_confs.find(key);
        if (found == _cluster_shard_confs.end())
        {
            LOG(ERROR) << "No shard config found for :" << cluster << "_" << key.index << "_" << key.idx;
            return -1;
        }
        braft::Configuration& cfg = found->second;
        for (int32_t i = 0; i < nodes.size(); i++)
        {
            butil::EndPoint node_peer;
            butil::str2endpoint(nodes[i].node_peer.c_str(), &node_peer);
            braft::PeerId id(node_peer);
            if (!cfg.contains(id))
            {
                rpc_delete_index_shard(cluster, conf.name(), idx, nodes[i].node_peer);
            }
        }
        return 0;
    }

    void Master::check_index(const ShardNodeMetaTable& index_nodes)
    {
        std::lock_guard<std::mutex> guard(_index_mutex);
        ClusterIndexConfTable::iterator it = _cluster_index_confs.begin();
        while (it != _cluster_index_confs.end())
        {
            const std::string& cluster_name = it->first;
            auto iit = it->second.begin();
            while (iit != it->second.end())
            {
                const std::string& index_name = iit->first;
                IndexConf* conf = iit->second;
                for (int i = 0; i < conf->number_of_shards(); i++)
                {
                    ShardIndexKey key;
                    key.cluster = cluster_name;
                    key.index = index_name;
                    key.idx = i;

                    auto found = index_nodes.find(key);
                    if (found != index_nodes.end())
                    {
                        const ShardNodeMetaArray& nodes = found->second;
                        if (nodes.size() == conf->number_of_replicas())
                        {
                            continue;
                        }
                        if (nodes.empty())
                        {
                            LOG(ERROR) << "No nodes exist for index:" << cluster_name << "_" << index_name;
                            continue;
                        }
                        if (nodes.size() < conf->number_of_replicas())
                        {
                            add_index_shard(cluster_name, *conf, key.idx, nodes);
                        }
                        else
                        {
                            remove_index_shards(cluster_name, *conf, key.idx, nodes);
                        }
                    }
                    else
                    {
                        LOG(ERROR) << "No nodes exist for index:" << cluster_name << "_" << index_name;
                    }
                }
                iit++;
            }
            it++;
        }
    }

    void Master::check_node_timeout(ShardNodeMetaTable& index_nodes)
    {
        int64_t now = butil::gettimeofday_ms();
        std::lock_guard<std::mutex> guard(_cluster_mutex);
        ClusterStateTable::iterator it = _cluster_states.begin();
        while (it != _cluster_states.end())
        {
            const std::string& cluster_name = it->first;
            ClusterState* s = it->second;
            for (int i = 0; i < s->nodes_size(); i++)
            {
                if (now - s->nodes(i).last_active_ms() > FLAGS_node_heartbeat_timeout_ms)
                {
                    LOG(INFO) << "Node:" << s->nodes(i).peer_id() << " is timeout.";
                    s->mutable_nodes(i)->set_state(WNODE_TIMEOUT);
                }
                else
                {
                    WorkNode* node = s->mutable_nodes(i);
                    if (node->state() == WNODE_ACTIVE)
                    {
                        for (int j = 0; j < node->shards_size(); j++)
                        {
                            ShardIndexKey key;
                            key.cluster = cluster_name;
                            key.index = node->shards(j).name();
                            key.idx = node->shards(j).idx();
                            ShardNodeMeta meta;
                            meta.node_peer = node->peer_id();
                            meta.is_leader = node->shards(j).is_leader();
                            if (node->shards(j).is_leader())
                            {
                                index_nodes[key].push_front(meta);
                            }
                            else
                            {
                                index_nodes[key].push_back(meta);
                            }
                        }
                    }
                }
            }
            it++;
        }
    }

    void Master::routine()
    {
        LOG(INFO) << "master routine";
        ShardNodeMetaTable index_nodes;
        check_node_timeout(index_nodes);
        check_index(index_nodes);
    }
    void Master::Run()
    {
        butil::TimeDelta wtime = butil::TimeDelta::FromMilliseconds(1500);
        while (_running)
        {
            _routine_event.TimedWait(wtime);
            if (!_running)
            {
                break;
            }
            routine();
        }
    }

// Starts this node
    int Master::start()
    {
        _check_term = FLAGS_check_term;
        //butil::EndPoint addr(butil::my_ip(), port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(g_masters) != 0)
        {
            LOG(ERROR) << "Fail to parse configuration `" << g_masters << '\'';
            return -1;
        }
        node_options.election_timeout_ms = g_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_master_snapshot_interval;
        std::string prefix = "local://" + g_home + "/master";
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(g_master_group, braft::PeerId(g_listen_endpoint));
        if (node->init(node_options) != 0)
        {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        _running = true;
        Start();
        return 0;
    }

// Impelements Service methods
    void Master::bootstrap(const ::elasticfaiss::BootstrapRequest* request, ::elasticfaiss::BootstrapResponse* response,
            ::google::protobuf::Closure* done)
    {
        applyRPC(request, OP_NODE_BOOTSTRAP, response, done);
    }
    void Master::node_heartbeat(const ::elasticfaiss::NodeHeartbeatRequest* request,
            ::elasticfaiss::NodeHeartbeatResponse* response, ::google::protobuf::Closure* done)
    {
        applyRPC(request, OP_NODE_HEARTBEAT, response, done);
    }
    void Master::create_index(const ::elasticfaiss::CreateIndexRequest* request,
            ::elasticfaiss::CreateIndexResponse* response, ::google::protobuf::Closure* done)
    {
        applyRPC(request, OP_CREATE_INDEX, response, done);
    }
    void Master::delete_index(const ::elasticfaiss::DeleteIndexRequest* request,
            ::elasticfaiss::DeleteIndexResponse* response, ::google::protobuf::Closure* done)
    {
        applyRPC(request, OP_DELETE_INDEX, response, done);
    }
    void Master::update_index(const ::elasticfaiss::UpdateIndexRequest* request,
            ::elasticfaiss::UpdateIndexResponse* response, ::google::protobuf::Closure* done)
    {
        applyRPC(request, OP_UPDATE_INDEX, response, done);
    }
    void Master::get_cluster_state(const ::elasticfaiss::GetClusterStateRequest* request,
            ::elasticfaiss::GetClusterStateResponse* response)
    {
        if (!is_leader())
        {
            return redirect(response);
        }
        response->set_success(true);
        std::lock_guard<std::mutex> guard(_cluster_mutex);
        auto found = _cluster_states.find(request->cluster());
        if (found != _cluster_states.end())
        {
            response->mutable_state()->CopyFrom(*(found->second));
        }
    }

    bool Master::is_leader() const
    {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

// Shut this node down.
    void Master::shutdown()
    {
        if (_running)
        {
            _running = false;
            _routine_event.Signal();
            Join();
        }
        if (_node)
        {
            _node->shutdown(NULL);
        }
    }

// Blocking this thread until the node is eventually down.
    void Master::join()
    {
        if (_node)
        {
            _node->join();
        }
    }

// @braft::StateMachine
    void Master::on_apply(braft::Iterator& iter)
    {
        // A batch of tasks are committed, which must be processed through
        // |iter|
        for (; iter.valid(); iter.next())
        {
            butil::IOBuf data = iter.data();
            uint8_t type = OP_UNKNOWN;
            data.cutn(&type, sizeof(uint8_t));
            switch (type)
            {
                case OP_NODE_BOOTSTRAP:
                case OP_NODE_HEARTBEAT:
                case OP_CREATE_INDEX:
                case OP_UPDATE_INDEX:
                case OP_DELETE_INDEX:
                {
                    break;
                }
                default:
                {
                    continue;
                }
            }
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            ::google::protobuf::Message* response = NULL;
            const ::google::protobuf::Message* request = NULL;
            std::unique_ptr<::google::protobuf::Message> log_req;
            braft::AsyncClosureGuard closure_guard(iter.done());
            if (iter.done())
            {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                ReqResProtoHolder* c = dynamic_cast<ReqResProtoHolder*>(iter.done());
                response = c->get_response();
                request = c->get_request();
            }
            else
            {
                // Have to parse FetchAddRequest from this log.
                butil::IOBufAsZeroCopyInputStream wrapper(data);
                switch (type)
                {
                    case OP_NODE_BOOTSTRAP:
                    {
                        log_req.reset(new ::elasticfaiss::BootstrapRequest);
                        break;
                    }
                    case OP_NODE_HEARTBEAT:
                    {
                        log_req.reset(new ::elasticfaiss::NodeHeartbeatRequest);
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                CHECK(log_req.get()->ParseFromZeroCopyStream(&wrapper));
                request = log_req.get();
            }

            switch (type)
            {
                case OP_NODE_BOOTSTRAP:
                {
                    handle_node_bootstrap((const ::elasticfaiss::BootstrapRequest*) request,
                            (::elasticfaiss::BootstrapResponse*) response);
                    break;
                }
                case OP_NODE_HEARTBEAT:
                {
                    handle_node_heartbeat((const ::elasticfaiss::NodeHeartbeatRequest*) request,
                            (::elasticfaiss::NodeHeartbeatResponse*) response);
                    break;
                }
                default:
                {
                    break;
                }
            }
        }
    }

    struct SnapshotArg
    {
            ClusterStateTable states;
            braft::SnapshotWriter* writer;
            braft::Closure* done;
    };

    static void *save_snapshot(void* arg)
    {
        SnapshotArg* sa = (SnapshotArg*) arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        // Serialize StateMachine to the snapshot
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/states";
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        // Use protobuf to store the snapshot for backward compatibility.
        MasterSnapshot s;
        for (auto kv : sa->states)
        {
            s.add_states()->CopyFrom(*(kv.second));
        }
        braft::ProtoBufFile pb_file(snapshot_path);
        if (pb_file.save(&s, true) != 0)
        {
            sa->done->status().set_error(EIO, "Fail to save master snapshot pb_file");
            return NULL;
        }
        // Snapshot is a set of files in raft. Add the only file into the
        // writer here.
        if (sa->writer->add_file("states") != 0)
        {
            sa->done->status().set_error(EIO, "Fail to add file to writer");
            return NULL;
        }
        return NULL;
    }

    void Master::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done)
    {
        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotArg* arg = new SnapshotArg;
        {
            std::lock_guard<std::mutex> guard(_cluster_mutex);
            arg->states = _cluster_states;
        }
        //arg->value = _value.load(butil::memory_order_relaxed);
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int Master::on_snapshot_load(braft::SnapshotReader* reader)
    {
        // Load snasphot from reader, replacing the running StateMachine
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        if (reader->get_file_meta("states", NULL) != 0)
        {
            LOG(ERROR) << "Fail to find `states' on " << reader->get_path();
            return -1;
        }
        std::string snapshot_path = reader->get_path() + "/states";
        braft::ProtoBufFile pb_file(snapshot_path);
        MasterSnapshot s;
        if (pb_file.load(&s) != 0)
        {
            LOG(ERROR) << "Fail to load master snapshot from " << snapshot_path;
            return -1;
        }
        {
            std::lock_guard<std::mutex> guard(_cluster_mutex);
            _cluster_workers.clear();
            _cluster_states.clear();
            LOG(INFO) << "all cluster size:" << s.states_size();
            for (int i = 0; i < s.states_size(); i++)
            {
                LOG(INFO) << "cluster:" << s.states(i).name();
                ClusterState* st = new ClusterState;
                _cluster_states[s.states(i).name()] = st;
                st->CopyFrom(s.states(i));
                for (int j = 0; j < st->nodes_size(); j++)
                {
                    LOG(INFO) << "cluster:" << s.states(i).name() << " add node peer:"
                            << s.states(i).nodes(j).peer_id();
                    _cluster_workers[s.states(i).name()][s.states(i).nodes(j).peer_id()] = st->mutable_nodes(j);
                }
            }
        }
        return 0;
    }

    void Master::on_leader_start(int64_t term)
    {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void Master::on_leader_stop(const butil::Status& status)
    {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void Master::on_shutdown()
    {
        LOG(INFO) << "This node is down";
    }
    void Master::on_error(const ::braft::Error& e)
    {
        LOG(ERROR) << "Met raft error " << e;
    }
    void Master::on_configuration_committed(const ::braft::Configuration& conf)
    {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void Master::on_stop_following(const ::braft::LeaderChangeContext& ctx)
    {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void Master::on_start_following(const ::braft::LeaderChangeContext& ctx)
    {
        LOG(INFO) << "Node start following " << ctx;
    }

    int Master::handle_node_bootstrap(const ::elasticfaiss::BootstrapRequest* request,
            ::elasticfaiss::BootstrapResponse* response)
    {
        const std::string& cluster_name = request->cluster();
        const std::string& node_peer = request->node_peer();
        std::lock_guard<std::mutex> guard(_cluster_mutex);
        WorkNode* wnode = _cluster_workers[cluster_name][node_peer];
        ClusterState* cstate = _cluster_states[cluster_name];
        if (NULL == cstate)
        {
            cstate = new ClusterState;
            _cluster_states[cluster_name] = cstate;
            cstate->set_name(cluster_name);
        }
        if (NULL == wnode)
        {
            wnode = cstate->add_nodes();
            wnode->set_peer_id(node_peer);
        }
        int64_t active_ms = request->has_boot_ms() ? request->boot_ms() : butil::gettimeofday_ms();
        wnode->set_last_active_ms(active_ms);
        wnode->set_state(WNODE_ACTIVE);
        if (NULL != response)
        {
            response->set_success(true);
            //response->mu
        }
        return 0;
    }
    int Master::handle_node_heartbeat(const ::elasticfaiss::NodeHeartbeatRequest* request,
            ::elasticfaiss::NodeHeartbeatResponse* response)
    {
        const std::string& cluster_name = request->cluster();
        const std::string& node_peer = request->node_peer();

        std::lock_guard<std::mutex> guard(_cluster_mutex);
        auto found = _cluster_workers.find(cluster_name);
        if (found == _cluster_workers.end())
        {
            if (NULL != response)
            {
                response->set_success(false);
            }
            return 0;
        }
        auto found2 = found->second.find(node_peer);
        if (found2 == found->second.end())
        {
            if (NULL != response)
            {
                response->set_success(false);
            }
            return 0;
        }
        else
        {
            int64_t active_tms = request->has_active_ms() ? request->active_ms() : butil::gettimeofday_ms();
            found2->second->set_last_active_ms(active_tms);
        }
        if (NULL != response)
        {
            response->set_success(true);
        }
        return 0;
    }

    int Master::rpc_delete_index_shard(const std::string& cluster, const std::string& name, int32_t idx,
            const std::string& node)
    {
        brpc::Channel channel;
        if (channel.Init(node.c_str(), NULL) != 0)
        {
            LOG(ERROR) << "Fail to init channel to " << node;
            return -1;
        }
        WorkNodeService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        DeleteShardRequest request;
        request.set_cluster(cluster);
        request.set_name(name);
        request.set_idx(idx);
        //request.set_allocated_index_idx()
        DeleteShardResponse response;
        stub.delete_shard(&cntl, &request, &response, NULL);
        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to send request to " << node;
            return -1;
        }
        if (!response.success())
        {
            LOG(ERROR) << "Fail to rpc to " << node << " with error:" << response.error();
            return -1;
        }
        return 0;
    }

    int Master::index_shard_add_peer(const std::string& cluster, const IndexConf& conf, int32_t idx,
            const std::string& node, const std::string& all_nodes)
    {

        //braft::cli::add_peer();
    }

    int Master::rpc_create_index_shard(const std::string& cluster, const IndexConf& conf, int32_t idx,
            const std::string& node, const std::string& all_nodes)
    {
        brpc::Channel channel;
        if (channel.Init(node.c_str(), NULL) != 0)
        {
            LOG(ERROR) << "Fail to init channel to " << node;
            return -1;
        }
        WorkNodeService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        CreateShardRequest request;
        request.set_cluster(cluster);
        request.mutable_conf()->CopyFrom(conf);
        request.set_shard_nodes(all_nodes);
        request.set_idx(idx);
        //request.set_allocated_index_idx()
        CreateShardResponse response;
        stub.create_shard(&cntl, &request, &response, NULL);
        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to send request to " << node;
            return -1;
        }
        if (!response.success())
        {
            LOG(ERROR) << "Fail to rpc to " << node << " with error:" << response.error();
            return -1;
        }
        return 0;
    }

    int Master::transaction_create_index(const std::string& cluster, const IndexConf& conf)
    {
        typedef std::map<ShardIndexKey, std::vector<std::string> > SuccessNodes;
        SuccessNodes success_nodes;
        bool create_index_fail = false;
        for (int32_t i = 0; i < conf.number_of_shards(); i++)
        {
            std::vector<std::string> nodes;
            StringSet empty;
            ShardIndexKey key;
            key.cluster = cluster;
            key.index = conf.name();
            key.idx = i;

            select_nodes4index(cluster, conf.number_of_replicas(), empty, nodes);
            if ((int32_t) nodes.size() == conf.number_of_replicas())
            {
                std::string nodes_str = string_join_container(nodes, ",");
                for (auto& node : nodes)
                {
                    if (0 != rpc_create_index_shard(cluster, conf, i, node, nodes_str))
                    {
                        create_index_fail = true;
                        break;
                    }
                    else
                    {
                        success_nodes[key].push_back(node);
                    }
                }
                if (!create_index_fail)
                {
                    braft::Configuration cfg;
                    cfg.parse_from(nodes_str);
                    _cluster_shard_confs[key] = cfg;
                }
            }
            else
            {
                create_index_fail = true;
            }
            if (create_index_fail)
            {
                break;
            }
        }
        if (create_index_fail)
        {
            auto it = success_nodes.begin();
            while (it != success_nodes.end())
            {
                const ShardIndexKey& key = it->first;
                for (size_t i = 0; i < it->second.size(); i++)
                {
                    rpc_delete_index_shard(cluster, conf.name(), key.idx, it->second[i]);
                }
                _cluster_shard_confs.erase(key);
                it++;
            }
            auto closure = new MessagePairClosure<DeleteIndexRequest, DeleteIndexResponse>;
            DeleteIndexRequest& dreq = closure->req;
            dreq.set_cluster(cluster);
            dreq.set_index_name(conf.name());
            DeleteIndexResponse& dres = closure->res;
            applyRPC(&dreq, OP_DELETE_INDEX, &dres, closure);
        }
        return create_index_fail ? -1 : 0;
    }

    int Master::handle_create_index(const ::elasticfaiss::CreateIndexRequest* request,
            ::elasticfaiss::CreateIndexResponse* response)
    {
        const std::string& cluster_name = request->cluster();
        const std::string& index_name = request->conf().name();
        {
            std::lock_guard<std::mutex> guard(_index_mutex);
            IndexConf* conf = _cluster_index_confs[cluster_name][index_name];
            if (NULL == conf)
            {
                conf = new IndexConf;
                conf->CopyFrom(request->conf());
                _cluster_index_confs[cluster_name][index_name] = conf;
            }
        }
        if (NULL != response)  //leader peer do later work
        {
            int ret = transaction_create_index(cluster_name, request->conf());
            if (0 != ret)
            {
                response->set_success(false);
                response->set_error("failed to create index");
            }
            else
            {
                response->set_success(true);
            }
        }
        return 0;
    }
    int Master::handle_delete_index(const ::elasticfaiss::DeleteIndexRequest* request,
            ::elasticfaiss::DeleteIndexResponse* response)
    {
        const std::string& cluster_name = request->cluster();
        const std::string& index_name = request->index_name();
        {
            std::lock_guard<std::mutex> guard(_index_mutex);
            IndexConfTable& t = _cluster_index_confs[cluster_name];
            IndexConfTable::iterator found = t.find(index_name);
            if (found != t.end())
            {
                IndexConf* conf = found->second;
                for (int32_t i = 0; i < conf->number_of_shards(); i++)
                {
                    ShardIndexKey key;
                    key.cluster = cluster_name;
                    key.index = index_name;
                    key.idx = i;
                    _cluster_shard_confs.erase(key);
                }
                delete found->second;
                t.erase(found);
            }
        }
        if (NULL != response)
        {
            response->set_success(true);
            typedef std::pair<std::string, int32_t> NodeWithIdx;
            typedef std::vector<NodeWithIdx> NodeWithIdxArray;
            NodeWithIdxArray to_delete;
            {
                std::lock_guard<std::mutex> guard(_index_mutex);
                WorkNodeTable& nodes = _cluster_workers[cluster_name];
                for (auto& kv : nodes)
                {
                    const std::string& peer_id = kv.first;
                    WorkNode* node = kv.second;
                    for (int i = 0; i < node->shards_size(); i++)
                    {
                        if (node->shards(i).name() == index_name)
                        {
                            to_delete.push_back(std::make_pair(peer_id, node->shards(i).idx()));
                        }
                    }
                }
            }
            for (auto& node_idx : to_delete)
            {
                rpc_delete_index_shard(cluster_name, index_name, node_idx.second, node_idx.first);
            }
        }
        return 0;
    }
    int Master::handle_update_index(const ::elasticfaiss::UpdateIndexRequest* request,
            ::elasticfaiss::UpdateIndexResponse* response)
    {
        return 0;
    }

}  // namespace example
