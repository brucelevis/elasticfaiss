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
#include "google/protobuf/util/message_differencer.h"

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

    int Master::allocate_nodes4index(const IndexConf& conf, const StringSet& current_nodes, StringSet& nodes, int limit)
    {
        //std::lock_guard<std::mutex> guard(_cluster_mutex);
        ClusterState& c = _cluster_state;
        int32_t expected_count = conf.number_of_replicas() - current_nodes.size();
        if (limit > 0 && limit < expected_count)
        {
            expected_count = limit;
        }
        std::set<WorkNode*> avaliable_nodes;
        for (int i = 0; i < c.nodes_size(); i++)
        {
            WorkNode* node = c.mutable_nodes(i);
            if (node->state() != WNODE_ACTIVE || current_nodes.count(node->peer_id()) > 0)
            {
                continue;
            }
            avaliable_nodes.insert(node);
        }
        if (avaliable_nodes.size() < (size_t) expected_count)
        {
            LOG(ERROR) << "No enough nodes in cluster to create index:" << conf.name() << " with replica:"
                    << conf.number_of_replicas();
            return -1;
        }
        while (nodes.size() != (size_t) expected_count && !avaliable_nodes.empty())
        {
            WorkNode* min_weight_node = NULL;
            int32_t min_shard_count = 0;
            auto it = avaliable_nodes.begin();
            while (it != avaliable_nodes.end())
            {
                WorkNode* node = *it;
                if (node->shards_size() == 0)
                {
                    nodes.insert(node->peer_id());
                    avaliable_nodes.erase(it);
                    min_weight_node = NULL;
                    break;
                }
                if (0 == min_shard_count || min_shard_count < node->shards_size())
                {
                    min_weight_node = node;
                    min_shard_count = node->shards_size();
                }
                it++;
            }
            if (NULL != min_weight_node)
            {
                nodes.insert(min_weight_node->peer_id());
                avaliable_nodes.erase(min_weight_node);
            }
        }
        if (nodes.size() != (size_t) expected_count)
        {
            LOG(ERROR) << "No enough nodes in cluster to create index:" << conf.name() << " with replica:"
                    << conf.number_of_replicas();
            nodes.clear();
            return -1;
        }
        return 0;
    }

    void Master::check_index_shard(const IndexConf& conf, ShardNodes& snodes)
    {
        StringSet all_nodes;
        StringSet current_nodes;
        std::set<const WorkNode*> valid_nodes;
        std::string group_id = shard_cluster_name(conf.name(), snodes.shard_idx);
        for (const auto node : snodes.nodes)
        {
            if (node->state() == WNODE_ACTIVE)
            {
                valid_nodes.insert(node);
                current_nodes.insert(node->peer_id());
            }
            all_nodes.insert(node->peer_id());
        }
        if (!valid_nodes.empty())
        {
            LOG(ERROR) << "No valid nodes exist for index:" << group_id;
            return;
        }
        braft::Configuration braft_conf;
        std::string all_nodes_str = string_join_container(all_nodes, ",");
        braft_conf.parse_from(all_nodes_str);

        braft::cli::CliOptions opt;
        opt.timeout_ms = 2000;
        opt.max_retry = 3;
        if (!snodes.has_leader())
        {
            //only handle replica == 2
            if (valid_nodes.size() == 1 && conf.number_of_replicas() == 2)
            {
                //promote the left node to leader
                auto promote_func = [](){

                };
                start_bthread_function(promote_func);
            }
        }
        else
        {
            if (valid_nodes.size() == (size_t) conf.number_of_replicas())
            {
                return;
            }
            if (valid_nodes.size() < (size_t) conf.number_of_replicas())
            {
                StringSet new_nodes;
                if (0 == allocate_nodes4index(conf, current_nodes, new_nodes, 1))
                {
                    auto update_peer = [](){

                    };
                    start_bthread_function(update_peer);
                }
            }
        }
    }

    void Master::check_index(RoutineContext& ctx)
    {
        std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
        ShardNodeTable::iterator it = _shard_nodes.begin();
        while (it != _shard_nodes.end())
        {
            const ShardIndexKey& key = it->first;
            const std::string& index_name = key.index;
            ShardNodes& snodes = *(it->second);
            IndexConf conf;
            if (!get_index_conf(index_name, conf))
            {
                LOG(ERROR) << "No conf exist for index:" << index_name;
                it++;
                continue;
            }
            check_index_shard(conf, snodes);
            it++;
        }
    }

    void Master::check_node_timeout(RoutineContext& ctx)
    {
        int64_t now = butil::gettimeofday_ms();
        std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
        ClusterState& s = _cluster_state;
        for (int i = 0; i < s.nodes_size(); i++)
        {
            if (now - s.nodes(i).last_active_ms() > FLAGS_node_heartbeat_timeout_ms)
            {
                LOG(INFO) << "Node:" << s.nodes(i).peer_id() << " is timeout.";
                s.mutable_nodes(i)->set_state(WNODE_TIMEOUT);
            }
        }
    }

    void Master::routine()
    {
        //LOG(INFO) << "master routine";
        RoutineContext ctx;
        check_node_timeout(ctx);
        check_index(ctx);
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
            if (is_leader())
            {
                routine();
            }
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
        response->mutable_state()->CopyFrom(_cluster_state);
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
            ClusterState state;
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
        s.mutable_state()->CopyFrom(sa->state);
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
            std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
            arg->state = _cluster_state;
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
            std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
            _all_nodes.clear();
            _cluster_state.Clear();
            _shard_nodes.clear();
            _cluster_state = s.state();
            for (int j = 0; j < s.state().nodes_size(); j++)
            {
                LOG(INFO) << "cluster add node peer:" << s.state().nodes(j).peer_id();
                WorkNode* node = _cluster_state.mutable_nodes(j);
                _all_nodes[s.state().nodes(j).peer_id()] = node;
            }
            for (int j = 0; j < s.state().nodes_size(); j++)
            {
                WorkNode* node = _cluster_state.mutable_nodes(j);
                update_data_shard_nodes(node, node->shards());
            }
            LOG(INFO) << "all cluster node size:" << _cluster_state.nodes_size();
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
        const std::string& node_peer = request->node_peer();
        std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
        WorkNode* wnode = _all_nodes[node_peer];
        if (NULL == wnode)
        {
            wnode = _cluster_state.add_nodes();
            wnode->set_peer_id(node_peer);
            _all_nodes[node_peer] = wnode;
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

    void Master::update_data_shard_nodes(const WorkNode* node,
            const ::google::protobuf::RepeatedPtrField<Shard>& shards)
    {
        for (int32_t i = 0; i < shards.size(); i++)
        {
            const Shard& shard = shards.Get(i);
            if (shard.is_leader())
            {
                ShardIndexKey key;
                key.shard_idx = shard.idx();
                key.index = shard.name();
                ShardNodes* snodes = _shard_nodes[key];
                if (NULL == snodes)
                {
                    snodes = new ShardNodes;
                    _shard_nodes[key] = snodes;
                }
                snodes->shard_idx = key.shard_idx;
                snodes->leader = node;
                for (int32_t j = 0; j < shard.nodes_size(); j++)
                {
                    WorkNode* wnode = get_node(shard.nodes(j));
                    if (NULL != wnode)
                    {
                        snodes->nodes.insert(wnode);
                    }
                    else
                    {
                        LOG(ERROR) << "No node found for peer:" << shard.nodes(j);
                    }
                }
            }
        }
    }

    template<typename T>
    static bool compare_proto_repeated(const ::google::protobuf::RepeatedPtrField<T>& a,
            const ::google::protobuf::RepeatedPtrField<T>& b)
    {
        if (a.size() != b.size())
        {
            return false;
        }
        for (int i = 0; i < a.size(); i++)
        {
            if (!google::protobuf::util::MessageDifferencer::Equals(a.Get(i), b.Get(i)))
            {
                return false;
            }
        }
        return true;
    }

    int Master::handle_node_heartbeat(const ::elasticfaiss::NodeHeartbeatRequest* request,
            ::elasticfaiss::NodeHeartbeatResponse* response)
    {
        const std::string& node_peer = request->node_peer();
        std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
        auto found = _all_nodes.find(node_peer);
        if (found == _all_nodes.end())
        {
            LOG(ERROR) << "No node found for peer:" << node_peer;
            if (NULL != response)
            {
                response->set_success(false);
            }
            return 0;
        }
        WorkNode* wnode = found->second;
        if (!compare_proto_repeated(wnode->shards(), request->shards()))
        {
            for (int32_t i = 0; i < wnode->shards_size(); i++)
            {
                const Shard& shard = wnode->shards(i);
                if (shard.is_leader())
                {
                    ShardIndexKey key;
                    key.index = shard.idx();
                    key.index = shard.name();
                    auto found = _shard_nodes.find(key);
                    if (found != _shard_nodes.end())
                    {
                        delete found->second;
                        _shard_nodes.erase(found);
                    }
                }
            }
            update_data_shard_nodes(wnode, request->shards());
        }

        wnode->mutable_shards()->CopyFrom(request->shards());
        int64_t active_tms = request->has_active_ms() ? request->active_ms() : butil::gettimeofday_ms();
        wnode->set_last_active_ms(active_tms);
        if (NULL != response)
        {
            response->set_success(true);
        }
        return 0;
    }

    int Master::rpc_delete_index_shard(const std::string& name, int32_t idx, const std::string& node)
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

    int Master::rpc_create_index_shard(const IndexConf& conf, int32_t shard_idx, const std::string& node,
            const StringSet& nodes)
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
        request.mutable_conf()->mutable_conf()->CopyFrom(conf);
        request.mutable_conf()->set_shard_idx(shard_idx);
        for (const auto& node : nodes)
        {
            request.mutable_conf()->add_nodes(node);
        }
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

    int Master::transaction_create_index(const IndexConf& conf)
    {
        typedef std::map<ShardIndexKey, std::vector<std::string> > SuccessNodes;
        SuccessNodes success_nodes;
        bool create_index_fail = false;
        for (int32_t i = 0; i < conf.number_of_shards(); i++)
        {
            StringSet nodes;
            StringSet empty;
            ShardIndexKey key;
            key.index = conf.name();
            key.shard_idx = i;

            {
                std::lock_guard<bthread::Mutex> guard(_cluster_mutex);
                allocate_nodes4index(conf, empty, nodes);
            }

            if ((int32_t) nodes.size() == conf.number_of_replicas())
            {
                for (auto& node : nodes)
                {
                    if (0 != rpc_create_index_shard(conf, i, node, nodes))
                    {
                        create_index_fail = true;
                        break;
                    }
                    else
                    {
                        success_nodes[key].push_back(node);
                    }
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
                    rpc_delete_index_shard(conf.name(), key.shard_idx, it->second[i]);
                }
                it++;
            }
            auto closure = new MessagePairClosure<DeleteIndexRequest, DeleteIndexResponse>;
            DeleteIndexRequest& dreq = closure->req;
            dreq.set_index_name(conf.name());
            DeleteIndexResponse& dres = closure->res;
            applyRPC(&dreq, OP_DELETE_INDEX, &dres, closure);
        }
        return create_index_fail ? -1 : 0;
    }

    WorkNode* Master::get_node(const std::string& name)
    {
        auto found = _all_nodes.find(name);
        if (found == _all_nodes.end())
        {
            return NULL;
        }
        return found->second;
    }

    bool Master::get_index_conf(const std::string& name, IndexConf& conf)
    {
        std::lock_guard<bthread::Mutex> guard(_index_mutex);
        auto found = _data_index_confs.find(name);
        if (found == _data_index_confs.end())
        {
            return false;
        }
        conf.CopyFrom(*(found->second));
        return true;
    }

    int Master::handle_create_index(const ::elasticfaiss::CreateIndexRequest* request,
            ::elasticfaiss::CreateIndexResponse* response)
    {
        const std::string& index_name = request->conf().name();
        const std::string& key = index_name;
        {
            std::lock_guard<bthread::Mutex> guard(_index_mutex);
            IndexConf* conf = _data_index_confs[key];
            if (NULL == conf)
            {
                conf = new IndexConf;
                conf->CopyFrom(request->conf());
                _data_index_confs[key] = conf;
            }
        }
        if (NULL != response)  //leader peer do later work
        {
            int ret = transaction_create_index(request->conf());
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
        const std::string& index_name = request->index_name();
        const std::string& key = index_name;
        int32_t num_of_shards = 0;
        std::vector<ShardNodes> to_delete;
        {
            std::lock_guard<bthread::Mutex> guard(_index_mutex);
            auto found = _data_index_confs.find(key);
            if (found != _data_index_confs.end())
            {
                IndexConf* conf = found->second;
                num_of_shards = conf->number_of_shards();
                for (int32_t i = 0; i < num_of_shards; i++)
                {
                    ShardIndexKey key;
                    key.index = index_name;
                    key.shard_idx = i;
                    ShardNodeTable::iterator found = _shard_nodes.find(key);
                    if (found != _shard_nodes.end())
                    {
                        to_delete.push_back(*found->second);
                    }
                }
                delete conf;
                _data_index_confs.erase(found);
            }
        }
        if (NULL != response)
        {
            response->set_success(true);
            for (auto& snode : to_delete)
            {
                for (auto& node : snode.nodes)
                {
                    rpc_delete_index_shard(index_name, snode.shard_idx, node->peer_id());
                }
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
