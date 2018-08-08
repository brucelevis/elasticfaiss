/*
 * shard_service.cpp
 *
 *  Created on: 2018年7月19日
 *      Author: qiyingwang
 */
#include "shard_service.h"
#include "local_db.h"
#include <braft/protobuf_file.h>

DEFINE_int32(shard_snapshot_interval, 30, "Interval between each snapshot");

namespace elasticfaiss
{
    ShardManager g_shards;

    std::string ShardIndexKey::to_string() const
    {
        return shard_cluster_name(index, shard_idx);
    }

    std::string shard_cluster_name(const std::string& index, int32_t shard_idx)
    {
        std::stringstream cluster_group;
        cluster_group << index << "_" << shard_idx;
        return cluster_group.str();
    }
    void ShardNode::remove_db()
    {
        if(NULL != _db)
        {
            g_local_db.drop_shard_db(_db);
        }
    }
    int ShardNode::start(const IndexShardConf& req)
    {
        braft::NodeOptions node_options;
        std::string nodes_str = string_join_container(req.nodes(), ",");
        //node_options.initial_conf.add_peer(req.nodes_size())
        if (node_options.initial_conf.parse_from(nodes_str) != 0)
        {
            LOG(ERROR) << "Fail to parse configuration '" << nodes_str << "'";
            return -1;
        }
        _init_conf = req;
        ShardIndexKey sk;
        sk.index = req.conf().name();
        sk.shard_idx = req.shard_idx();
        _db = g_local_db.get_shard_db(sk, true);
        if(NULL == _db.get())
        {
            return -1;
        }

        std::string index_group = sk.to_string();
        node_options.election_timeout_ms = g_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_shard_snapshot_interval;
        std::string prefix = "local://" + g_home + "/shards/" + index_group;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        //node_options.disable_cli = FLAGS_disable_cli;

        braft::Node* node = new braft::Node(index_group, braft::PeerId(g_listen_endpoint));
        if (node->init(node_options) != 0)
        {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        _state = SHARD_ACTIVE;
        return 0;
    }
    bool ShardNode::list_peers(std::vector<std::string>& ids)
    {
        std::vector<braft::PeerId> peers;
        butil::Status st = _node->list_peers(&peers);
        if(st.ok())
        {
            for(const braft::PeerId& peer:peers)
            {
                ids.push_back( butil::endpoint2str(peer.addr).c_str());
            }
        }
        return st.ok();
    }

    void ShardNode::on_apply(braft::Iterator& iter)
    {

    }
    void ShardNode::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done)
    {

    }
    int ShardNode::on_snapshot_load(braft::SnapshotReader* reader)
    {
        return 0;
    }
    int ShardManager::load_shards(const BootstrapResponse& conf)
    {
        for (int i = 0; i < conf.shards_size(); i++)
        {
            create_shard(conf.shards(i));
        }
        return 0;
    }
    int ShardManager::init(const BootstrapResponse& conf)
    {
        _init_conf_path = g_home + "/shards/shards.pb";
        return load_shards(conf);
    }

    void ShardManager::fill_heartbeat_request(NodeHeartbeatRequest& req)
    {
        std::lock_guard<bthread::Mutex> guard(_shards_mutex);
        auto it = _shards.begin();
        while (it != _shards.end())
        {
            ShardNode* node = it->second;
            Shard* shard = req.add_shards();
            shard->set_name(it->first.index);
            shard->set_idx(node->get_init_conf().shard_idx());
            shard->set_is_leader(node->is_leader());
            shard->set_state(node->get_state());
            std::vector<std::string> ids;
            if(node->is_leader() && node->list_peers(ids))
            {
                for(auto& id:ids)
                {
                    shard->add_nodes()->assign(id);
                }
            }
            it++;
        }
    }

    int ShardManager::create_shard(const IndexShardConf& req)
    {
        std::lock_guard<bthread::Mutex> guard(_shards_mutex);
        ShardIndexKey id(req.conf().name(), req.shard_idx());
        ShardNodeTable::iterator found = _shards.find(id);
        if (found != _shards.end())
        {
            LOG(ERROR) << "Duplicate index with name:" << req.conf().name() << " & idx:" << req.shard_idx();
            return -1;
        }
        ShardNode* node = new ShardNode;
        if (0 != node->start(req))
        {
            node->shutdown();
            delete node;
            return -1;
        }
        _shards.insert(ShardNodeTable::value_type(id, node));
        return 0;
    }

    int ShardManager::remove_shard(const DeleteShardRequest& req)
    {
        std::lock_guard<bthread::Mutex> guard(_shards_mutex);
        ShardIndexKey id(req.name(), req.idx());
        ShardNodeTable::iterator found = _shards.find(id);
        if (found == _shards.end())
        {
            LOG(ERROR) << "No index with name:" << req.name() << " & idx:" << req.idx();
            return -1;
        }
        ShardNode* node = found->second;
        node->shutdown();
        node->remove_db();
        delete node;
        _shards.erase(found);
        return 0;
    }
}

