/*
 * shard_service.cpp
 *
 *  Created on: 2018年7月19日
 *      Author: qiyingwang
 */
#include "shard_service.h"
#include <braft/protobuf_file.h>

DEFINE_int32(shard_snapshot_interval, 30, "Interval between each snapshot");

namespace elasticfaiss
{
    ShardManager g_shards;

    std::string cluster_group_name(const std::string& cluster, const std::string& index, int32_t shard_idx)
    {
        std::stringstream cluster_group;
        cluster_group << cluster << "_" << index << shard_idx;
        return cluster_group.str();
    }
    int ShardNode::start(const CreateShardRequest& req)
    {
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(req.shard_nodes()) != 0)
        {
            LOG(ERROR) << "Fail to parse configuration `" << req.shard_nodes() << '\'';
            return -1;
        }
        _init_conf = req;
        std::string index_group = cluster_group_name(req.cluster(), req.conf().name(), req.idx());
        node_options.election_timeout_ms = g_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_shard_snapshot_interval;
        std::string prefix = "local://" + g_home + "/shards/" + req.cluster() + "/" + index_group;
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
        return 0;
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
    int ShardManager::sync_shards_info()
    {
        _init_conf.Clear();
        auto it = _shards.begin();
        while (it != _shards.end())
        {
            ShardNode* node = it->second;
            const CreateShardRequest& conf = node->get_init_conf();
            _init_conf.add_shards()->CopyFrom(conf);
            it++;
        }
        braft::ProtoBufFile pb_file(_init_conf_path);
        if (pb_file.save(&_init_conf, true) != 0)
        {
            LOG(ERROR) << "Fail to save shards info pb_file";
            return -1;
        }
        return 0;
    }
    int ShardManager::load_shards()
    {
        braft::ProtoBufFile pb_file(_init_conf_path);
        if (pb_file.load(&_init_conf) != 0)
        {
            return 0;
        }
        for (int i = 0; i < _init_conf.shards_size(); i++)
        {
            create_shard(_init_conf.shards(i));
        }
        return 0;
    }
    int ShardManager::init()
    {
        _init_conf_path = g_home + "/shards/shards.pb";
        return load_shards();
    }

    void ShardManager::fill_heartbeat_request(NodeHeartbeatRequest& req)
    {
        std::lock_guard<std::mutex> guard(_shards_mutex);
        auto it = _shards.begin();
        while (it != _shards.end())
        {
            ShardNode* node = it->second;
            Shard* shard = req.add_shards();
            shard->set_name(node->get_init_conf().conf().name());
            shard->set_idx(node->get_init_conf().idx());
            shard->set_is_leader(node->is_leader());
            shard->set_state(node->get_state());
            it++;
        }
    }

    int ShardManager::create_shard(const CreateShardRequest& req)
    {
        std::lock_guard<std::mutex> guard(_shards_mutex);
        ShardNodeId id(req.conf().name(), req.idx());
        ShardNodeTable::iterator found = _shards.find(id);
        if (found != _shards.end())
        {
            LOG(ERROR) << "Duplicate index with name:" << req.conf().name() << " & idx:" << req.idx();
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
        sync_shards_info();
        return 0;
    }

    int ShardManager::remove_shard(const DeleteShardRequest& req)
    {
        std::lock_guard<std::mutex> guard(_shards_mutex);
        ShardNodeId id(req.name(), req.idx());
        ShardNodeTable::iterator found = _shards.find(id);
        if (found == _shards.end())
        {
            LOG(ERROR) << "No index with name:" << req.name() << " & idx:" << req.idx();
            return -1;
        }
        ShardNode* node = found->second;
        node->shutdown();
        delete node;
        _shards.erase(found);
        sync_shards_info();
        return 0;
    }
}
