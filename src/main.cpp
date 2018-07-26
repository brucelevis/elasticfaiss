/*
 * main.cpp
 *
 *  Created on: 2018年7月15日
 *      Author: wangqiying
 */
#include <pthread.h>
#include <gflags/gflags.h>              // DEFINE_*
//#include <glog/logging.h>
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "proto/master.pb.h"                 // CounterService
#include "master/master.h"
#include "worker/node_service.h"
#include "worker/shard_service.h"
#include "common/elasticfaiss.h"

//DEFINE_bool(check_term, true, "Check if the leader changed to another term");
//DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
//DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
//DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
//DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_bool(data, true, "Run as data node.");
DEFINE_bool(proxy, true, "Run as proxy node.");
DEFINE_string(master_conf, "", "Initial configuration of the masters");
DEFINE_string(listen, "127.0.0.1:8100", "Listen address of this peer");
DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
//DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_string(home, "./", "Home of running process.");

std::string elasticfaiss::g_home;
std::string elasticfaiss::g_listen;
std::string elasticfaiss::g_masters;
std::string elasticfaiss::g_master_group;
butil::EndPoint elasticfaiss::g_listen_endpoint;
int32_t elasticfaiss::g_election_timeout_ms;
int32_t elasticfaiss::g_node_snapshot_interval;
int32_t elasticfaiss::g_master_snapshot_interval;

int main(int argc, char* argv[])
{
    //elasticfaiss::g_current_peerid = butil::my_ip()
    google::ParseCommandLineFlags(&argc, &argv, true);
    elasticfaiss::g_listen = FLAGS_listen;
    elasticfaiss::g_master_group = "elasicfaiss_master";
    elasticfaiss::g_home = FLAGS_home;
    elasticfaiss::g_election_timeout_ms = FLAGS_election_timeout_ms;
    elasticfaiss::g_masters = FLAGS_master_conf;
    if (0 != butil::str2endpoint(elasticfaiss::g_listen.c_str(), &elasticfaiss::g_listen_endpoint))
    {
        LOG(ERROR) << "Invalid listen address:" << elasticfaiss::g_listen;
        return -1;
    }
    //google::InitGoogleLogging(argv[0]);

    // Generally you only need one Server.
    brpc::Server server;
    elasticfaiss::Master master;
    elasticfaiss::MasterServiceImpl service(&master);

    // Add your service into RPC rerver
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Fail to add master service";
        return -1;
    }
    elasticfaiss::WorkNodeServiceImpl node_service;
    if (FLAGS_data)
    {
        auto start_node_func = [&]()
        {
            if(0 != node_service.init())
            {
                LOG(ERROR) << "Fail to init shards service.";
                return;
            }
            if (server.AddService(&node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            {
                LOG(ERROR) << "Fail to add node service";
                return;
            }
            node_service.Start();
        };
        elasticfaiss::start_bthread_function(start_node_func);
    }
    if (FLAGS_proxy)
    {

    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, elasticfaiss::g_listen_endpoint) != 0)
    {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Atomic is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(elasticfaiss::g_listen_endpoint, NULL) != 0)
    {
        LOG(ERROR) << "Fail to start rpc Server";
        return -1;
    }

    // It's ok to start Atomic;
    if (master.start() != 0)
    {
        LOG(ERROR) << "Fail to start master";
        return -1;
    }

    LOG(INFO) << "Master service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit())
    {
        sleep(1);
    }

    LOG(INFO) << "Master service is going to quit";

    // Stop master before server
    master.shutdown();
    node_service.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    master.join();
    server.Join();
    return 0;
}
