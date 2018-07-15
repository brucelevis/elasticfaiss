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

//DEFINE_bool(check_term, true, "Check if the leader changed to another term");
//DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
//DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
//DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
//DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
//DEFINE_string(conf, "", "Initial configuration of the replication group");
//DEFINE_string(data_path, "./data", "Path of data stored on");
//DEFINE_string(home, "./", "Home of running process.");


int main(int argc, char* argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    //google::InitGoogleLogging(argv[0]);

    // Generally you only need one Server.
    brpc::Server server;
    elasticfaiss::Master master;
    elasticfaiss::MasterServiceImpl service(&master);

    // Add your service into RPC rerver
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0)
    {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Atomic is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0)
    {
        LOG(ERROR) << "Fail to start rpc Server";
        return -1;
    }

    // It's ok to start Atomic;
    if (master.start(FLAGS_port) != 0)
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
    server.Stop(0);

    // Wait until all the processing tasks are over.
    master.join();
    server.Join();
    return 0;
}
