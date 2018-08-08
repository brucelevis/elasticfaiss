/*
 * node_service.cpp
 *
 *  Created on: 2018年7月18日
 *      Author: qiyingwang
 */
#include "node_service.h"
#include "common/elasticfaiss.h"
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include <braft/util.h>
#include <braft/route_table.h>
#include "proto/master.pb.h"
#include "shard_service.h"
#include "local_db.h"

namespace elasticfaiss
{
    void WorkNodeServiceImpl::Run()
    {
        braft::rtb::update_configuration(g_master_group, g_masters);
        butil::TimeDelta retry_init_time = butil::TimeDelta::FromMilliseconds(1500);
        _running = true;
        while(0 != init())
        {
            if (!_running)
            {
                return;
            }
            _bg_event.TimedWait(retry_init_time);
        }
        _inited = true;
        butil::TimeDelta wtime = butil::TimeDelta::FromMilliseconds(3000);
        while (_running)
        {
            if (!_running)
            {
                break;
            }
            report_heartbeat();
            _bg_event.TimedWait(wtime);
        }
    }

    template<typename T>
    static bool handle_master_err_res(brpc::Controller& cntl, braft::PeerId& leader, const T& response)
    {
        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to send request with response type:" << response.GetTypeName() << " to " << leader << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(g_master_group, braft::PeerId());
            return true;
        }
        if (!response.success())
        {
            LOG(ERROR) << "Fail to send request with response type:" << response.GetTypeName() << " to " << leader << ", redirecting to "
                    << (response.has_redirect() ? response.redirect() : "nowhere");
            // Update route table since we have redirect information
            return true;;
        }
        return false;
    }

    int WorkNodeServiceImpl::init()
    {
        if(!g_local_db.is_inited() && 0 != g_local_db.init())
        {
            return -1;
        }
        if(0 != report_bootstrap())
        {
            LOG(ERROR) << "Fail to report boot to master.";
            return -1;
        }
        return g_shards.init(_boot_res);
    }

    int WorkNodeServiceImpl::report_bootstrap()
    {
        braft::PeerId leader;
        braft::Configuration master_conf;
        master_conf.parse_from(g_masters);
        braft::rtb::update_configuration(g_master_group, master_conf);
        if (braft::rtb::select_leader(g_master_group, &leader) != 0)
        {
            butil::Status st = braft::rtb::refresh_leader(g_master_group, 2000);
            if (!st.ok())
            {
                LOG(ERROR) << "Fail to refresh_leader : " << st;
            }
            return -1;
        }
        LOG(INFO) << "Master leader is " << leader;
        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0)
        {
            LOG(ERROR) << "Fail to init channel to " << leader;
            return -1;
        }
        elasticfaiss::MasterService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        ::elasticfaiss::BootstrapRequest request;
        request.set_node_peer(g_listen);
        request.set_boot_ms(butil::gettimeofday_ms());
        stub.bootstrap(&cntl, &request, &_boot_res, NULL);
        if (handle_master_err_res(cntl, leader, _boot_res))
        {
            return -1;
        }
        return 0;
    }

    void WorkNodeServiceImpl::report_heartbeat()
    {
        //LOG(ERROR) << "report heatbeat";
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(g_master_group, &leader) != 0)
        {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(g_master_group, 2000);
            if (!st.ok())
            {
                // Not sure about the leader, sleep for a while and the ask again.
                LOG(ERROR) << "Fail to refresh_leader : " << st;
            }
            return;
        }

        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0)
        {
            LOG(ERROR) << "Fail to init channel to " << leader;
            return;
        }
        elasticfaiss::MasterService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        ::elasticfaiss::NodeHeartbeatRequest request;
        request.set_node_peer(g_listen);
        request.set_active_ms(butil::gettimeofday_ms());
        g_shards.fill_heartbeat_request(request);
        ::elasticfaiss::NodeHeartbeatResponse response;
        stub.node_heartbeat(&cntl, &request, &response, NULL);
        handle_master_err_res(cntl, leader, response);
    }
    void WorkNodeServiceImpl::shutdown()
    {
        if (_running)
        {
            _running = false;
            _bg_event.Signal();
            Join();
        }
    }
    void WorkNodeServiceImpl::rebuild(::google::protobuf::RpcController* controller,
            const ::elasticfaiss::ShardRebuildRequest* request, ::elasticfaiss::ShardRebuildResponse* response,
            ::google::protobuf::Closure* done)
    {

    }
    void WorkNodeServiceImpl::put(::google::protobuf::RpcController* controller, const ::elasticfaiss::ShardPutRequest* request,
            ::elasticfaiss::ShardPutResponse* response, ::google::protobuf::Closure* done)
    {

    }
    void WorkNodeServiceImpl::create_shard(::google::protobuf::RpcController* controller,
            const ::elasticfaiss::CreateShardRequest* request, ::elasticfaiss::CreateShardResponse* response,
            ::google::protobuf::Closure* done)
    {
        brpc::ClosureGuard done_guard(done);
        if (_inited && 0 != g_shards.create_shard(request->conf()))
        {
            response->set_success(false);
            LOG(ERROR) << "Fail to create shard:" << request->conf().conf().name() << "_" << request->conf().shard_idx();
        }
        else
        {
            response->set_success(true);
            LOG(INFO) << "Success to create shard:" << request->conf().conf().name() << "_" << request->conf().shard_idx();
            //report_heartbeat();
        }
    }
    void WorkNodeServiceImpl::delete_shard(::google::protobuf::RpcController* controller,
            const ::elasticfaiss::DeleteShardRequest* request, ::elasticfaiss::DeleteShardResponse* response,
            ::google::protobuf::Closure* done)
    {
        brpc::ClosureGuard done_guard(done);
        if (_inited && 0 != g_shards.remove_shard(*request))
        {
            response->set_success(false);
        }
        else
        {
            //report_heartbeat();
            response->set_success(true);
        }
    }
}

