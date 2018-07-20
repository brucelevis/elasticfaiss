/*
 * node_service.h
 *
 *  Created on: 2018年7月15日
 *      Author: wangqiying
 */

#ifndef SRC_WORKER_NODE_SERVICE_H_
#define SRC_WORKER_NODE_SERVICE_H_

#include <butil/threading/simple_thread.h>
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "proto/work_node.pb.h"                 // CounterService

namespace elasticfaiss
{
    class WorkNodeServiceImpl: public WorkNodeService, public butil::SimpleThread
    {
        private:
            butil::WaitableEvent _bg_event;
            std::atomic<bool> _running;
            bool _booted;
            void Run();
            void report_heartbeat();
        public:
            explicit WorkNodeServiceImpl()
                    : butil::SimpleThread("node_background"), _bg_event(false, false), _running(true), _booted(false)
            {
            }
            void shutdown();
            void create_shard(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::CreateShardRequest* request, ::elasticfaiss::CreateShardResponse* response,
                    ::google::protobuf::Closure* done);
            void delete_shard(::google::protobuf::RpcController* controller,
                    const ::elasticfaiss::DeleteShardRequest* request, ::elasticfaiss::DeleteShardResponse* response,
                    ::google::protobuf::Closure* done);
    };
}

#endif /* SRC_WORKER_NODE_SERVICE_H_ */
