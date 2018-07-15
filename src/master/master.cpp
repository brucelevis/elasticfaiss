#include <pthread.h>
#include <gflags/gflags.h>              // DEFINE_*
//#include <glog/logging.h>
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "master.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000,
		"Start election in such milliseconds if disconnect with the leader");
//DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(home, "./", "Home of running process.");

namespace elasticfaiss {
class Master;
// Define types for different operation
enum MasterOpType {
	OP_UNKNOWN = 0, OP_BOOTSTRAP = 1, OP_HEARTBEAT = 2,
};

// Implements Closure which encloses RPC stuff
class MasterOpClosure: public braft::Closure {
public:
	MasterOpClosure(Master* m, const ::google::protobuf::Message* request,
			::google::protobuf::Message* response,
			google::protobuf::Closure* done) :
			_master(m), _request(request), _response(response), _done(done) {
	}
	~MasterOpClosure() {
	}

	const ::google::protobuf::Message* request() const {
		return _request;
	}
	::google::protobuf::Message* response() const {
		return _response;
	}
	void Run();

private:
	Master* _master;
	const ::google::protobuf::Message* _request;
	::google::protobuf::Message* _response;
	google::protobuf::Closure* _done;
};

// Starts this node
int Master::start(int32_t port) {
	butil::EndPoint addr(butil::my_ip(), port);
	braft::NodeOptions node_options;
	if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
		LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
		return -1;
	}
	node_options.election_timeout_ms = FLAGS_election_timeout_ms;
	node_options.fsm = this;
	node_options.node_owns_fsm = false;
	node_options.snapshot_interval_s = FLAGS_snapshot_interval;
	std::string prefix = "local://" + FLAGS_home + "/" + FLAGS_data_path
			+ "/master";
	node_options.log_uri = prefix + "/log";
	node_options.raft_meta_uri = prefix + "/raft_meta";
	node_options.snapshot_uri = prefix + "/snapshot";
	node_options.disable_cli = FLAGS_disable_cli;
	std::string group = "master";
	braft::Node* node = new braft::Node(group, braft::PeerId(addr));
	if (node->init(node_options) != 0) {
		LOG(ERROR) << "Fail to init raft node";
		delete node;
		return -1;
	}
	_node = node;
	return 0;
}

// Impelements Service methods
void Master::bootstrap(::google::protobuf::RpcController* controller,
		const ::elasticfaiss::BootstrapRequest* request,
		::elasticfaiss::BootstrapResponse* response,
		::google::protobuf::Closure* done) {
	brpc::ClosureGuard done_guard(done);
	// Serialize request to the replicated write-ahead-log so that all the
	// peers in the group receive this request as well.
	// Notice that _value can't be modified in this routine otherwise it
	// will be inconsistent with others in this group.

	// Serialize request to IOBuf
	const int64_t term = _leader_term.load(butil::memory_order_relaxed);
	if (term < 0) {
		return redirect(response);
	}
	butil::IOBuf log;
	log.push_back((int8_t) OP_BOOTSTRAP);
	butil::IOBufAsZeroCopyOutputStream wrapper(&log);
	if (!request->SerializeToZeroCopyStream(&wrapper)) {
		LOG(ERROR) << "Fail to serialize request";
		response->set_success(false);
		return;
	}
	// Apply this log as a braft::Task
	braft::Task task;
	task.data = &log;
	// This callback would be iovoked when the task actually excuted or
	// fail
	task.done = new MasterOpClosure(this, request, response,
			done_guard.release());
	if (FLAGS_check_term) {
		// ABA problem can be avoid if expected_term is set
		task.expected_term = term;
	}
	// Now the task is applied to the group, waiting for the result.
	return _node->apply(task);
}

bool Master::is_leader() const {
	return _leader_term.load(butil::memory_order_acquire) > 0;
}

// Shut this node down.
void Master::shutdown() {
	if (_node) {
		_node->shutdown(NULL);
	}
}

// Blocking this thread until the node is eventually down.
void Master::join() {
	if (_node) {
		_node->join();
	}
}

void Master::redirect(::google::protobuf::Message* response) {
//                response->set_success(false);
//                if (_node)
//                {
//                    braft::PeerId leader = _node->leader_id();
//                    if (!leader.is_empty())
//                    {
//                        response->set_redirect(leader.to_string());
//                    }
//                }
}

// @braft::StateMachine
void Master::on_apply(braft::Iterator& iter) {
	// A batch of tasks are committed, which must be processed through
	// |iter|
	for (; iter.valid(); iter.next()) {
		butil::IOBuf data = iter.data();
		uint8_t type = OP_UNKNOWN;
		data.cutn(&type, sizeof(uint8_t));
		// This guard helps invoke iter.done()->Run() asynchronously to
		// avoid that callback blocks the StateMachine.
		braft::AsyncClosureGuard closure_guard(iter.done());
		if (iter.done()) {
			// This task is applied by this node, get value from this
			// closure to avoid additional parsing.
//                        FetchAddClosure* c = dynamic_cast<FetchAddClosure*>(iter.done());
//                        response = c->response();
//                        detal_value = c->request()->value();
		} else {
			// Have to parse FetchAddRequest from this log.
//                        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
//                        FetchAddRequest request;
//                        CHECK(request.ParseFromZeroCopyStream(&wrapper));
//                        detal_value = request.value();
		}

		// Now the log has been parsed. Update this state machine by this
		// operation.
//                    const int64_t prev = _value.fetch_add(detal_value, butil::memory_order_relaxed);
//                    if (response)
//                    {
//                        response->set_success(true);
//                        response->set_value(prev);
//                    }

		// The purpose of following logs is to help you understand the way
		// this StateMachine works.
		// Remove these logs in performance-sensitive servers.
//                    LOG_IF(INFO, FLAGS_log_applied_task) << "Added value=" << prev << " by detal=" << detal_value
//                                                                 << " at log_index=" << iter.index();
	}
}

struct SnapshotArg {
	int64_t value;
	braft::SnapshotWriter* writer;
	braft::Closure* done;
};

static void *save_snapshot(void* arg) {
	SnapshotArg* sa = (SnapshotArg*) arg;
	std::unique_ptr<SnapshotArg> arg_guard(sa);
	// Serialize StateMachine to the snapshot
	brpc::ClosureGuard done_guard(sa->done);
	std::string snapshot_path = sa->writer->get_path() + "/data";
	LOG(INFO) << "Saving snapshot to " << snapshot_path;
	// Use protobuf to store the snapshot for backward compatibility.
//                Snapshot s;
//                s.set_value(sa->value);
	braft::ProtoBufFile pb_file(snapshot_path);
//                if (pb_file.save(&s, true) != 0)
//                {
//                    sa->done->status().set_error(EIO, "Fail to save pb_file");
//                    return NULL;
//                }
	// Snapshot is a set of files in raft. Add the only file into the
	// writer here.
	if (sa->writer->add_file("data") != 0) {
		sa->done->status().set_error(EIO, "Fail to add file to writer");
		return NULL;
	}
	return NULL;
}

void Master::on_snapshot_save(braft::SnapshotWriter* writer,
		braft::Closure* done) {
	// Save current StateMachine in memory and starts a new bthread to avoid
	// blocking StateMachine since it's a bit slow to write data to disk
	// file.
	SnapshotArg* arg = new SnapshotArg;
	//arg->value = _value.load(butil::memory_order_relaxed);
	arg->writer = writer;
	arg->done = done;
	bthread_t tid;
	bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}

int Master::on_snapshot_load(braft::SnapshotReader* reader) {
	// Load snasphot from reader, replacing the running StateMachine
	CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
	if (reader->get_file_meta("data", NULL) != 0) {
		LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
		return -1;
	}
	std::string snapshot_path = reader->get_path() + "/data";
	braft::ProtoBufFile pb_file(snapshot_path);
//                Snapshot s;
//                if (pb_file.load(&s) != 0)
//                {
//                    LOG(ERROR) << "Fail to load snapshot from " << snapshot_path;
//                    return -1;
//                }
//                _value.store(s.value(), butil::memory_order_relaxed);
	return 0;
}

void Master::on_leader_start(int64_t term) {
	_leader_term.store(term, butil::memory_order_release);
	LOG(INFO) << "Node becomes leader";
}
void Master::on_leader_stop(const butil::Status& status) {
	_leader_term.store(-1, butil::memory_order_release);
	LOG(INFO) << "Node stepped down : " << status;
}

void Master::on_shutdown() {
	LOG(INFO) << "This node is down";
}
void Master::on_error(const ::braft::Error& e) {
	LOG(ERROR) << "Met raft error " << e;
}
void Master::on_configuration_committed(const ::braft::Configuration& conf) {
	LOG(INFO) << "Configuration of this group is " << conf;
}
void Master::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
	LOG(INFO) << "Node stops following " << ctx;
}
void Master::on_start_following(const ::braft::LeaderChangeContext& ctx) {
	LOG(INFO) << "Node start following " << ctx;
}

void MasterOpClosure::Run() {
	// Auto delete this after Run()
	std::unique_ptr<MasterOpClosure> self_guard(this);
	// Repsond this RPC.
	brpc::ClosureGuard done_guard(_done);
	if (status().ok()) {
		return;
	}
	// Try redirect if this request failed.
	_master->redirect(_response);
}



}  // namespace example
