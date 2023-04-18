#pragma once

#include <grpc++/grpc++.h>
#include<unistd.h>
#include "masterworker.grpc.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"


using namespace std;

using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using namespace masterworker;


typedef enum MasterState {
	MASTER_STATE_INIT = 0,
	MASTER_STATE_MAPPING,
	MASTER_STATE_REDUCING,
	MASTER_STATE_COMPLETE

} MasterState_t;


typedef struct ShardMapState {
	uint32_t seq_num;
	struct FileShard shard;
	struct Worker *worker;
	ShardStatus status;
} ShardMapState_t;


typedef struct AsyncGrpcInfo {
	WorkerReply reply;
	Status status;

	ClientContext context;
	unique_ptr<ClientAsyncResponseReader<WorkerReply> > response_reader;

	ShardMapState_t *sm;
} AsyncGrpcInfo_t;


class WorkerServiceClient {

 public:
	WorkerServiceClient(shared_ptr<Channel>, string w_ip);
	bool getWorkerStatus( WorkerReply & reply);
	bool sendMapCommand(FileShard & shard, WorkerReply & reply);
	bool sendMapCommand( WorkerCommand & cmd, WorkerReply & reply);
	bool sendMapCommandAsync( WorkerCommand & cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq);
	//bool sendReduceCommand(const WorkerCommand & cmd, WorkerReply & reply);
	bool sendStopWorkerCommand(WorkerReply & reply);

	string get_worker_ip() { return worker_ip_; }
	void set_worker_ip( string w_ip) { worker_ip_ = w_ip; }

 private:
  	unique_ptr<WorkerService::Stub> stub_;
	string worker_ip_;

	bool executeCommand( WorkerCommand & wrk_cmd, WorkerReply & reply);
	bool executeCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq);
};



WorkerServiceClient::WorkerServiceClient(shared_ptr<Channel> channel, string w_ip)
  : stub_(WorkerService::NewStub(channel)), worker_ip_(w_ip)
  {}


bool WorkerServiceClient::executeCommand( WorkerCommand & wrk_cmd, WorkerReply & reply)
{
	ClientContext context;

	Status status = stub_->ExecuteCommand(&context, wrk_cmd, &reply);

	if (!status.ok()) {
		std::cout << status.error_code() << ": " << status.error_message()
				  << std::endl;
		return false;
	}

	return true;
}


bool WorkerServiceClient::executeCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq)
{
	info->response_reader = stub_->AsyncExecuteCommand(&info->context, wrk_cmd, &cq);
	info->response_reader->Finish(&info->reply, &info->status, (void*)info);

	return true;
}

bool WorkerServiceClient::getWorkerStatus( WorkerReply & reply)
{
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_STATUS);
	wrk_cmd.mutable_status_cmd()->set_dummy(1);

	return executeCommand(wrk_cmd, reply);
}


bool WorkerServiceClient::sendMapCommand(FileShard & shard, WorkerReply & reply) {
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_MAP);

	MapCommand* map_cmd = wrk_cmd.mutable_map_cmd();
	FileShardInfo* info = map_cmd->mutable_shard_info();
	for (FileSegment seg : shard.segments) {
		FileSegmentInfo* fsi = info->add_segment();
		fsi->set_filename(seg.filename);
		fsi->set_start_line(seg.start_line);
		fsi->set_end_line(seg.end_line);
	}

	ClientContext context;

	Status status = stub_->ExecuteCommand(&context, wrk_cmd, &reply);

	if (!status.ok()) {
		std::cout << status.error_code() << ": " << status.error_message()
				<< std::endl;
		return false;
	}
	shard.mapped = true;
	return true;
}


bool WorkerServiceClient::sendStopWorkerCommand(WorkerReply & reply)
{
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_STOP_WORKER);

	return executeCommand(wrk_cmd, reply);
}


bool WorkerServiceClient::sendMapCommand( WorkerCommand & wrk_cmd, WorkerReply & reply)
{
	return executeCommand(wrk_cmd, reply);
}


bool WorkerServiceClient::sendMapCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq)
{
	return executeCommandAsync(wrk_cmd, info, cq);
}


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec mr_spec_;
		MasterState_t mr_state_;
		vector<FileShard> file_shards_;

		std::vector<ShardMapState_t> shard_map_state_;
		CompletionQueue cq_;

		vector<WorkerServiceClient *> worker_clients_;

		void getWorkersStatus();
		void doShardMapping();
		void mapShard( Worker & w, ShardMapState_t* sm );
		void handleResponse();

		WorkerServiceClient* getWorkerServiceClient(string ip);

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

	mr_state_ = MASTER_STATE_INIT;
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;

	for ( Worker w : mr_spec.workers) {
		WorkerServiceClient *c = new WorkerServiceClient ( grpc::CreateChannel( w.ip, grpc::InsecureChannelCredentials()), w.ip);
		worker_clients_.push_back(c);
	}

	uint32_t s_count = 0;
	for ( FileShard s : file_shards) {
		ShardMapState_t sm;
		sm.seq_num = s_count++;
		sm.shard = s;
		sm.status = SHARD_STATUS_INIT;
		shard_map_state_.push_back(sm);
	}
}


WorkerServiceClient* Master::getWorkerServiceClient(string ip)
{
	WorkerServiceClient* ret_val = nullptr;

	vector<WorkerServiceClient*>::iterator it = worker_clients_.begin();
	for (it; it < worker_clients_.end(); it++) {
		if ((*it)->get_worker_ip() == ip) {
			ret_val = (*it);
			break;
		}
	}

	GPR_ASSERT(ret_val != nullptr);
	return ret_val;
}


void Master::handleResponse()
{
	cout << "handleResponse() ..waiting" << endl;

	void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq_.Next(&got_tag, &ok));
    GPR_ASSERT(ok);

	AsyncGrpcInfo_t* call = static_cast<AsyncGrpcInfo_t*>(got_tag);

	if (call->status.ok()) {
		std::cout << "Received: " << call->reply.DebugString() << std::endl;

		call->sm->status = SHARD_STATUS_PROCESSED;
		call->sm->worker->state = STATE_IDLE;

	} else {
		std::cout << "RPC failed" << std::endl;
		call->sm->status = SHARD_STATUS_FAILED;
		call->sm->worker->state = STATE_FAILED;
	}

	// Once we're complete, deallocate the call object.
	delete call;

}

/*
void Master::emptyQueue(CompletionQueue & cq, vector<AsyncGrpcInfo_t> &rpcRequests) {
	while (cq_size > 0) {
		handleResponse(cq, rpcRequests);
	}
}
*/


void Master::getWorkersStatus()
{
	// Query all workers about their status to know about available workers
	cout << "sending getStatus command to workers" << endl;

	for (Worker & w: mr_spec_.workers) {
		WorkerReply reply;
		WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);
		if (clientObj->getWorkerStatus(reply) == true) {
			cout << "Status Reply: " << reply.DebugString() << endl;
			w.state = reply.status_reply().worker_state();
			w.role = reply.status_reply().worker_role();
		} else {
			cout << "ERROR: getWorkerStatus reply failed for address: " << w.ip << endl;
		}
	}
}


void Master::mapShard( Worker & w, ShardMapState_t* sm )
{
	WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);

	AsyncGrpcInfo_t *info = new AsyncGrpcInfo_t;
	info->sm = sm;

	WorkerCommand cmd;
	cmd.set_cmd_seq_num(1); // TODO: Hardcoded
	cmd.set_cmd_type(CMD_TYPE_MAP);

	MapCommand *map_cmd = cmd.mutable_map_cmd();
	map_cmd->set_output_dir( mr_spec_.output_dir);
	map_cmd->set_n_output_files( mr_spec_.n_output_files);

	FileShardInfo *shard_info = map_cmd->mutable_shard_info();

	for ( FileSegment fs : sm->shard.segments) {
		FileSegmentInfo *fsi = shard_info->add_segment();
		fsi->set_filename( fs.filename);
		fsi->set_start_line( fs.start_line);
		fsi->set_end_line( fs.end_line);
	}

	if (clientObj->sendMapCommandAsync(cmd, info, cq_) == true) {

		//rpcRequests_.push_back(info);

		/*
		cout << "Status Reply: " << reply.DebugString() << endl;
		if (reply.cmd_status() == CMD_STATUS_SUCCESS) {
			sm.status = SHARD_STATUS_PROCESSED;
		}
		*/
	} else {
		cout << "ERROR: getWorkerStatus reply failed for address: " << w.ip << endl;
	}

}


void Master::doShardMapping()
{
	bool allShardsMappingDone = false;

	// Run map on all shards
	while (1) {
		allShardsMappingDone = true;
		for ( ShardMapState_t & sm : shard_map_state_) {
			if (SHARD_STATUS_INIT == sm.status) {
				allShardsMappingDone = false;
				// Map this shard to available worker:
				for (Worker & w: mr_spec_.workers) {
					if (STATE_IDLE == w.state) {
						w.state = STATE_WORKING;
						w.role = ROLE_MAPPER;
						sm.status = SHARD_STATUS_MAPPING_IN_PROGRESS;
						sm.worker = &w;
						mapShard(w, &sm);
					}
				}
			} else if (SHARD_STATUS_MAPPING_IN_PROGRESS == sm.status) {
				allShardsMappingDone = false;
			}
		}

		if (true == allShardsMappingDone) {
			break;	// break while
		} else {
			// TODO: This function isn't good, it puts most of the work on 1 worker somehow
			handleResponse();
		}

		// Fetch Workers status:
		//getWorkersStatus();

	}

}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Query all workers about their status
	// Run map on all shards
	// Run reduce on all reduce "objects"

	while (1) {
		cout << "Current state: " << mr_state_ << endl;

		switch (mr_state_) {
			case MASTER_STATE_INIT:
			{
				getWorkersStatus();

				// Done with this state, move next
				mr_state_ = MASTER_STATE_MAPPING;
			}
			break;

			case MASTER_STATE_MAPPING:
			{
				doShardMapping();

				// Done with this state, move next
				mr_state_ = MASTER_STATE_REDUCING;
			}
			break;

			case MASTER_STATE_REDUCING:
			{

				// Run reduce on all reduce "objects"

				// Done with this state, move next
				mr_state_ = MASTER_STATE_COMPLETE;
			}
			break;

			case MASTER_STATE_COMPLETE:
			{

				// Stop all workers
				cout << "sending Stop-Worker command:" << endl;
				for (Worker w: mr_spec_.workers) {
					WorkerReply reply;
					WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);
					if (clientObj->sendStopWorkerCommand(reply) == true) {
						cout << "Stop-Worker Reply: " << reply.DebugString() << endl;
					} else {
						cout << "ERROR: sendStopWorkerCommand reply failed for address: " << w.ip << endl;
					}
				}

				// Exit state machine
				return true;
			}
			break;

			default:
				break;

		}
		sleep(1);
	}

	return false;
}
