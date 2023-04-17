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



class WorkerServiceClient {

 public:
	WorkerServiceClient(shared_ptr<Channel>);
	bool getWorkerStatus( WorkerReply & reply);
	// bool sendMapCommand(FileShard & shard, WorkerReply & reply);
	bool sendMapCommand(CompletionQueue & cq, struct AsyncGrpcInfo &info);
	//bool sendReduceCommand(const WorkerCommand & cmd, WorkerReply & reply);
	bool sendStopWorkerCommand(WorkerReply & reply);

 private:
  	unique_ptr<WorkerService::Stub> stub_;

	bool executeCommand( WorkerCommand & wrk_cmd, WorkerReply & reply);
};



WorkerServiceClient::WorkerServiceClient(shared_ptr<Channel> channel)
  : stub_(WorkerService::NewStub(channel))
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


bool WorkerServiceClient::getWorkerStatus( WorkerReply & reply)
{
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_STATUS);
	wrk_cmd.mutable_status_cmd()->set_dummy(1);

	return executeCommand(wrk_cmd, reply);
}

struct AsyncGrpcInfo {
	void* tag;
	WorkerReply reply;
	Status status;
	Worker* worker;
	FileShard* shard;
	// unique_ptr<ClientAsyncResponseReader<WorkerReply> > rpc_thing;
};

// bool WorkerServiceClient::sendMapCommand(FileShard & shard, WorkerReply & reply, CompletionQueue & cq, AsyncGrpcInfo info) {
bool WorkerServiceClient::sendMapCommand(CompletionQueue & cq, struct AsyncGrpcInfo &info) {
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_MAP);

	MapCommand* map_cmd = wrk_cmd.mutable_map_cmd();
	FileShardInfo* shard_info = map_cmd->mutable_shard_info();
	for (FileSegment seg : info.shard->segments) {
		FileSegmentInfo* fsi = shard_info->add_segments();
		fsi->set_file_name(seg.filename);
		fsi->set_start_line(seg.start_line);
		fsi->set_end_line(seg.end_line);
	}

	ClientContext context;

	// Status status = stub_->ExecuteCommand(&context, wrk_cmd, &reply);

    // CompletionQueue cq;	
	// Status status;
	unique_ptr<ClientAsyncResponseReader<WorkerReply> > rpc(
		stub_->AsyncExecuteCommand(&context, wrk_cmd, &cq));
	// info.rpc_thing = (
	// 	stub_->AsyncExecuteCommand(&context, wrk_cmd, &cq));
	rpc->Finish(&info.reply, &info.status, info.tag);
	// info.rpc_thing = rpc;





	// void* got_tag;
    // bool ok = false;
    // GPR_ASSERT(cq.Next(&got_tag, &ok));

	// if (!info.status.ok()) {
	// 	std::cout << info.status.error_code() << ": " << info.status.error_message()
	// 			<< std::endl;
	// 	info.shard->state = FAILED;
	// 	// resposne->worker->stater
	// 	// TODO: Handle error
	// } else {
	// 	// TODO: Actually look at the response
	// 	info.shard->state = COMPLETE;
	// 	info.worker->state = STATE_IDLE;
	// }







	return true;
}

bool WorkerServiceClient::sendStopWorkerCommand(WorkerReply & reply)
{
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_STOP_WORKER);

	return executeCommand(wrk_cmd, reply);
}


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();
		void handleResponse(CompletionQueue & cq, vector<struct AsyncGrpcInfo> &rpcRequests);
		void emptyQueue(CompletionQueue & cq, vector<struct AsyncGrpcInfo> &rpcRequests);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec mr_spec_;
		MasterState_t mr_state_;
		vector<FileShard> file_shards_;
		int cq_size;
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	mr_state_ = MASTER_STATE_INIT;
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;
	cq_size = 0;
}

void Master::handleResponse(CompletionQueue & cq, vector<struct AsyncGrpcInfo> &rpcRequests) {
	void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq.Next(&got_tag, &ok));
    // GPR_ASSERT(ok);

	vector<AsyncGrpcInfo>::iterator response = rpcRequests.begin();
	for (response; response < rpcRequests.end(); response++) {
		if (got_tag != (response->tag)) {
			continue;
		}
		if (!response->status.ok()) {
			std::cout << response->status.error_code() << ": " << response->status.error_message()
					<< std::endl;
			response->shard->state = FAILED;
			// resposne->worker->stater
			// TODO: Handle error
		} else {
			// TODO: Actually look at the response
			response->shard->state = COMPLETE;
			response->worker->state = STATE_IDLE;
		}
		break;
	}
	cq_size--;
	rpcRequests.erase(response);
}

void Master::emptyQueue(CompletionQueue & cq, vector<struct AsyncGrpcInfo> &rpcRequests) {
	while (cq_size > 0) {
		handleResponse(cq, rpcRequests);
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Query all workers about their status
	// Run map on all shards
	// Run reduce on all reduce "objects"
	CompletionQueue cq;	
	vector<struct AsyncGrpcInfo> rpcRequests;
	void* globalTag = (void*)1;

	while (1) {
		cout << "Current state: " << mr_state_ << endl;

		switch (mr_state_) {
			case MASTER_STATE_INIT:
			{
				// Query all workers about their status to know about available workers
				cout << "sending getStatus command to workers" << endl;
				// for (Worker w: mr_spec_.workers) {
				vector<Worker>::iterator w = mr_spec_.workers.begin();
				for(w; w < mr_spec_.workers.end(); w++) {
					WorkerReply reply;
            		WorkerServiceClient clientObj( grpc::CreateChannel( w->ip, grpc::InsecureChannelCredentials()));
					if (clientObj.getWorkerStatus(reply) == true) {
						cout << "Status Reply: " << reply.DebugString() << endl;
						w->state = reply.status_reply().worker_state(); 
					} else {
						w->state = STATE_FAILED; 
						cout << "ERROR: getWorkerStatus reply failed for address: " << w->ip << endl;
					}
				}

				// Done with this state, move next
				mr_state_ = MASTER_STATE_MAPPING;
			}
			break;

			case MASTER_STATE_MAPPING:
			{
				// Run map on all shards
				bool all_mapped = true;
				
				vector<FileShard>::iterator shard = file_shards_.begin();
				for (shard; shard < file_shards_.end(); shard++) {
					if (shard->state == COMPLETE) {
						cout << "Mapped\n";
						continue;
					}
					all_mapped = false;

					vector<Worker>::iterator w = mr_spec_.workers.begin();
					if (shard->state == UNTOUCHED) {
						bool all_idle = true;
						for(w; w < mr_spec_.workers.end(); w++) {
							if (w->state == STATE_IDLE) {
								all_idle = false;
								cout << "At least one idle" << endl;
								w->state = STATE_WORKING;

								struct AsyncGrpcInfo info;
								WorkerServiceClient clientObj( grpc::CreateChannel( w->ip, grpc::InsecureChannelCredentials()));

								info.worker	= &(*w);
								info.shard = &(*shard);
								info.tag = globalTag;
								info.shard->state = IN_PROGRESS;
								globalTag = static_cast<char*>(globalTag) + 1;
								clientObj.sendMapCommand(cq, info);
								cout << "Greate, one part down, only 1000 to go" << endl;
								rpcRequests.push_back(info);
								cq_size++;
								break;
							} 
						}
						// Queue must be full here
						if (all_idle) {
							cout << "Reached the place" << endl;
							handleResponse(cq, rpcRequests);
							cout << "And left it" << endl; 
						}
					} else {
						// All requests must be sent out here
						emptyQueue(cq, rpcRequests);
					}
				}
				if (all_mapped) {
					// Done with this state, move next
					mr_state_ = MASTER_STATE_REDUCING;
				}
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
            		WorkerServiceClient clientObj( grpc::CreateChannel( w.ip, grpc::InsecureChannelCredentials()));
					if (clientObj.sendStopWorkerCommand(reply) == true) {
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
