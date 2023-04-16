#pragma once

#include <grpc++/grpc++.h>
#include<unistd.h>
#include "masterworker.grpc.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"


using namespace std;

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
	bool sendMapCommand(FileShard & shard, WorkerReply & reply);
	//bool sendReduceCommand(const WorkerCommand & cmd, WorkerReply & reply);
	//bool sendStopWorkerCommand(const WorkerCommand & cmd, WorkerReply & reply);

 private:
  	unique_ptr<WorkerService::Stub> stub_;
};



WorkerServiceClient::WorkerServiceClient(shared_ptr<Channel> channel)
  : stub_(WorkerService::NewStub(channel))
  {}

bool WorkerServiceClient::getWorkerStatus( WorkerReply & reply)
{
  WorkerCommand wrk_cmd;
  wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
  wrk_cmd.set_cmd_type(CMD_TYPE_STATUS);
  wrk_cmd.mutable_status_cmd()->set_dummy(1);

  ClientContext context;

  Status status = stub_->executeCommand(&context, wrk_cmd, &reply);

  if (!status.ok()) {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
    return false;
  }

  return true;
}


bool WorkerServiceClient::sendMapCommand(FileShard & shard, WorkerReply & reply) {
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_MAP);

	MapCommand* map_cmd = wrk_cmd.mutable_map_cmd();
	FileShardInfo* info = map_cmd->mutable_shard_info();
	for (FileSegment seg : shard.segments) {
		FileSegmentInfo* fsi = info->add_segments();
		fsi->set_file_name(seg.filename);
		fsi->set_start_line(seg.start_line);
		fsi->set_end_line(seg.end_line);
	}

	ClientContext context;

	Status status = stub_->executeCommand(&context, wrk_cmd, &reply);

	if (!status.ok()) {
		std::cout << status.error_code() << ": " << status.error_message()
				<< std::endl;
		return false;
	}
	shard.mapped = true;
	return true;
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
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	mr_state_ = MASTER_STATE_INIT;
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;
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
				// for (FileShard shard : file_shards_) {
				vector<FileShard>::iterator shard = file_shards_.begin();
				for (shard; shard < file_shards_.end(); shard++) {
					if (shard->mapped) {
						cout << "Mapped\n";
						continue;
					}
					all_mapped = false;
					// for (Worker w: mr_spec_.workers) {

					vector<Worker>::iterator w = mr_spec_.workers.begin();
					for(w; w < mr_spec_.workers.end(); w++) {
						if (w->state == STATE_IDLE) {
							cout << "At least one idle" << endl;
							w->state = STATE_WORKING;

							WorkerReply reply;
		            		WorkerServiceClient clientObj( grpc::CreateChannel( w->ip, grpc::InsecureChannelCredentials()));
							if (clientObj.sendMapCommand(*shard, reply) == true) {
								w->state = STATE_IDLE;
								cout << "Greate, one part down, only 1000 to go" << endl;
							} else {
								cout << "That went about how I expected it would" << endl;
							}
							break;
						} else { 
							cout << "ni" << w->state;
						}
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
