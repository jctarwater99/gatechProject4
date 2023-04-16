#pragma once

#include <grpc++/grpc++.h>
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
	//bool sendMapCommand(const WorkerCommand & cmd, WorkerReply & reply);
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

	Status status = stub_->executeCommand(&context, wrk_cmd, &reply);

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

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec mr_spec_;
		MasterState_t mr_state_;

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	mr_state_ = MASTER_STATE_INIT;
	mr_spec_ = mr_spec;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Query all workers about their status
	// Run map on all shards
	// Run reduce on all reduce "objects"

	while (1) {
		switch (mr_state_) {
			case MASTER_STATE_INIT:
			{
				cout << "Current state: " << mr_state_ << endl;

				// Query all workers about their status to know about available workers
				cout << "sending getStatus command to workers" << endl;
				for (Worker w: mr_spec_.workers) {
					WorkerReply reply;
            		WorkerServiceClient clientObj( grpc::CreateChannel( w.ip, grpc::InsecureChannelCredentials()));
					if (clientObj.getWorkerStatus(reply) == true) {
						cout << "Status Reply: " << reply.DebugString() << endl;
					} else {
						cout << "ERROR: getWorkerStatus reply failed for address: " << w.ip << endl;
					}
				}

				// Done with this state, move next
				mr_state_ = MASTER_STATE_MAPPING;
			}
			break;

			case MASTER_STATE_MAPPING:
			{
				cout << "Current state: " << mr_state_ << endl;

				// Run map on all shards


				// Done with this state, move next
				mr_state_ = MASTER_STATE_REDUCING;
			}
			break;

			case MASTER_STATE_REDUCING:
			{
				cout << "Current state: " << mr_state_ << endl;

				// Run reduce on all reduce "objects"

				// Done with this state, move next
				mr_state_ = MASTER_STATE_COMPLETE;
			}
			break;

			case MASTER_STATE_COMPLETE:
			{
				cout << "Current state: " << mr_state_ << endl;

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
	}

	return false;
}
