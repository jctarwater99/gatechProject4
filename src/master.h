#pragma once

#include <chrono>
#include <grpc++/grpc++.h>
#include<unistd.h>
#include "masterworker.grpc.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"


using namespace std;
using namespace std::chrono;
using namespace std::literals;

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

typedef enum ReduceJobState {
	REDUCE_JOB_INIT = 0,
	REDUCE_JOB_IN_PROGRESS,
	REDUCE_JOB_COMPLETE,
	REDUCE_JOB_FAILED
} ReduceJobState;

typedef struct AsyncGrpcInfo {
	WorkerReply reply;
	Status status;

	ClientContext context;
	unique_ptr<ClientAsyncResponseReader<WorkerReply> > response_reader;

	struct Worker *worker;
	struct FileShard *shard;
	ReduceJobState *reduce_job;

} AsyncGrpcInfo_t;


class WorkerServiceClient {

 public:

	WorkerServiceClient(shared_ptr<Channel>, string w_ip);
	bool getWorkerStatus( WorkerReply & reply);
	bool sendCommandAsync( WorkerCommand & cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq);
	bool sendStopWorkerCommand(WorkerReply & reply);

	string get_worker_ip() { return worker_ip_; }
	void set_worker_ip( string w_ip) { worker_ip_ = w_ip; }

 private:
  	unique_ptr<WorkerService::Stub> stub_;
	string worker_ip_;

	bool executeCommand( WorkerCommand & wrk_cmd, WorkerReply & reply);
	bool executeCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq);

	void setMessageTimeDeadline( ClientContext & context );
};



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
		vector<ReduceJobState> reduce_jobs_complete_;

		//std::vector<ShardMapState_t> shard_map_state_;
		CompletionQueue cq_;

		vector<WorkerServiceClient *> worker_clients_;

		void getWorkersStatus();
		void doShardMapping();
		void doReducing();
		void mapShard( FileShard & s, Worker & w );
		// void runReduceJob( ReduceJobState & s, Worker & w );
		void runReduceJob( ReduceJobState & s, Worker & w, int reducer_num );
		void handleMapResponse();
		void handleReduceResponse();

		WorkerServiceClient* getWorkerServiceClient(string ip);


};

