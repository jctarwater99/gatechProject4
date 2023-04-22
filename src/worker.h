#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include<unistd.h>
#include<fstream>
#include<sstream>

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include "masterworker.grpc.pb.h"

#include "threadpool.h"
#include "circular_buffer.h"


// #include <filesystem>
// namespace fs = std::filesystem;

using namespace std;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using namespace masterworker;


class Worker;

// Class encompasing the state and logic needed to serve a request.
class CallData {

    public:

        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallData( Worker & wrkServer, WorkerService::AsyncService* service, ServerCompletionQueue* cq);

        void proceed();

        ServerContext* getServerContext() { return &ctx_; }
        WorkerCommand* getWorkerCommand() { return &request_; }
        WorkerReply* getWorkerReply() { return &reply_; }
        ServerAsyncResponseWriter<WorkerReply>* getWorkerResponder() { return &responder_; }


    private:

        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        WorkerService::AsyncService* service_;
        // The producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cq_;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        ServerContext ctx_;

        // What we get from the client.
        WorkerCommand request_;
        // What we send back to the client.
        WorkerReply reply_;

        // The means to get back to the client.
        ServerAsyncResponseWriter<WorkerReply> responder_;

        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.

        Worker & work_server_;
};


/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		~Worker();
		void setFail(bool fail);
		void handleRequest( void* tag);
		void handleCommand( CallData* msg);
		void handleGetStatusRequest( CallData *msg);
		void handleStopWorkerRequest( CallData *msg);
		void handleMapRequest( CallData *msg);
		void handleReduceRequest( CallData *msg);

		bool createThreadPool( uint8_t t_count );
		void enqueRequest( void* req_data);
		void* dequeRequest();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		WorkerState state_;
		WorkStatus work_status_;
		WorkerRole role_;
		int requests_handled_;
		std::string ip_addr_port_;
        std::unique_ptr<ServerCompletionQueue> cq_;
        WorkerService::AsyncService service_;
        std::unique_ptr<Server> server_;

		ThreadPool *thread_pool_;
		CircularBuffer circ_buff_;

};

