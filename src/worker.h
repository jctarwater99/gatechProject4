#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include<unistd.h>
#include<fstream>

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include "masterworker.grpc.pb.h"


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

		void handleGetStatusRequest( CallData *msg);
		void handleStopWorkerRequest( CallData *msg);
		void handleMapRequest( CallData *msg);
		// void handleReduceRequest( CallData *msg);


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

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	std::cout << "ip_addr_port: " << ip_addr_port << std::endl;
	ip_addr_port_ = ip_addr_port;
	requests_handled_ = 0;

	state_ = STATE_IDLE;
	work_status_ = WORK_INVALID;
	role_ = ROLE_NONE;
}


Worker::~Worker() {

	server_->Shutdown();
	// Always shutdown the completion queue after the server.
	cq_->Shutdown();

}


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {

    void* tag;  // uniquely identifies a request.
    bool ok;
	CallData* msg = nullptr;

	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	/*
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	*/

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(ip_addr_port_, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Worker listening on " << ip_addr_port_ << std::endl;


	// Handle RPCs
	while (1) {

        // Spawn a new CallData instance to serve new clients.
        CallData *newReq = new CallData( *this, &service_, cq_.get());

        // As part of the initial CREATE state, we *request* that the system
        // start processing client requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_.RequestExecuteCommand(newReq->getServerContext(), newReq->getWorkerCommand(), newReq->getWorkerResponder(), cq_.get(), cq_.get(), newReq);

        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or cq_ is shutting down.
        GPR_ASSERT(cq_->Next(&tag, &ok));
        GPR_ASSERT(ok);

		msg = static_cast<CallData*>(tag);
		WorkerCommand *cmd_received = msg->getWorkerCommand();
		CommandType cmd_type = cmd_received->cmd_type();
		std::cout << "Worker Received Command : " << cmd_received->DebugString() << std::endl;

		switch (cmd_type) {

			case CMD_TYPE_MAP:
			{
				/*
				// Test delay from one worker
				if (ip_addr_port_ == "localhost:50051") {
					sleep(10);
				}
				*/

				handleMapRequest(msg);
			}
			break;

			case CMD_TYPE_REDUCE:
			{

			}
			break;

			case CMD_TYPE_STATUS:
			{
				handleGetStatusRequest( msg);
			}
			break;

			case CMD_TYPE_STOP_WORKER:
			{
				// Stop worker execution
				handleStopWorkerRequest( msg);
				return true;
			}
			break;

			default:
				break;
		}

	}

	return false;
}


void Worker::handleGetStatusRequest( CallData *msg)
{
	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	reply->set_cmd_seq_num( cmd_received->cmd_seq_num());
	reply->set_cmd_type( cmd_received->cmd_type());
	reply->set_cmd_status( CMD_STATUS_SUCCESS);

	StatusReply *status_reply = reply->mutable_status_reply();
	status_reply->set_worker_state( state_);
	status_reply->set_work_status( work_status_);
	status_reply->set_worker_role( role_);
	cout << reply->DebugString() << endl;

	msg->proceed();
	cout << "CMD_TYPE_STATUS: Sent Reply " << endl;
}

void Worker::handleStopWorkerRequest( CallData *msg)
{
	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	reply->set_cmd_seq_num( cmd_received->cmd_seq_num());
	reply->set_cmd_type( cmd_received->cmd_type());
	reply->set_cmd_status( CMD_STATUS_SUCCESS);

	msg->proceed();
	cout << "CMD_TYPE_STOP_WORKER: Sent Reply " << endl;
}


void Worker::handleMapRequest( CallData *msg)
{
	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	MapCommand map_cmd = cmd_received->map_cmd();

	state_ = STATE_WORKING;
	role_ = ROLE_MAPPER;


	// Perform Mapping function
	bool failed = false;
	auto mapper = get_mapper_from_task_factory("cs6210"); // TODO: actually get needed string
	for (FileSegmentInfo segment :  map_cmd.shard_info().segment()) {
		// skip to segment.first_line
		string line;
		ifstream file(segment.filename());
		if (!file.is_open()) {
			failed = true;
			break;
		}
		for (int i = 0; i < segment.start_line() && file.good(); i++) {
			getline(file, line);
		}
		for (int i = segment.start_line(); i < segment.end_line() && file.good(); i++) {
			getline(file, line);
		 	mapper->map(line);
		}
	}

	// Save to intermediate file
	vector<pair<string, string> >& pairs = mapper->impl_->key_value_pairs;
	sort(pairs.begin(), pairs.end());

	string output_filename = "mapper_" + ip_addr_port_ + "_" + to_string(requests_handled_++);
	std::ofstream file(output_filename);
	vector<pair<string, string> >::iterator it;
	if (!file.is_open()) {
		failed = true;
	} else {
		for (it = pairs.begin(); it != pairs.end(); it++) {
			file << (*it).first << " " << (*it).second << "\n";
		}
		file.close();		
	}

	// Return reply:
	reply->set_cmd_seq_num( cmd_received->cmd_seq_num());
	reply->set_cmd_type( cmd_received->cmd_type());
	if (failed) {
		reply->set_cmd_status( CMD_STATUS_FAIL);
	} else {
		reply->set_cmd_status( CMD_STATUS_SUCCESS);
		MapReply * r = reply->mutable_map_reply();
		r->set_mapper_id(ip_addr_port_);
		r->add_filenames(output_filename);
	}

	msg->proceed();
	cout << "CMD_TYPE_MAP: Sent Reply " << endl;

	state_ = STATE_IDLE;
	role_ = ROLE_NONE;
}


// Take in the "service" instance (in this case representing an asynchronous
// server) and the completion queue "cq" used for asynchronous communication
// with the gRPC runtime.
CallData::CallData( Worker & wrkServer, WorkerService::AsyncService* service, ServerCompletionQueue* cq)
        : work_server_(wrkServer), service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {

    // Invoke the serving logic right away.
    proceed();
}


void CallData::proceed() {

    if (status_ == CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = PROCESS;

    } else if (status_ == PROCESS) {

        // The actual processing.

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;

		Status retStatus = Status::OK;

		/*
		if (ctx_.IsCancelled()) {
			retStatus = Status::CANCELLED;
			cout << " CANCELLED by the client: ********" << endl;
		}
		*/

        //responder_.Finish(reply_, Status::OK, this);
        responder_.Finish(reply_, retStatus, this);

    } else {
		cout << "CallData: FINISH call, deleteing myself" << endl;
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
    }

}


