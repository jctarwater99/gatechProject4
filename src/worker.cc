
#include "worker.h"

// Global param
bool stop_execution = false;
bool will_fail = false;

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	//std::cout << "ip_addr_port: " << ip_addr_port << std::endl;
	ip_addr_port_ = ip_addr_port;
	requests_handled_ = 0;

	state_ = STATE_IDLE;
	work_status_ = WORK_INVALID;
	role_ = ROLE_NONE;

    createThreadPool(1);        // Create one background thread
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
    //std::cout << "Worker listening on " << ip_addr_port_ << std::endl;


	// Handle RPCs
	while (!stop_execution) {

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
        //GPR_ASSERT(ok);  // It can be false also in case of cancellation, so comment assert

		handleRequest(tag);
	}

	return false;
}

void Worker::setFail(bool fail) {
	will_fail = fail;
}


void Worker::handleRequest( void* tag)
{
	CallData *msg = nullptr;
	msg = static_cast<CallData*>(tag);

    if (msg) {

		WorkerCommand *cmd_received = msg->getWorkerCommand();
		CommandType cmd_type = cmd_received->cmd_type();
		//cout << "Worker Received Command : " << cmd_type << std::endl;

		switch (cmd_type) {

			// Move heavy tasks to bacground thread
			case CMD_TYPE_MAP:
			case CMD_TYPE_REDUCE:
			case CMD_TYPE_STOP_WORKER:
			{
				enqueRequest(tag);
			}
			break;

			// Reply ping requests quickly
			case CMD_TYPE_STATUS:
			{
				handleGetStatusRequest( msg);
			}
			break;

			default:
				break;

		}
    } else {
        cerr << "ERROR: Worker::handleRequest(): Invalid message" << endl;
    }
}


void Worker::handleCommand( CallData* msg )
{
    if (msg) {

		WorkerCommand *cmd_received = msg->getWorkerCommand();
		CommandType cmd_type = cmd_received->cmd_type();
		//cout << "handleCommand() : " << cmd_received->DebugString() << std::endl;

		switch (cmd_type) {

			case CMD_TYPE_MAP:
			{
				/*
				// TODO: Remove this delay after testing is done.
				// Test delay from one worker
				if (ip_addr_port_ == "localhost:50051") {
					sleep(20);
				}
				*/

				handleMapRequest(msg);
			}
			break;

			case CMD_TYPE_REDUCE:
			{
				handleReduceRequest(msg);
			}
			break;

			case CMD_TYPE_STOP_WORKER:
			{
				// Stop worker execution
				handleStopWorkerRequest( msg);
                stop_execution = true;
			}
			break;

			default:
				break;

		}
    } else {
        cerr << "ERROR: Worker::handleCommand(): Invalid message" << endl;
    }
}


static void request_handler(int idx, void* param )
{
	Worker* worker = static_cast<Worker*>(param);
    CallData* msg = nullptr;

	while(!stop_execution) {

        msg = static_cast<CallData*>(worker->dequeRequest());
        worker->handleCommand( msg);
    }

    return;
}


bool Worker::createThreadPool( uint8_t t_count)
{
    bool ret_val = false;

    if (thread_pool_ == nullptr) {
        thread_pool_ = new ThreadPool( t_count, request_handler, this);
        ret_val = true;
    }

    return ret_val;
}


void Worker::enqueRequest( void* req_data)
{
    circ_buff_.insert( req_data);
}


void* Worker::dequeRequest()
{
    return circ_buff_.remove();
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

	msg->proceed();
	//cout << "CMD_TYPE_STATUS: Sent Reply " << endl;
}

void Worker::handleStopWorkerRequest( CallData *msg)
{
	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	reply->set_cmd_seq_num( cmd_received->cmd_seq_num());
	reply->set_cmd_type( cmd_received->cmd_type());
	reply->set_cmd_status( CMD_STATUS_SUCCESS);

	msg->proceed();
	//cout << "CMD_TYPE_STOP_WORKER: Sent Reply " << endl;
}


void Worker::handleMapRequest( CallData *msg)
{
	if (will_fail) {
		stop_execution = true;
		return;
	}

	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	MapCommand map_cmd = cmd_received->map_cmd();

	state_ = STATE_WORKING;
	role_ = ROLE_MAPPER;


	// Perform Mapping function
	bool failed = false;
	auto mapper = get_mapper_from_task_factory(map_cmd.user_id()); 
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

	// Save to intermediate files

	vector<pair<string, string> >& pairs = mapper->impl_->key_value_pairs;
	sort(pairs.begin(), pairs.end());
	vector<ofstream> files;
	int n_output_files = map_cmd.n_output_files();

	// string output_filename = "mapper_" + ip_addr_port_ + "_" + to_string(map_cmd.shard_info().number());
	std::ofstream file; 
	for (int i = 0; i < n_output_files; i++) {
		// file = new ofstream("i_" + ip_addr_port_ + "_m" + to_string(map_cmd.shard_info().number()) + "_r" + to_string(i));
		files.push_back(ofstream("intermediate_m" + to_string(map_cmd.shard_info().number()) + "_r" + to_string(i) + ".txt"));
		if (!files.back().is_open()) {
			failed = true;
			break;
		}
		// files.push_back(file);
	}
 	if (!failed) {
		vector<pair<string, string> >::iterator it;
		for (it = pairs.begin(); it != pairs.end(); it++) {
			std::hash<std::string> hasher;
			auto hashed_string = hasher((*it).first);
			files[hashed_string % n_output_files] << (*it).first << " " << (*it).second << "\n";
		}
		// file.close(); // Called automatically when destructor is called
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

		for (int i = 0; i < n_output_files; i++) {
			r->add_filenames("intermediate_m" + to_string(map_cmd.shard_info().number()) + "_r" + to_string(i) + ".txt");
		}
	}

	msg->proceed();
	//cout << "CMD_TYPE_MAP: Sent Reply " << endl;

	state_ = STATE_IDLE;
	role_ = ROLE_NONE;
}


void Worker::handleReduceRequest( CallData *msg)
{
	WorkerCommand *cmd_received = msg->getWorkerCommand();
	WorkerReply * reply = msg->getWorkerReply();
	ReduceCommand reduce_cmd = cmd_received->reduce_cmd();

	state_ = STATE_WORKING;
	role_ = ROLE_REDUCER;

	bool failed = false;
	auto reducer = get_reducer_from_task_factory(reduce_cmd.user_id()); 

	vector<pair<string, string> > temp_pairs;

	// Get Data from Intermediate Files
	for (int i = 0; i < reduce_cmd.n_intermediate_files(); i++) {
		string line;
		ifstream file("intermediate_m" + to_string(i) + "_r" + to_string(reduce_cmd.reducer_num()) + ".txt");
		if (!file.is_open()) {
			failed = true;
			break;
		}
		string key, value;
		while (getline(file, line)) {
			istringstream is_line(line);
			if (getline(is_line, key, ' ') && getline(is_line, value)) {
				temp_pairs.push_back(make_pair(key, value));
			}
		}
	}

	sort(temp_pairs.begin(), temp_pairs.end());

	// Perform Reducing
	string previous;
	vector<string> values;
	vector<pair<string, string> >::iterator it;
	for (it = temp_pairs.begin(); it != temp_pairs.end(); it++) {
		if (previous.compare("") == 0 || (*it).first.compare(previous) == 0) {
			values.push_back((*it).second);
			previous = (*it).first;
		} else {
			reducer->reduce(previous, values);
			previous = (*it).first;
			values.clear();
			values.push_back((*it).second);
		}
	}
	reducer->reduce(previous, values);
	values.clear();
	temp_pairs.clear();

	// Save to output file
	vector<pair<string, string> >& pairs = reducer->impl_->key_value_pairs;
	sort(pairs.begin(), pairs.end());
	vector<ofstream> files;
	std::ofstream file(reduce_cmd.output_dir() + "/output_"+ to_string(reduce_cmd.reducer_num()) + ".txt"); // TODO: Fix folder
	if (!file.is_open()) {
		failed = true;
	}
	if (!failed) {
		vector<pair<string, string> >::iterator it;
		for (it = pairs.begin(); it != pairs.end(); it++) {
			file << (*it).first << " " << (*it).second << "\n";
		}
		// file.close(); // Called automatically when destructor is called
	}

	// Return reply:
	reply->set_cmd_seq_num( cmd_received->cmd_seq_num());
	reply->set_cmd_type( cmd_received->cmd_type());
	if (failed) {
		reply->set_cmd_status( CMD_STATUS_FAIL);
	} else {
		reply->set_cmd_status( CMD_STATUS_SUCCESS);
		// MapReply * r = reply->mutable_map_reply();
		// r->set_mapper_id(ip_addr_port_);

		// for (int i = 0; i < n_output_files; i++) {
		// 	r->add_filenames("i_" + ip_addr_port_ + "_m" + to_string(map_cmd.shard_info().number()) + "_r" + to_string(i));
		// }
	}

	msg->proceed();
	//cout << "CMD_TYPE_REDUCE: Sent Reply " << endl;

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
		//ctx_.AsyncNotifyWhenDone(this);

    } else if (status_ == PROCESS) {

        // The actual processing.

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = FINISH;

		Status retStatus = Status::OK;

		/*
		if (ctx_.IsCancelled()) {  // TODO: uncomment above AsyncNotifyWhenDone() call along with this
			retStatus = Status::CANCELLED;
			cout << " CANCELLED by the client: ********" << endl;
		}
		*/

        //responder_.Finish(reply_, Status::OK, this);
        responder_.Finish(reply_, retStatus, this);

    } else {
		//cout << "CallData: FINISH call, deleteing myself" << endl;
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
    }

}


