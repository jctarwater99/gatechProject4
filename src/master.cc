#include "master.h"
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>

static constexpr int MESSAGE_TIMEOUT_IN_SECONDS = 1000;
static constexpr float MICRO_SECONDS = 1000000.0;


WorkerServiceClient::WorkerServiceClient(shared_ptr<Channel> channel, string w_ip)
  : stub_(WorkerService::NewStub(channel)), worker_ip_(w_ip)
  {}


void WorkerServiceClient::setMessageTimeDeadline( ClientContext & context )
{
	// Set message time deadline:#include <iostream>
#include <fstream>
#include <cstdio>
    time_point<system_clock> now_point = system_clock::now();
	auto deadline = now_point  + seconds(MESSAGE_TIMEOUT_IN_SECONDS);

    //time_t tt_now = system_clock::to_time_t(now_point);
    //time_t tt_deadline = system_clock::to_time_t(deadline);
    //cout << "Now time: " << ctime(&tt_now) << ", deadline: " << ctime(&tt_deadline) << endl;

	context.set_deadline(deadline);
}


bool WorkerServiceClient::executeCommand( WorkerCommand & wrk_cmd, WorkerReply & reply)
{
	ClientContext context;

	setMessageTimeDeadline(context);
	Status status = stub_->ExecuteCommand(&context, wrk_cmd, &reply);

	if (!status.ok()) {
		std::cerr << status.error_code() << ": " << status.error_message()
				  << std::endl;
		return false;
	}

	return true;
}


bool WorkerServiceClient::executeCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq)
{
	setMessageTimeDeadline(info->context);
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


bool WorkerServiceClient::sendStopWorkerCommand(WorkerReply & reply)
{
	WorkerCommand wrk_cmd;
	wrk_cmd.set_cmd_seq_num(1);	// TODO: keep it hard-coded for now
	wrk_cmd.set_cmd_type(CMD_TYPE_STOP_WORKER);

	return executeCommand(wrk_cmd, reply);
}


bool WorkerServiceClient::sendCommandAsync( WorkerCommand & wrk_cmd, AsyncGrpcInfo_t *info, CompletionQueue &cq)
{
	return executeCommandAsync(wrk_cmd, info, cq);
}


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {

	mr_state_ = MASTER_STATE_INIT;
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;

	for (int i = 0; i < mr_spec.n_output_files; i++) {
		ReduceJobState s = REDUCE_JOB_INIT;
		reduce_jobs_complete_.push_back(s);
	}

	// Create client-server channels
	for ( Worker w : mr_spec.workers) {
		WorkerServiceClient *c = new WorkerServiceClient ( grpc::CreateChannel( w.ip, grpc::InsecureChannelCredentials()), w.ip);
		worker_clients_.push_back(c);
	}

	// Init shards status
	for ( FileShard & s : file_shards_) {
		s.status = SHARD_STATUS_INIT;
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


void Master::handleMapResponse()
{
	//cout << "handleResponse() ..waiting" << endl;

	void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq_.Next(&got_tag, &ok));

	AsyncGrpcInfo_t* call = static_cast<AsyncGrpcInfo_t*>(got_tag);

	if (call->status.ok()) {
		//std::cout << "Received: " << call->reply.DebugString() << std::endl;

		// Check if shard is already processed by someother worker
		if ( SHARD_STATUS_PROCESSED != call->shard->status) {
			call->shard->status = SHARD_STATUS_PROCESSED;

			// TODO: Do we need to Store MapReply info somewhere that will be used for further Reduce phase???
		}

		call->worker->state = STATE_IDLE;

		// Once we're complete, deallocate the call object.
		delete call;

	} else {

		std::cerr << "ERROR: RPC failed*****: " << call->status.error_code() << ": " << call->status.error_message()
				  << std::endl;

		call->shard->status = SHARD_STATUS_FAILED;
		call->worker->state = STATE_FAILED;

		// TODO: memory-leak on "call" in the Non-OK status case??? Need to add cancellation logic at client side.
	}

}

void Master::handleReduceResponse()
{
	//cout << "handleResponse() ..waiting" << endl;

	void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq_.Next(&got_tag, &ok));
    GPR_ASSERT(ok);

	AsyncGrpcInfo_t* call = static_cast<AsyncGrpcInfo_t*>(got_tag);

	if (call->status.ok()) {
		//std::cout << "Received: " << call->reply.DebugString() << std::endl;

		*(call->reduce_job) = REDUCE_JOB_COMPLETE;
		call->worker->state = STATE_IDLE;

		// Once we're complete, deallocate the call object.
		delete call;

	} else {

		std::cerr << "ERROR: " << __func__  << ": RPC failed*****: " << call->status.error_code() << ": " << call->status.error_message()
				  << std::endl;
		*(call->reduce_job) = REDUCE_JOB_FAILED;
		call->worker->state = STATE_FAILED;

		// TODO: memory-leak on "call" in the Non-OK status case??? Need to add cancellation logic at client side.
	}
}


void Master::getWorkersStatus()
{
	// Query all workers about their status to know about available workers
	//cout << "sending getStatus command to workers" << endl;

	for (Worker & w: mr_spec_.workers) {
		WorkerReply reply;
		WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);
		if (clientObj->getWorkerStatus(reply) == true) {
			//cout << "Status Reply: " << reply.DebugString() << endl;
			w.state = reply.status_reply().worker_state();
			w.role = reply.status_reply().worker_role();
		} else {
			cerr << "ERROR: getWorkerStatus reply failed for address: " << w.ip << endl;
		}
	}
}


void Master::mapShard( FileShard & s, Worker & w )
{
	//cout << "mapShard(): shard_number: " << s.number << ", worker: " << w.ip << endl;
	WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);

	AsyncGrpcInfo_t *info = new AsyncGrpcInfo_t;

	w.state = STATE_WORKING;
	w.role = ROLE_MAPPER;
	s.status = SHARD_STATUS_MAPPING_IN_PROGRESS;
	s.map_begin_time = steady_clock::now();
	s.worker_id = w.ip;

	info->worker = &w;
	info->shard = &s;

	WorkerCommand cmd;
	cmd.set_cmd_seq_num(1); // TODO: Hardcoded
	cmd.set_cmd_type(CMD_TYPE_MAP);

	MapCommand *map_cmd = cmd.mutable_map_cmd();
	map_cmd->set_output_dir( mr_spec_.output_dir);
	map_cmd->set_n_output_files( mr_spec_.n_output_files);
	map_cmd->set_user_id( mr_spec_.user_id );

	FileShardInfo *shard_info = map_cmd->mutable_shard_info();
	shard_info->set_number(s.number);

	for ( FileSegment fs : s.segments) {
		FileSegmentInfo *fsi = shard_info->add_segment();
		fsi->set_filename( fs.filename);
		fsi->set_start_line( fs.start_line);
		fsi->set_end_line( fs.end_line);
	}

	// Send command to client
	clientObj->sendCommandAsync(cmd, info, cq_);

}


void Master::doShardMapping()
{
	bool allShardsMappingDone = false;

	// Run map on all shards
	while (1) {
		allShardsMappingDone = true;
		for ( FileShard & s : file_shards_ ) {
			if ((SHARD_STATUS_INIT == s.status) || (SHARD_STATUS_FAILED == s.status)) {
				allShardsMappingDone = false;

				if (SHARD_STATUS_FAILED == s.status) {
					cerr << "Moving Failed shard ( number: " << s.number << ", prev worker: " << s.worker_id << ") to next available worker >>>>>-----" << endl;
				}

				// Map this shard to available worker:
				for (Worker & w: mr_spec_.workers) {
					if (STATE_IDLE == w.state) {
						mapShard( s, w);
						break;
					}
				}
			} else if (SHARD_STATUS_MAPPING_IN_PROGRESS == s.status) {
				allShardsMappingDone = false;

				/* TODO: Do we need this???
				steady_clock::time_point current_time = steady_clock::now();
				auto elapsed_time_in_seconds = ((duration_cast<microseconds>(current_time - s.map_begin_time).count()) / MICRO_SECONDS);

				// If a shard-mapping is slow then schedule on different worker
				if (elapsed_time_in_seconds > MESSAGE_TIMEOUT_IN_SECONDS) {
					cout << "SLOW worker. Rescheduling shard (%d) to new mapper " << s.number << endl;
					for (Worker & w: mr_spec_.workers) {
						if (STATE_IDLE == w.state) {
							mapShard( s, w);
							break;
						}
					}
				}
				*/

			}
		}

		if (true == allShardsMappingDone) {
			break;	// break while
		} else {
			handleMapResponse();
		}

		// Fetch Workers status:
		//getWorkersStatus();

	}
}


void Master::runReduceJob( ReduceJobState & s, Worker & w, int reducer_num )
{
	WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);

	AsyncGrpcInfo_t *info = new AsyncGrpcInfo_t;
	info->worker = &w;
	// info->shard = &s;
	info->reduce_job = &s;

	WorkerCommand cmd;
	cmd.set_cmd_seq_num(1); // TODO: Hardcoded
	cmd.set_cmd_type(CMD_TYPE_REDUCE);

	ReduceCommand *reduce_cmd = cmd.mutable_reduce_cmd();
	reduce_cmd->set_output_dir( mr_spec_.output_dir);
	reduce_cmd->set_n_intermediate_files( file_shards_.size());
	reduce_cmd->set_user_id( mr_spec_.user_id );
	reduce_cmd->set_reducer_num( reducer_num );


	// Send reduce to client
	clientObj->sendCommandAsync(cmd, info, cq_);

}

void Master::doReducing()
{
	bool allReducingJobsComplete = false;

	// Run reduce on all shards
	while (1) {
		allReducingJobsComplete = true;
		int i = 0;
		for ( ReduceJobState & reduce_job_state : reduce_jobs_complete_ ) {
			if (reduce_job_state == REDUCE_JOB_INIT || reduce_job_state == REDUCE_JOB_FAILED) {
				allReducingJobsComplete = false;
				// Map this shard to available worker:
				for (Worker & w: mr_spec_.workers) {
					if (STATE_IDLE == w.state) {
						w.state = STATE_WORKING;
						w.role = ROLE_REDUCER;
						reduce_job_state = REDUCE_JOB_IN_PROGRESS;
						runReduceJob( reduce_job_state, w, i);
						break;
					}
				}
			} else if (reduce_job_state == REDUCE_JOB_IN_PROGRESS) { // TODO: Race condition? 
				allReducingJobsComplete = false;
			}
			i++;
		}

		if (true == allReducingJobsComplete) {
			break;	// break while
		} else {
			handleReduceResponse();
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
		//cout << "Current state: " << mr_state_ << endl;

		switch (mr_state_) {
			case MASTER_STATE_INIT:
			{
				//getWorkersStatus();

				// TODO: Change getWorkersStatus() to Async call. Slow worker can stall everything with synchronous call.
				for (Worker & w: mr_spec_.workers) {
					w.state = STATE_IDLE;
					w.role = ROLE_MAPPER;
				}

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
				doReducing();

				// Done with this state, move next
				mr_state_ = MASTER_STATE_COMPLETE;
			}
			break;

			case MASTER_STATE_COMPLETE:
			{

				// Stop all workers
				//cout << "sending Stop-Worker command:" << endl;
				for (Worker w: mr_spec_.workers) {
					WorkerReply reply;
					WorkerServiceClient* clientObj = getWorkerServiceClient(w.ip);
					if (clientObj->sendStopWorkerCommand(reply) == false) {
						cerr << "ERROR: sendStopWorkerCommand reply failed for address: " << w.ip << endl;
					}
				}

				for (int i = 0; i < file_shards_.size(); i++) {
					for (int j = 0; j < mr_spec_.n_output_files; j++) {
						string filename = "intermediate_m" + to_string(i) + "_r" + to_string(j)+ ".txt";
						char* filename_c = new char[filename.length() + 1];
						strcpy(filename_c, filename.c_str());
						std::remove(filename_c);
						delete[] filename_c;
					}
				}

				// Exit state machine
				return true;
			}
			break;

			default:
				return false;
				break;

		}
	}

	return false;
}
