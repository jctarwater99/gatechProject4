#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include "masterworker.grpc.pb.h"

using namespace masterworker;


/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	std::cout << "ip_addr_port: " << ip_addr_port << std::endl;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {


	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	/*
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	*/

	while (1) {
		// Wait for Command from Master
		WorkerCommand cmd_received;

		CommandType cmd_type = cmd_received.cmd_type();
		switch (cmd_type) {

			case CMD_TYPE_MAP:
			{

			}
			break;

			case CMD_TYPE_REDUCE:
			{

			}
			break;

			case CMD_TYPE_STATUS:
			{

			}
			break;

			case CMD_TYPE_STOP_WORKER:
			{
				// Stop worker execution
				return true;
			}
			break;

			default:
				break;
		}

	}

	return false;
}
