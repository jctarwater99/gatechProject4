#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>

#include "masterworker.grpc.pb.h"

using namespace std;
using namespace masterworker;


struct Worker {
	string ip;
	WorkerState sate;
	WorkerRole role;
};

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	vector<struct Worker> workers;
	vector<string> input_files;
	string output_dir;
	int n_output_files; 
	int map_kilobytes;
	string user_id;
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	ifstream config_file(config_filename);
	if (!config_file.is_open()) {
		return false;
	} 
	string line;
	string key;
	string value;
	string sub_value;

	while (getline(config_file, line)) {
		istringstream is_line(line); 
		if (getline(is_line, key, '=') && (getline(is_line, value))) {
			if (key == "n_workers") {
				istringstream(value) >> mr_spec.n_workers;
			} else if (key == "worker_ipaddr_ports") {
				istringstream ss(value);
				while(getline(ss, sub_value, ',')) {
					Worker w;
					w.ip = sub_value;
					mr_spec.workers.push_back(w);
				}
			} else if (key == "input_files")  {
				istringstream ss(value);
				while(getline(ss, sub_value, ',')) {
					mr_spec.input_files.push_back(sub_value);
				}
			} else if (key == "output_dir") {
				mr_spec.output_dir = value;
			} else if (key == "n_output_files") {
				istringstream(value) >> mr_spec.n_output_files;
			} else if (key == "map_kilobytes") {
				istringstream(value) >> mr_spec.map_kilobytes;
			} else if (key == "user_id") {
				mr_spec.user_id = value;
			} else {
				return false;
			}
		}
	}

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if ((mr_spec.n_workers == 0) ||
		(mr_spec.workers.size() != mr_spec.n_workers) ||
		(mr_spec.workers.size() == 0) ||
		(mr_spec.output_dir == "") ||
		(mr_spec.n_output_files == 0) ||
		(mr_spec.map_kilobytes == 0) ||
		(mr_spec.user_id == "")){
		return false;
	}

	for (Worker w: mr_spec.workers) {
		// Should maybe check that PORT is valid? 
		if (w.ip == "") {
			return false;
		}
	} 
	for (string s: mr_spec.input_files) {
		if (s == "" || !ifstream(s).good()) {
			cerr << "File: " << s << " not found\n";
			return false;
		}
	} 

	return true;
}
