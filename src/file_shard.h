#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"
using namespace std;


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     vector<FileSegment> segments;
};

struct FileSegment {
     string filename;
     int start_file_offset;
     int end_file_offset;     
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
     for (string filename : mr_spec.input_files) {

          cout << ifstream(filename).tellg() << endl;
     }
     

	return false;
}
