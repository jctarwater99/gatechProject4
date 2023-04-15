#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"
using namespace std;


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     vector<struct FileSegment> segments;
};

struct FileSegment {
     string filename;
     int start_line;
     int end_line;     
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
     const int MAX_SHARD_SIZE = mr_spec.map_kilobytes * 1024;
     int file_index;
     int shard_size = 0;
     string line;
     FileShard currShard;

     for (string filename : mr_spec.input_files) {
          FileSegment currSegment;
          currSegment.start_line = 0;
          currSegment.filename = filename;

          file_index = 0;
          ifstream file(filename);
          while(getline(file, line)) {
               file_index += 1;
               shard_size += line.size();

               // If the end of the shard is reached while mid file
               if (shard_size > MAX_SHARD_SIZE) {
                    currSegment.end_line = file_index;
                    currShard.segments.push_back(currSegment);
                    FileSegment newSegment;
                    newSegment.filename = filename;
                    newSegment.start_line = file_index;
                    currSegment = newSegment;

                    fileShards.push_back(currShard);
                    FileShard newShard;
                    currShard = newShard;
                    shard_size = 0;
               }
          }

          // If the end of the file is reached while mid shard
          currSegment.end_line = file_index;
          currShard.segments.push_back(currSegment);
     }

     fileShards.push_back(currShard);

	return true;
}
