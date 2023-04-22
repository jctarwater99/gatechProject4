
Project 4


Contributions
Shyam More and and Joshua Tarwater worked together for project 4. This project was difficult to split up given the synchronous way in which one would naturally code map reduce. So while the below might be a rough breakdown of who worked on what, it's important to note that we both worked on a little bit of everything. 

Shyam: 
* Wrote most of the definitions of the GRPC between the workers and master
* implemented the server and client communication between workers and master
* implemented the main loops of the master and workers to be fault compliant

Joshua: 
* Wrote the config.ini parser and mr_config_ datastructre 
* wrote the file_shard_ datastructure and sharding algorithm
* implemented the map and reduce functions on the master and client sides


Implementation
The majority of our code is spent monitoring the state of the task and setting up tearing down communication between the workers and master. The actual map and reduce functions are both relatively short and straight forward. After setup, the master checks the state of each worker, if any do not respond, then they are assumed immediately to have failed. Then file shards are distributed to the remaining workers. On responding, the shards are marked completed, any timed out grpc messages will result in the corresponding worker/shard being marked as failed and the shard will be rescheduled. The reduce tasks function similarly. Once every task is complete, the master cleans up the intermediate files. While this might not be in the spirit of map reduce, since the master and workers are on the same system, doing so simplified our handling of failed workers. If for example a reducer did not fail but was only slow, then it might delete the files while the rescheduled worker is trying to read the files resulting in a big mess of failing tasks. 


Organization 
mapreduce_impl.h/cc: 
These files are responsible for the setup. Doing things like sharding the input files, reading the config file, and calling the master's run function are all part of these files. 

file_shard.h:
This is the file *actually* responsible for sharding the input. Shards are just a list of segments, which consist of a start and end line, and a filename. 

mapreduce_spec.h:
This is the fiel *actually* responsible for parsing the input. It checks that n_workers match the worker ip fields, and that the provided input files actually exist.

master.h/cc: 
The master handles the coordination. It spins up a server to communicate through GRPC with all the workers, then it assigns map and reduce tasks, monitors for completion, and reassigns failed tasks to other workers when appropriate. Our implementation assumes workers fail after a set timeout period. 

worker.h/cc:
Our worker waits for a command from the master. All instructions are sent through the same GRPC service, and the type of command determines whether the worker will map, reduce, or just respond to a ping. Once it receives a task, it will parse the rest of the instruction so it knows what shards to use for mapping or what intermediate files to use for reducing. Then it reads in the appropriate data, passes it to the provided user map or reduce function, and once the key value pairs are emited, it puts them in the appropriate intermediate or output file. Much of the storage is accomplished through vectors. 

mr_tasks.h:
This file in theory handles the emit keypair functions calls from the provided user map and reduce functions, but all it does is append the keypair to a vector which is later dumped to a file. 


Difficulties
As mentioned above, it felt difficult to just do a part of the project, as every piece is dependent on the other parts. We also had to deal with the typical core dump execution errors that are inevitable when working in c/c++, especially when we were first setting up the commnication between the workers and master. Additionally, at one point during testing, we came across a race condition that only occurs when there are many mappers and reducers, and as of yet, we have not found the problem. 















# cs6210Project4
MapReduce Infrastructure

## Project Instructions

[Project Description](description.md)

[Code walk through](structure.md)

### How to setup the project  
Same as project 3 instructions
