
#include <iostream>
#include <thread>
#include <vector>

#include "threadpool.h"


int ThreadPool::MAX_THREADS_COUNT = 100;


ThreadPool::ThreadPool (uint8_t t_count, func_ptr fn, void* buff_ctx): thread_count_{t_count}, thread_fn_{fn}, buff_ctx_{buff_ctx}
{
    for (int i = 0; i < thread_count_; ++i) {
        threads_.push_back(thread_ptr(new std::thread(thread_fn_, i, buff_ctx)));
    }

    std::cout << "ThreadPool::start(): threads_.size(): " << threads_.size() << std::endl;
}


ThreadPool::~ThreadPool()
{
    for (long unsigned int i = 0; i < threads_.size(); ++i) {
        if (threads_[i]->joinable()) {
            threads_[i]->join();
        }
    }

}


