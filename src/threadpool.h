#pragma once

#include <thread>
#include <vector>

typedef std::unique_ptr<std::thread> thread_ptr;

//typedef void (*func_ptr)(int, void*);
using func_ptr = void (*)(int, void*);


class ThreadPool
{
    public:
        ThreadPool (uint8_t t_count, func_ptr fn, void* buff_ctx);
        ~ThreadPool();

        void* getBuffContext() { return buff_ctx_; }

    private:

        static int MAX_THREADS_COUNT;

        uint8_t thread_count_;
        std::vector<thread_ptr> threads_;
        func_ptr thread_fn_;

        void* buff_ctx_;
};

