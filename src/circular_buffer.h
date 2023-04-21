#ifndef __CIRCULAR_BUFFER_H__
#define __CIRCULAR_BUFFER_H__

#include <mutex>
#include <condition_variable>


#define MAX_BUFFER_SIZE  1024

class CircularBuffer
{
    public:
        CircularBuffer ();
        ~CircularBuffer();

        void insert( void* item);
        void* remove();


    private:

        bool is_buffer_full();
        bool is_buffer_empty();

        void* buff_[MAX_BUFFER_SIZE];
        uint16_t count_;
        uint16_t head_;
        uint16_t tail_;
        std::mutex m_;
        std::condition_variable space_available_;
        std::condition_variable item_available_;

};

#endif      // __CIRCULAR_BUFFER_H__



