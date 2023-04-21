
#include <iostream>
#include <memory>

#include "circular_buffer.h"


CircularBuffer::CircularBuffer(): count_{0}, head_{0}, tail_{0}
{

}


CircularBuffer::~CircularBuffer()
{}


void CircularBuffer::insert( void* item)
{

    std::unique_lock< std::mutex> lock(m_);
    while( is_buffer_full() ) {
        space_available_.wait( lock);
    }

    buff_[head_] = item;
    head_ = ((head_ + 1) % MAX_BUFFER_SIZE);
    count_++;

    item_available_.notify_one();

}


void* CircularBuffer::remove()
{
    void* item = 0;

    std::unique_lock< std::mutex> lock(m_);
    while( is_buffer_empty() ) {
        item_available_.wait( lock);
    }

    item = buff_[tail_];
    tail_ = ((tail_ + 1) % MAX_BUFFER_SIZE);
    count_--;

    space_available_.notify_one();

    return item;

}


bool CircularBuffer::is_buffer_full()
{
    bool ret_val = false;

    if (count_ == MAX_BUFFER_SIZE) {
        ret_val = true;
    }

    return ret_val;
}


bool CircularBuffer::is_buffer_empty()
{
    bool ret_val = false;

    if (count_ == 0) {
        ret_val = true;
    }

    return ret_val;
}



