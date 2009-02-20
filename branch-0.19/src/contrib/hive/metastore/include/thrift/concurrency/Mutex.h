// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_CONCURRENCY_MUTEX_H_
#define _THRIFT_CONCURRENCY_MUTEX_H_ 1

#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace concurrency { 

/**
 * A simple mutex class
 *
 * @author marc
 * @version $Id:$
 */
class Mutex {
 public:
  Mutex();
  virtual ~Mutex() {}
  virtual void lock() const;
  virtual bool trylock() const;
  virtual void unlock() const;

 private:

  class impl;
  boost::shared_ptr<impl> impl_;
};

class ReadWriteMutex {
public:
  ReadWriteMutex();
  virtual ~ReadWriteMutex() {}

  // these get the lock and block until it is done successfully
  virtual void acquireRead() const;
  virtual void acquireWrite() const;

  // these attempt to get the lock, returning false immediately if they fail
  virtual bool attemptRead() const;
  virtual bool attemptWrite() const;

  // this releases both read and write locks
  virtual void release() const;
   
private:

  class impl;
  boost::shared_ptr<impl> impl_;
};

class Guard {
 public: 
  Guard(const Mutex& value) : mutex_(value) {
    mutex_.lock();
  }
  ~Guard() {
    mutex_.unlock();
  }

 private:
  const Mutex& mutex_;
};

class RWGuard {
  public: 
    RWGuard(const ReadWriteMutex& value, bool write = 0) : rw_mutex_(value) {
      if (write) {
        rw_mutex_.acquireWrite();
      } else {
        rw_mutex_.acquireRead();
      }
    }  
    ~RWGuard() {
      rw_mutex_.release();
    }  
  private: 
    const ReadWriteMutex& rw_mutex_;
};  


// A little hack to prevent someone from trying to do "Guard(m);"
// Sorry for polluting the global namespace, but I think it's worth it.
#define Guard(m) incorrect_use_of_Guard(m)
#define RWGuard(m) incorrect_use_of_RWGuard(m)


}}} // facebook::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_MUTEX_H_
