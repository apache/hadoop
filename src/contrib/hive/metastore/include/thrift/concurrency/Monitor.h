// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_CONCURRENCY_MONITOR_H_
#define _THRIFT_CONCURRENCY_MONITOR_H_ 1

#include "Exception.h"

namespace facebook { namespace thrift { namespace concurrency {

/**
 * A monitor is a combination mutex and condition-event.  Waiting and
 * notifying condition events requires that the caller own the mutex.  Mutex
 * lock and unlock operations can be performed independently of condition
 * events.  This is more or less analogous to java.lang.Object multi-thread
 * operations
 *
 * Note that all methods are const.  Monitors implement logical constness, not
 * bit constness.  This allows const methods to call monitor methods without
 * needing to cast away constness or change to non-const signatures.
 *
 * @author marc
 * @version $Id:$
 */
class Monitor {

 public:

  Monitor();

  virtual ~Monitor();

  virtual void lock() const;

  virtual void unlock() const;

  virtual void wait(int64_t timeout=0LL) const;

  virtual void notify() const;

  virtual void notifyAll() const;

 private:

  class Impl;

  Impl* impl_;
};

class Synchronized {
 public:

 Synchronized(const Monitor& value) :
   monitor_(value) {
   monitor_.lock();
  }

  ~Synchronized() {
    monitor_.unlock();
  }

 private:
  const Monitor& monitor_;
};


}}} // facebook::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_MONITOR_H_
