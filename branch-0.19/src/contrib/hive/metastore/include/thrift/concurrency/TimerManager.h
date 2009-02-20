// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_CONCURRENCY_TIMERMANAGER_H_
#define _THRIFT_CONCURRENCY_TIMERMANAGER_H_ 1

#include "Exception.h"
#include "Monitor.h"
#include "Thread.h"

#include <boost/shared_ptr.hpp>
#include <map>
#include <time.h>

namespace facebook { namespace thrift { namespace concurrency { 

/**
 * Timer Manager 
 * 
 * This class dispatches timer tasks when they fall due.
 *  
 * @author marc
 * @version $Id:$
 */
class TimerManager {

 public:

  TimerManager();

  virtual ~TimerManager();

  virtual boost::shared_ptr<const ThreadFactory> threadFactory() const;

  virtual void threadFactory(boost::shared_ptr<const ThreadFactory> value);

  /**
   * Starts the timer manager service 
   *
   * @throws IllegalArgumentException Missing thread factory attribute
   */
  virtual void start();

  /**
   * Stops the timer manager service
   */
  virtual void stop();

  virtual size_t taskCount() const ;

  /**
   * Adds a task to be executed at some time in the future by a worker thread.
   * 
   * @param task The task to execute
   * @param timeout Time in milliseconds to delay before executing task
   */
  virtual void add(boost::shared_ptr<Runnable> task, int64_t timeout);

  /**
   * Adds a task to be executed at some time in the future by a worker thread.
   * 
   * @param task The task to execute
   * @param timeout Absolute time in the future to execute task.
   */ 
  virtual void add(boost::shared_ptr<Runnable> task, const struct timespec& timeout);

  /**
   * Removes a pending task 
   *
   * @throws NoSuchTaskException Specified task doesn't exist. It was either
   *                             processed already or this call was made for a
   *                             task that was never added to this timer
   *
   * @throws UncancellableTaskException Specified task is already being
   *                                    executed or has completed execution.
   */
  virtual void remove(boost::shared_ptr<Runnable> task);

  enum STATE {
    UNINITIALIZED,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED
  };
  
  virtual const STATE state() const;

 private:
  boost::shared_ptr<const ThreadFactory> threadFactory_;
  class Task;
  friend class Task;
  std::multimap<int64_t, boost::shared_ptr<Task> > taskMap_;
  size_t taskCount_;
  Monitor monitor_;
  STATE state_;
  class Dispatcher;
  friend class Dispatcher;
  boost::shared_ptr<Dispatcher> dispatcher_;
  boost::shared_ptr<Thread> dispatcherThread_;
};

}}} // facebook::thrift::concurrency

#endif // #ifndef _THRIFT_CONCURRENCY_TIMERMANAGER_H_
