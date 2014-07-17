/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SYNCUTILS_H_
#define SYNCUTILS_H_

#include <unistd.h>
#include <string.h>
#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif
#include <pthread.h>

namespace NativeTask {

class Condition;

class Lock {
public:
  Lock();
  ~Lock();

  void lock();
  void unlock();

private:
  friend class Condition;
  pthread_mutex_t _mutex;

  // No copying
  Lock(const Lock&);
  void operator=(const Lock&);
};

class SpinLock {
public:
  SpinLock();
  ~SpinLock();

  void lock();
  void unlock();

private:
#ifdef __MACH__
  OSSpinLock _spin;
#else
  pthread_spinlock_t _spin;
#endif

  // No copying
  SpinLock(const Lock&);
  void operator=(const Lock&);
};

class Condition {
public:
  explicit Condition(Lock* mu);
  ~Condition();
  void wait();
  void signal();
  void signalAll();
private:
  pthread_cond_t _condition;
  Lock* _lock;
};

template<typename LockT>
class ScopeLock {
public:
  ScopeLock(LockT & lock)
      : _lock(&lock) {
    _lock->lock();
  }
  ~ScopeLock() {
    _lock->unlock();
  }
private:
  LockT * _lock;

  // No copying
  ScopeLock(const ScopeLock&);
  void operator=(const ScopeLock&);
};

class Runnable {
public:
  virtual ~Runnable() {
  }
  virtual void run() = 0;
};

class Thread : public Runnable {
protected:
  pthread_t _thread;
  Runnable * _runable;
public:
  Thread();
  Thread(Runnable * runnable);
  virtual ~Thread();

  void setTask(const Runnable & runnable);
  void start();
  void join();
  void stop();
  virtual void run();

  /**
   * Enable JNI for current thread
   */
  static void EnableJNI();
  /**
   * Release JNI when thread is at end if current
   * thread called EnableJNI before
   */
  static void ReleaseJNI();
private:
  static void * ThreadRunner(void * pthis);
};

// Sure <tr1/functional> is better
template<typename Subject, typename Method>
class FunctionRunner : public Runnable {
protected:
  Subject & _subject;
  Method _method;
public:
  FunctionRunner(Subject & subject, Method method)
      : _subject(subject), _method(method) {
  }

  virtual void run() {
    (_subject.*_method)();
  }
};

template<typename Subject, typename Method, typename Arg>
class FunctionRunner1 : public Runnable {
protected:
  Subject & _subject;
  Method _method;
  Arg _arg;
public:
  FunctionRunner1(Subject & subject, Method method, Arg arg)
      : _subject(subject), _method(method), _arg(arg) {
  }

  virtual void run() {
    (_subject.*_method)(_arg);
  }
};

template<typename Subject, typename Method, typename Arg1, typename Arg2>
class FunctionRunner2 : public Runnable {
protected:
  Subject & _subject;
  Method _method;
  Arg1 _arg1;
  Arg2 _arg2;
public:
  FunctionRunner2(Subject & subject, Method method, Arg1 arg1, Arg2 arg2)
      : _subject(subject), _method(method), _arg1(arg1), _arg2(arg2) {
  }

  virtual void run() {
    (_subject.*_method)(_arg1, _arg2);
  }
};

template<typename Subject, typename Method>
inline FunctionRunner<Subject, Method> * BindNew(Subject & subject, Method method) {
  return new FunctionRunner<Subject, Method>(subject, method);
}

template<typename Subject, typename Method, typename Arg>
inline FunctionRunner1<Subject, Method, Arg> * BindNew(Subject & subject, Method method, Arg arg) {
  return new FunctionRunner1<Subject, Method, Arg>(subject, method, arg);
}

template<typename Subject, typename Method, typename Arg1, typename Arg2>
inline FunctionRunner2<Subject, Method, Arg1, Arg2> * BindNew(Subject & subject, Method method,
    Arg1 arg1, Arg2 arg2) {
  return new FunctionRunner2<Subject, Method, Arg1, Arg2>(subject, method, arg1, arg2);
}

class ConcurrentIndex {
private:
  size_t _index;
  size_t _end;
  SpinLock _lock;
public:
  ConcurrentIndex(size_t count)
      : _index(0), _end(count) {
  }

  ConcurrentIndex(size_t start, size_t end)
      : _index(start), _end(end) {
  }

  size_t count() {
    return _end;
  }

  ssize_t next() {
    ScopeLock<SpinLock> autoLock(_lock);
    if (_index == _end) {
      return -1;
    } else {
      ssize_t ret = _index;
      _index++;
      return ret;
    }
  }
};

template<typename Subject, typename Method, typename RangeType>
class ParallelForWorker : public Runnable {
protected:
  ConcurrentIndex * _index;
  Subject * _subject;
  Method _method;
public:
  ParallelForWorker()
      : _index(NULL), _subject(NULL) {
  }

  ParallelForWorker(ConcurrentIndex * index, Subject * subject, Method method)
      : _index(index), _subject(subject), _method(method) {
  }

  void reset(ConcurrentIndex * index, Subject * subject, Method method) {
    _index = index;
    _subject = subject;
    _method = method;
  }

  virtual void run() {
    ssize_t i;
    while ((i = _index->next()) >= 0) {
      (_subject->*_method)(i);
    }
  }
};

template<typename Subject, typename Method, typename RangeType>
void ParallelFor(Subject & subject, Method method, RangeType begin, RangeType end,
    size_t thread_num) {
  RangeType count = end - begin;
  if (thread_num <= 1 || count <= 1) {
    for (RangeType i = begin; i < end; i++) {
      (subject.*method)(i);
    }
  } else if (thread_num == 2) {
    ConcurrentIndex index = ConcurrentIndex(begin, end);
    ParallelForWorker<Subject, Method, RangeType> workers[2];
    Thread sideThread;
    workers[0].reset(&index, &subject, method);
    workers[1].reset(&index, &subject, method);
    sideThread.setTask(workers[0]);
    sideThread.start();
    workers[1].run();
    sideThread.join();
  } else {
    ConcurrentIndex index = ConcurrentIndex(begin, end);
    ParallelForWorker<Subject, Method, RangeType> * workers = new ParallelForWorker<Subject, Method,
        RangeType> [thread_num];
    Thread * threads = new Thread[thread_num - 1];
    for (size_t i = 0; i < thread_num - 1; i++) {
      workers[i].reset(&index, &subject, method);
      threads[i].setTask(workers[i]);
      threads[i].start();
    }
    workers[thread_num - 1].reset(&index, &subject, method);
    workers[thread_num - 1].run();
    for (size_t i = 0; i < thread_num - 1; i++) {
      threads[i].join();
    }
    delete[] threads;
    delete[] workers;
  }
}

} // namespace NativeTask

#endif /* SYNCUTILS_H_ */
