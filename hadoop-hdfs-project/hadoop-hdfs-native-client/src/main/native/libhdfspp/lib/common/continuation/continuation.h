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
#ifndef LIB_COMMON_CONTINUATION_CONTINUATION_H_
#define LIB_COMMON_CONTINUATION_CONTINUATION_H_

#include "hdfspp/status.h"
#include "common/cancel_tracker.h"

#include <functional>
#include <memory>
#include <vector>

namespace hdfs {
namespace continuation {

class PipelineBase;

/**
 * A continuation is a fragment of runnable code whose execution will
 * be scheduled by a \link Pipeline \endlink.
 *
 * The Continuation class is a build block to implement the
 * Continuation Passing Style (CPS) in libhdfs++. In CPS, the
 * upper-level user specifies the control flow by chaining a sequence
 * of continuations explicitly through the \link Run() \endlink method,
 * while in traditional imperative programming the sequences of
 * sentences implicitly specify the control flow.
 *
 * See http://en.wikipedia.org/wiki/Continuation for more details.
 **/
class Continuation {
public:
  typedef std::function<void(const Status &)> Next;
  virtual ~Continuation() = default;
  virtual void Run(const Next &next) = 0;
  Continuation(const Continuation &) = delete;
  Continuation &operator=(const Continuation &) = delete;

protected:
  Continuation() = default;
};

/**
 * A pipeline schedules the execution of a chain of \link Continuation
 * \endlink. The pipeline schedules the execution of continuations
 * based on their order in the pipeline, where the next parameter for
 * each continuation points to the \link Schedule() \endlink
 * method. That way the pipeline executes all scheduled continuations
 * in sequence.
 *
 * The typical use case of a pipeline is executing continuations
 * asynchronously. Note that a continuation calls the next
 * continuation when it is finished. If the continuation is posted
 * into an asynchronous event loop, invoking the next continuation
 * can be done in the callback handler in the asynchronous event loop.
 *
 * The pipeline allocates the memory as follows. A pipeline is always
 * allocated on the heap. It owns all the continuations as well as the
 * the state specified by the user. Both the continuations and the
 * state have the same life cycle of the pipeline. The design
 * simplifies the problem of ensuring that the executions in the
 * asynchronous event loop always hold valid pointers w.r.t. the
 * pipeline. The pipeline will automatically deallocate itself right
 * after it invokes the callback specified the user.
 **/
template <class State> class Pipeline {
public:
  typedef std::function<void(const Status &, const State &)> UserHandler;
  static Pipeline *Create() { return new Pipeline(); }
  static Pipeline *Create(CancelHandle cancel_handle) {
    return new Pipeline(cancel_handle);
  }
  Pipeline &Push(Continuation *stage);
  void Run(UserHandler &&handler);
  State &state() { return state_; }

private:
  State state_;
  std::vector<std::unique_ptr<Continuation>> routines_;
  size_t stage_;
  std::function<void(const Status &, const State &)> handler_;

  Pipeline() : stage_(0), cancel_handle_(CancelTracker::New()) {}
  Pipeline(CancelHandle cancel_handle) : stage_(0), cancel_handle_(cancel_handle) {}
  ~Pipeline() = default;
  void Schedule(const Status &status);
  CancelHandle cancel_handle_;
};

template <class State>
inline Pipeline<State> &Pipeline<State>::Push(Continuation *stage) {
  routines_.emplace_back(std::unique_ptr<Continuation>(stage));
  return *this;
}

template <class State>
inline void Pipeline<State>::Schedule(const Status &status) {
  // catch cancelation signalled from outside of pipeline
  if(cancel_handle_->is_canceled()) {
    handler_(Status::Canceled(), state_);
    routines_.clear();
    delete this;
  } else if (!status.ok() || stage_ >= routines_.size()) {
    handler_(status, state_);
    routines_.clear();
    delete this;
  } else {
    auto next = routines_[stage_].get();
    ++stage_;
    next->Run(std::bind(&Pipeline::Schedule, this, std::placeholders::_1));
  }
}

template <class State> inline void Pipeline<State>::Run(UserHandler &&handler) {
  handler_ = std::move(handler);
  Schedule(Status::OK());
}

template <class Handler> class BindContinuation : public Continuation {
public:
  BindContinuation(const Handler &handler) : handler_(handler) {}
  virtual void Run(const Next &next) override { handler_(next); }

private:
  Handler handler_;
};

template <class Handler> static inline Continuation *Bind(const Handler &handler) {
  return new BindContinuation<Handler>(handler);
}
}
}

#endif
