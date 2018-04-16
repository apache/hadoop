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

#ifndef LIB_COMMON_LOGGING_H_
#define LIB_COMMON_LOGGING_H_

#include <asio/ip/tcp.hpp>

#include "hdfspp/log.h"

#include <sstream>
#include <mutex>
#include <memory>
#include <thread>

namespace hdfs {

/**
 *  Logging mechanism to provide lightweight logging to stderr as well as
 *  as a callback mechanism to allow C clients and larger third party libs
 *  to be used to handle logging.  When adding a new log message to the
 *  library use the macros defined below (LOG_TRACE..LOG_ERROR) rather than
 *  using the LogMessage and LogManager objects directly.
 **/

enum LogLevel {
  kTrace     = 0,
  kDebug     = 1,
  kInfo      = 2,
  kWarning   = 3,
  kError     = 4,
};

enum LogSourceComponent {
  kUnknown      = 1 << 0,
  kRPC          = 1 << 1,
  kBlockReader  = 1 << 2,
  kFileHandle   = 1 << 3,
  kFileSystem   = 1 << 4,
  kAsyncRuntime = 1 << 5,
};

#define LOG_TRACE(C, MSG) do { \
if(LogManager::ShouldLog(kTrace,C)) { \
  LogMessage(kTrace, __FILE__, __LINE__, C) MSG; \
}} while (0);


#define LOG_DEBUG(C, MSG) do { \
if(LogManager::ShouldLog(kDebug,C)) { \
  LogMessage(kDebug, __FILE__, __LINE__, C) MSG; \
}} while (0);

#define LOG_INFO(C, MSG) do { \
if(LogManager::ShouldLog(kInfo,C)) { \
  LogMessage(kInfo, __FILE__, __LINE__, C) MSG; \
}} while (0);

#define LOG_WARN(C, MSG) do { \
if(LogManager::ShouldLog(kWarning,C)) { \
  LogMessage(kWarning, __FILE__, __LINE__, C) MSG; \
}} while (0);

#define LOG_ERROR(C, MSG) do { \
if(LogManager::ShouldLog(kError,C)) { \
  LogMessage(kError, __FILE__, __LINE__, C) MSG; \
}} while (0);


class LogMessage;

class LoggerInterface {
 public:
  LoggerInterface() {};
  virtual ~LoggerInterface() {};

  /**
   *  User defined handling messages, common case would be printing somewhere.
   **/
  virtual void Write(const LogMessage& msg) = 0;
};

/**
 *  StderrLogger unsuprisingly dumps messages to stderr.
 *  This is the default logger if nothing else is explicitly set.
 **/
class StderrLogger : public LoggerInterface {
 public:
  StderrLogger() : show_timestamp_(true), show_level_(true),
                   show_thread_(true), show_component_(true),
                   show_file_(true) {}
  void Write(const LogMessage& msg);
  void set_show_timestamp(bool show);
  void set_show_level(bool show);
  void set_show_thread(bool show);
  void set_show_component(bool show);
 private:
  bool show_timestamp_;
  bool show_level_;
  bool show_thread_;
  bool show_component_;
  bool show_file_;
};


/**
 *  LogManager provides a thread safe static interface to the underlying
 *  logger implementation.
 **/
class LogManager {
 friend class LogMessage;
 public:
  //  allow easy inlining
  static bool ShouldLog(LogLevel level, LogSourceComponent source) {
    std::lock_guard<std::mutex> impl_lock(impl_lock_);
    if(level < level_threshold_)
      return false;
    if(!(source & component_mask_))
      return false;
    return true;
  }
  static void Write(const LogMessage & msg);
  static void EnableLogForComponent(LogSourceComponent c);
  static void DisableLogForComponent(LogSourceComponent c);
  static void SetLogLevel(LogLevel level);
  static void SetLoggerImplementation(std::unique_ptr<LoggerInterface> impl);

 private:
  // don't create instances of this
  LogManager();
  // synchronize all unsafe plugin calls
  static std::mutex impl_lock_;
  static std::unique_ptr<LoggerInterface> logger_impl_;
  // component and level masking
  static uint32_t component_mask_;
  static uint32_t level_threshold_;
};

/**
 *  LogMessage contains message text, along with other metadata about the message.
 *  Note:  For performance reasons a set of macros (see top of file) is used to
 *  create these inside of an if block.  Do not instantiate these directly, doing
 *  so will cause the message to be uncontitionally logged.  This minor inconvinience
 *  gives us a ~20% performance increase in the (common) case where few messages
 *  are worth logging; std::stringstream is expensive to construct.
 **/
class LogMessage {
 friend class LogManager;
 public:
  LogMessage(const LogLevel &l, const char *file, int line,
             LogSourceComponent component = kUnknown) :
             level_(l), component_(component), origin_file_(file), origin_line_(line){}

  ~LogMessage();

  const char *level_string() const;
  const char *component_string() const;
  LogLevel level() const {return level_; }
  LogSourceComponent component() const {return component_; }
  int file_line() const {return origin_line_; }
  const char * file_name() const {return origin_file_; }

  //print as-is, indicates when a nullptr was passed in
  LogMessage& operator<<(const char *);
  LogMessage& operator<<(const std::string*);
  LogMessage& operator<<(const std::string&);

  //convert to a string "true"/"false"
  LogMessage& operator<<(bool);

  //integral types
  LogMessage& operator<<(int32_t);
  LogMessage& operator<<(uint32_t);
  LogMessage& operator<<(int64_t);
  LogMessage& operator<<(uint64_t);

  //print address as hex
  LogMessage& operator<<(void *);

  //asio types
  LogMessage& operator<<(const ::asio::ip::tcp::endpoint& endpoint);

  //thread and mutex types
  LogMessage& operator<<(const std::thread::id& tid);


  std::string MsgString() const;

 private:
  LogLevel level_;
  LogSourceComponent component_;
  const char *origin_file_;
  const int origin_line_;
  std::stringstream msg_buffer_;
};

}

#endif
