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

#include "logging.h"

#include <ctime>
#include <cstring>
#include <thread>
#include <iostream>
#include <sstream>

namespace hdfs
{

LogManager::LogManager() {}
std::unique_ptr<LoggerInterface> LogManager::logger_impl_(new StderrLogger());
std::mutex LogManager::impl_lock_;
uint32_t LogManager::component_mask_ = 0xFFFFFFFF;
uint32_t LogManager::level_threshold_ = kWarning;

void LogManager::DisableLogForComponent(LogSourceComponent c) {
  // AND with all bits other than one we want to unset
  std::lock_guard<std::mutex> impl_lock(impl_lock_);
  component_mask_ &= ~c;
}

void LogManager::EnableLogForComponent(LogSourceComponent c) {
  // OR with bit to set
  std::lock_guard<std::mutex> impl_lock(impl_lock_);
  component_mask_ |= c;
}

void LogManager::SetLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> impl_lock(impl_lock_);
  level_threshold_ = level;
}

void LogManager::Write(const LogMessage& msg) {
  std::lock_guard<std::mutex> impl_lock(impl_lock_);
  if(logger_impl_)
    logger_impl_->Write(msg);
}

void LogManager::SetLoggerImplementation(std::unique_ptr<LoggerInterface> impl) {
  std::lock_guard<std::mutex> impl_lock(impl_lock_);
  logger_impl_.reset(impl.release());
}


/**
 *  Simple plugin to dump logs to stderr
 **/
void StderrLogger::Write(const LogMessage& msg) {
  std::stringstream formatted;

  if(show_level_)
    formatted << msg.level_string();

  if(show_component_)
    formatted << msg.component_string();

  if(show_timestamp_) {
    time_t current_time = std::time(nullptr);
    char timestr[128];
    memset(timestr, 0, 128);
    int res = std::strftime(timestr, 128, "%a %b %e %H:%M:%S %Y", std::localtime(&current_time));
    if(res > 0) {
      formatted << '[' << (const char*)timestr << ']';
    } else {
      formatted << "[Error formatting timestamp]";
    }
  }

  if(show_component_) {
    formatted << "[Thread id = " << std::this_thread::get_id() << ']';
  }

  if(show_file_) {
    //  __FILE__ contains absolute path, which is giant if doing a build inside the
    //  Hadoop tree.  Trim down to relative to libhdfspp/
    std::string abs_path(msg.file_name());
    size_t rel_path_idx = abs_path.find("libhdfspp/");
    //  Default to whole string if library is being built in an odd way
    if(rel_path_idx == std::string::npos)
      rel_path_idx = 0;

    formatted << '[' << (const char*)&abs_path[rel_path_idx] << ":" << msg.file_line() << ']';
  }

  std::cerr << formatted.str() << "    " << msg.MsgString() << std::endl;
}

void StderrLogger::set_show_timestamp(bool show) {
  show_timestamp_ = show;
}
void StderrLogger::set_show_level(bool show) {
  show_level_ = show;
}
void StderrLogger::set_show_thread(bool show) {
  show_thread_ = show;
}
void StderrLogger::set_show_component(bool show) {
  show_component_ = show;
}


LogMessage::~LogMessage() {
  LogManager::Write(*this);
}

LogMessage& LogMessage::operator<<(const std::string *str) {
  if(str)
    msg_buffer_ << str;
  else
    msg_buffer_ << "<nullptr>";
  return *this;
}

LogMessage& LogMessage::operator<<(const std::string& str) {
  msg_buffer_ << str;
  return *this;
}

LogMessage& LogMessage::operator<<(const ::asio::ip::tcp::endpoint& endpoint) {
  msg_buffer_ << endpoint;
  return *this;
}

LogMessage& LogMessage::operator<<(const char *str) {
  if(str)
    msg_buffer_ << str;
  else
    msg_buffer_ << "<nullptr>";
  return *this;
}

LogMessage& LogMessage::operator<<(bool val) {
  if(val)
    msg_buffer_ << "true";
  else
    msg_buffer_ << "false";
  return *this;
}

LogMessage& LogMessage::operator<<(int32_t val) {
  msg_buffer_ << val;
  return *this;
}

LogMessage& LogMessage::operator<<(uint32_t val) {
  msg_buffer_ << val;
  return *this;
}

LogMessage& LogMessage::operator<<(int64_t val) {
  msg_buffer_ << val;
  return *this;
}

LogMessage& LogMessage::operator<<(uint64_t val) {
  msg_buffer_ << val;
  return *this;
}

LogMessage& LogMessage::operator<<(void *ptr) {
  msg_buffer_ << ptr;
  return *this;
}


LogMessage& LogMessage::operator<<(const std::thread::id& tid) {
  msg_buffer_ << tid;
  return *this;
}

std::string LogMessage::MsgString() const {
  return msg_buffer_.str();
}

const char * kLevelStrings[5] = {
  "[TRACE ]",
  "[DEBUG ]",
  "[INFO  ]",
  "[WARN  ]",
  "[ERROR ]"
};

const char * LogMessage::level_string() const {
  return kLevelStrings[level_];
}

const char * kComponentStrings[6] = {
  "[Unknown       ]",
  "[RPC           ]",
  "[BlockReader   ]",
  "[FileHandle    ]",
  "[FileSystem    ]",
  "[Async Runtime ]",
};

const char * LogMessage::component_string() const {
  switch(component_) {
    case kRPC: return kComponentStrings[1];
    case kBlockReader: return kComponentStrings[2];
    case kFileHandle: return kComponentStrings[3];
    case kFileSystem: return kComponentStrings[4];
    case kAsyncRuntime: return kComponentStrings[5];
    default: return kComponentStrings[0];
  }
}

}
