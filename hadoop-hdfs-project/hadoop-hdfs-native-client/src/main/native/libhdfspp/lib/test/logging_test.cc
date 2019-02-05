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

#include <common/logging.h>
#include <bindings/c/hdfs.cc>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>

using namespace hdfs;

struct log_state {
  int trace_count;
  int debug_count;
  int info_count;
  int warning_count;
  int error_count;

  int origin_unknown;
  int origin_rpc;
  int origin_blockreader;
  int origin_filehandle;
  int origin_filesystem;

  std::string msg;

  log_state() {
    reset();
  }

  void reset() {
    trace_count = 0;
    debug_count = 0;
    info_count = 0;
    warning_count = 0;
    error_count = 0;

    origin_unknown = 0;
    origin_rpc = 0;
    origin_blockreader = 0;
    origin_filehandle = 0;
    origin_filesystem = 0;

    msg = "";
  }
};
log_state log_state_instance;

void process_log_msg(LogData *data) {
  if(data->msg)
    log_state_instance.msg = data->msg;

  switch(data->level) {
    case HDFSPP_LOG_LEVEL_TRACE:
      log_state_instance.trace_count++;
      break;
    case HDFSPP_LOG_LEVEL_DEBUG:
      log_state_instance.debug_count++;
      break;
    case HDFSPP_LOG_LEVEL_INFO:
      log_state_instance.info_count++;
      break;
    case HDFSPP_LOG_LEVEL_WARN:
      log_state_instance.warning_count++;
      break;
    case HDFSPP_LOG_LEVEL_ERROR:
      log_state_instance.error_count++;
      break;
    default:
      //should never happen
      std::cout << "foo" << std::endl;
      ASSERT_FALSE(true);
  }

  switch(data->component) {
    case HDFSPP_LOG_COMPONENT_UNKNOWN:
      log_state_instance.origin_unknown++;
      break;
    case HDFSPP_LOG_COMPONENT_RPC:
      log_state_instance.origin_rpc++;
      break;
    case HDFSPP_LOG_COMPONENT_BLOCKREADER:
      log_state_instance.origin_blockreader++;
      break;
    case HDFSPP_LOG_COMPONENT_FILEHANDLE:
      log_state_instance.origin_filehandle++;
      break;
    case HDFSPP_LOG_COMPONENT_FILESYSTEM:
      log_state_instance.origin_filesystem++;
      break;
    default:
      std::cout << "bar" << std::endl;
      ASSERT_FALSE(true);
  }

}

void reset_log_counters() {
  log_state_instance.reset();
}

void assert_nothing_logged() {
  if(log_state_instance.trace_count || log_state_instance.debug_count ||
     log_state_instance.info_count || log_state_instance.warning_count ||
     log_state_instance.error_count) {
    ASSERT_FALSE(true);
  }
}

void assert_trace_logged() { ASSERT_TRUE(log_state_instance.trace_count > 0); }
void assert_debug_logged() { ASSERT_TRUE(log_state_instance.debug_count > 0); }
void assert_info_logged() { ASSERT_TRUE(log_state_instance.info_count > 0); }
void assert_warning_logged() { ASSERT_TRUE(log_state_instance.warning_count > 0); }
void assert_error_logged() { ASSERT_TRUE(log_state_instance.error_count > 0); }

void assert_no_trace_logged() { ASSERT_EQ(log_state_instance.trace_count, 0); }
void assert_no_debug_logged() { ASSERT_EQ(log_state_instance.debug_count, 0); }
void assert_no_info_logged() { ASSERT_EQ(log_state_instance.info_count, 0); }
void assert_no_warning_logged() { ASSERT_EQ(log_state_instance.warning_count, 0); }
void assert_no_error_logged() { ASSERT_EQ(log_state_instance.error_count, 0); }

void assert_unknown_logged() { ASSERT_TRUE(log_state_instance.origin_unknown > 0); }
void assert_rpc_logged() { ASSERT_TRUE(log_state_instance.origin_rpc > 0); }
void assert_blockreader_logged() { ASSERT_TRUE(log_state_instance.origin_blockreader > 0); }
void assert_filehandle_logged() { ASSERT_TRUE(log_state_instance.origin_filehandle > 0); }
void assert_filesystem_logged() { ASSERT_TRUE(log_state_instance.origin_filesystem > 0); }

void assert_no_unknown_logged() { ASSERT_EQ(log_state_instance.origin_unknown, 0); }
void assert_no_rpc_logged() { ASSERT_EQ(log_state_instance.origin_rpc, 0); }
void assert_no_blockreader_logged() { ASSERT_EQ(log_state_instance.origin_blockreader, 0); }
void assert_no_filehandle_logged() { ASSERT_EQ(log_state_instance.origin_filehandle, 0); }
void assert_no_filesystem_logged() { ASSERT_EQ(log_state_instance.origin_filesystem, 0); }

void log_all_components_at_level(LogLevel lvl) {
  if(lvl == kTrace) {
    LOG_TRACE(kUnknown, << 'a');
    LOG_TRACE(kRPC, << 'b');
    LOG_TRACE(kBlockReader, << 'c');
    LOG_TRACE(kFileHandle, << 'd');
    LOG_TRACE(kFileSystem, << 'e');
  } else if (lvl == kDebug) {
    LOG_DEBUG(kUnknown, << 'a');
    LOG_DEBUG(kRPC, << 'b');
    LOG_DEBUG(kBlockReader, << 'c');
    LOG_DEBUG(kFileHandle, << 'd');
    LOG_DEBUG(kFileSystem, << 'e');
  } else if (lvl == kInfo) {
    LOG_INFO(kUnknown, << 'a');
    LOG_INFO(kRPC, << 'b');
    LOG_INFO(kBlockReader, << 'c');
    LOG_INFO(kFileHandle, << 'd');
    LOG_INFO(kFileSystem, << 'e');
  } else if (lvl == kWarning) {
    LOG_WARN(kUnknown, << 'a');
    LOG_WARN(kRPC, << 'b');
    LOG_WARN(kBlockReader, << 'c');
    LOG_WARN(kFileHandle, << 'd');
    LOG_WARN(kFileSystem, << 'e');
  } else if (lvl == kError) {
    LOG_ERROR(kUnknown, << 'a');
    LOG_ERROR(kRPC, << 'b');
    LOG_ERROR(kBlockReader, << 'c');
    LOG_ERROR(kFileHandle, << 'd');
    LOG_ERROR(kFileSystem, << 'e');
  } else {
    // A level was added and not accounted for here
    ASSERT_TRUE(false);
  }
}

// make sure everything can be masked
TEST(LoggingTest, MaskAll) {
  LogManager::DisableLogForComponent(kUnknown);
  LogManager::DisableLogForComponent(kRPC);
  LogManager::DisableLogForComponent(kBlockReader);
  LogManager::DisableLogForComponent(kFileHandle);
  LogManager::DisableLogForComponent(kFileSystem);

  // use trace so anything that isn't masked should come through
  LogManager::SetLogLevel(kTrace);
  log_state_instance.reset();
  log_all_components_at_level(kError);
  assert_nothing_logged();
  log_state_instance.reset();
}

// make sure components can be masked individually
TEST(LoggingTest, MaskOne) {
  LogManager::DisableLogForComponent(kUnknown);
  LogManager::DisableLogForComponent(kRPC);
  LogManager::DisableLogForComponent(kBlockReader);
  LogManager::DisableLogForComponent(kFileHandle);
  LogManager::DisableLogForComponent(kFileSystem);
  LogManager::SetLogLevel(kTrace);

  // Unknown - aka component not provided
  LogManager::EnableLogForComponent(kUnknown);
  log_all_components_at_level(kError);
  assert_unknown_logged();
  assert_error_logged();
  assert_no_rpc_logged();
  assert_no_blockreader_logged();
  assert_no_filehandle_logged();
  assert_no_filesystem_logged();
  log_state_instance.reset();
  LogManager::DisableLogForComponent(kUnknown);

  // RPC
  LogManager::EnableLogForComponent(kRPC);
  log_all_components_at_level(kError);
  assert_rpc_logged();
  assert_error_logged();
  assert_no_unknown_logged();
  assert_no_blockreader_logged();
  assert_no_filehandle_logged();
  assert_no_filesystem_logged();
  log_state_instance.reset();
  LogManager::DisableLogForComponent(kRPC);

  // BlockReader
  LogManager::EnableLogForComponent(kBlockReader);
  log_all_components_at_level(kError);
  assert_blockreader_logged();
  assert_error_logged();
  assert_no_unknown_logged();
  assert_no_rpc_logged();
  assert_no_filehandle_logged();
  assert_no_filesystem_logged();
  log_state_instance.reset();
  LogManager::DisableLogForComponent(kBlockReader);

  // FileHandle
  LogManager::EnableLogForComponent(kFileHandle);
  log_all_components_at_level(kError);
  assert_filehandle_logged();
  assert_error_logged();
  assert_no_unknown_logged();
  assert_no_rpc_logged();
  assert_no_blockreader_logged();
  assert_no_filesystem_logged();
  log_state_instance.reset();
  LogManager::DisableLogForComponent(kFileHandle);

  // FileSystem
  LogManager::EnableLogForComponent(kFileSystem);
  log_all_components_at_level(kError);
  assert_filesystem_logged();
  assert_error_logged();
  assert_no_unknown_logged();
  assert_no_rpc_logged();
  assert_no_blockreader_logged();
  assert_no_filehandle_logged();
  log_state_instance.reset();
  LogManager::DisableLogForComponent(kFileSystem);
}

TEST(LoggingTest, Levels) {
  // should be safe to focus on one component if MaskOne passes
  LogManager::EnableLogForComponent(kUnknown);
  LogManager::SetLogLevel(kError);

  LOG_TRACE(kUnknown, << "a");
  LOG_DEBUG(kUnknown, << "b");
  LOG_INFO(kUnknown,<< "c");
  LOG_WARN(kUnknown, << "d");
  assert_nothing_logged();
  LOG_ERROR(kUnknown, << "e");
  assert_error_logged();
  assert_unknown_logged();
  log_state_instance.reset();

  // anything >= warning
  LogManager::SetLogLevel(kWarning);
  LOG_TRACE(kUnknown, << "a");
  LOG_DEBUG(kUnknown, << "b");
  LOG_INFO(kUnknown, << "c");
  assert_nothing_logged();
  LOG_WARN(kUnknown, << "d");
  assert_warning_logged();
  LOG_ERROR(kUnknown, << "e");
  assert_error_logged();
  log_state_instance.reset();

  // anything >= info
  LogManager::SetLogLevel(kInfo);
  LOG_TRACE(kUnknown, << "a");
  LOG_DEBUG(kUnknown, << "b");
  assert_nothing_logged();
  LOG_INFO(kUnknown, << "c");
  assert_info_logged();
  LOG_WARN(kUnknown, << "d");
  assert_warning_logged();
  LOG_ERROR(kUnknown, << "e");
  assert_error_logged();
  log_state_instance.reset();

  // anything >= debug
  LogManager::SetLogLevel(kDebug);
  LOG_TRACE(kUnknown, << "a");
  assert_nothing_logged();
  LOG_DEBUG(kUnknown, << "b");
  assert_debug_logged();
  assert_no_info_logged();
  assert_no_warning_logged();
  assert_no_error_logged();
  LOG_INFO(kUnknown, << "c");
  assert_info_logged();
  assert_no_warning_logged();
  assert_no_error_logged();
  LOG_WARN(kUnknown, << "d");
  assert_warning_logged();
  assert_no_error_logged();
  LOG_ERROR(kUnknown, << "e");
  assert_error_logged();
  log_state_instance.reset();

  // anything
  LogManager::SetLogLevel(kTrace);
  assert_nothing_logged();
  LOG_TRACE(kUnknown, << "a");
  assert_trace_logged();
  log_state_instance.reset();
  LOG_DEBUG(kUnknown, << "b");
  assert_debug_logged();
  log_state_instance.reset();
  LOG_INFO(kUnknown, << "c");
  assert_info_logged();
  log_state_instance.reset();
  LOG_WARN(kUnknown, << "d");
  assert_warning_logged();
  log_state_instance.reset();
  LOG_ERROR(kUnknown, << "e");
  assert_error_logged();
}

TEST(LoggingTest, Text) {
  LogManager::EnableLogForComponent(kRPC);

  std::string text;
  LOG_ERROR(kRPC, << text);

  ASSERT_EQ(text, log_state_instance.msg);
}


int main(int argc, char *argv[]) {
  CForwardingLogger *logger = new CForwardingLogger();
  logger->SetCallback(process_log_msg);
  LogManager::SetLoggerImplementation(std::unique_ptr<LoggerInterface>(logger));

  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int res = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();
  return res;
}
