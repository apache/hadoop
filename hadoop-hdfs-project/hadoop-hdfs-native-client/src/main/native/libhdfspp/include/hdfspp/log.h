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

#ifndef LIBHDFSPP_HDFS_LOG
#define LIBHDFSPP_HDFS_LOG

#ifdef __cplusplus
extern "C" {
#endif

/**
 *  Things that are part of the public API but are specific to logging live here.
 *  Added to avoid including the whole public API into the implementation of the logger.
 **/

/* logging levels, compatible with enum in lib/common/logging.cc */
#define HDFSPP_LOG_LEVEL_TRACE 0
#define HDFSPP_LOG_LEVEL_DEBUG 1
#define HDFSPP_LOG_LEVEL_INFO  2
#define HDFSPP_LOG_LEVEL_WARN  3
#define HDFSPP_LOG_LEVEL_ERROR 4

/* components emitting messages, compatible with enum lib/common/logging.cc */
#define HDFSPP_LOG_COMPONENT_UNKNOWN      1 << 0
#define HDFSPP_LOG_COMPONENT_RPC          1 << 1
#define HDFSPP_LOG_COMPONENT_BLOCKREADER  1 << 2
#define HDFSPP_LOG_COMPONENT_FILEHANDLE   1 << 3
#define HDFSPP_LOG_COMPONENT_FILESYSTEM   1 << 4

/**
 *  POD struct for C to consume (C++ interface gets to take advantage of RAII)
 **/
typedef struct {
  const char *msg;
  int level;
  int component;
  const char *file_name;
  int file_line;
} LogData;

#ifdef __cplusplus
} // end extern C
#endif

#endif
