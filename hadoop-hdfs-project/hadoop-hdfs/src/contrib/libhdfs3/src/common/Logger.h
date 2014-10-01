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

#ifndef _HDFS_LIBHDFS3_COMMON_LOGGER_H_
#define _HDFS_LIBHDFS3_COMMON_LOGGER_H_

#define DEFAULT_LOG_LEVEL INFO

namespace hdfs {
namespace internal {

extern const char * const SeverityName[];

enum LogSeverity {
    FATAL, LOG_ERROR, WARNING, INFO, DEBUG1, DEBUG2, DEBUG3, NUM_SEVERITIES
};

class Logger;

class Logger {
public:
    Logger();

    ~Logger();

    void setOutputFd(int f);

    void setLogSeverity(LogSeverity l);

    void printf(LogSeverity s, const char * fmt, ...)
      __attribute__((format(printf, 3, 4)));

private:
    int fd;
    LogSeverity severity;
};

extern Logger RootLogger;

}
}

#define LOG(s, fmt, ...) \
    hdfs::internal::RootLogger.printf(s, fmt, ##__VA_ARGS__)

#endif /* _HDFS_LIBHDFS3_COMMON_LOGGER_H_ */
