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

#include "platform.h"

#include "Logger.h"

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

#include "DateTime.h"
#include "Thread.h"

namespace hdfs {
namespace internal {

Logger RootLogger;

static mutex LoggerMutex;
static THREAD_LOCAL char ProcessId[64];

const char * const SeverityName[] = {
  "FATAL", "ERROR", "WARNING", "INFO", "DEBUG1", "DEBUG2", "DEBUG3"
};

static void InitProcessId(char *p, size_t p_len) {
    std::stringstream ss;
    ss << "p" << getpid() << ", th" << pthread_self();
    snprintf(p, p_len, "%s", ss.str().c_str());
}

Logger::Logger() :
    fd(STDERR_FILENO), severity(DEFAULT_LOG_LEVEL) {
}

Logger::~Logger() {
}

void Logger::setOutputFd(int f) {
    fd = f;
}

void Logger::setLogSeverity(LogSeverity l) {
    severity = l;
}

void Logger::printf(LogSeverity s, const char *fmt, ...) {
    va_list ap;

    if (s > severity || fd < 0) {
        return;
    }

    try {
        if (ProcessId[0] == '\0') {
          InitProcessId(ProcessId, sizeof(ProcessId));
        }
        std::vector<char> buffer;
        struct tm tm_time;
        struct timeval tval;
        memset(&tval, 0, sizeof(tval));
        gettimeofday(&tval, NULL);
        localtime_r(&tval.tv_sec, &tm_time);
        //determine buffer size
        va_start(ap, fmt);
        int size = vsnprintf(&buffer[0], buffer.size(), fmt, ap);
        va_end(ap);
        //100 is enough for prefix
        buffer.resize(size + 100);
        size = snprintf(&buffer[0], buffer.size(), "%04d-%02d-%02d %02d:%02d:%02d.%06ld, %s, %s ", tm_time.tm_year + 1900,
                        1 + tm_time.tm_mon, tm_time.tm_mday, tm_time.tm_hour,
                        tm_time.tm_min, tm_time.tm_sec, static_cast<long>(tval.tv_usec), ProcessId, SeverityName[s]);
        va_start(ap, fmt);
        size += vsnprintf(&buffer[size], buffer.size() - size, fmt, ap);
        va_end(ap);
        lock_guard<mutex> lock(LoggerMutex);
        dprintf(fd, "%s\n", &buffer[0]);
        return;
    } catch (const std::exception &e) {
        dprintf(fd, "%s:%d %s %s", __FILE__, __LINE__,
                "FATAL: get an unexpected exception:", e.what());
        throw;
    }
}

}
}

