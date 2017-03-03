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

#ifndef LOG_H_
#define LOG_H_

#include <stdio.h>
#include <time.h>

namespace NativeTask {

#define PRINT_LOG

#ifdef PRINT_LOG

extern FILE * LOG_DEVICE;
#define LOG(_fmt_, args...)   if (LOG_DEVICE) { \
    time_t log_timer; struct tm log_tm; \
    time(&log_timer); localtime_r(&log_timer, &log_tm); \
    fprintf(LOG_DEVICE, "%02d/%02d/%02d %02d:%02d:%02d INFO " _fmt_ "\n", \
    log_tm.tm_year%100, log_tm.tm_mon+1, log_tm.tm_mday, \
    log_tm.tm_hour, log_tm.tm_min, log_tm.tm_sec, \
    ##args);}

#else

#define LOG(_fmt_, args...)

#endif

} // namespace NativeTask

#endif /* LOG_H_ */
