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

#include <time.h>
#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/Timer.h"

namespace NativeTask {

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>

static uint64_t clock_get() {
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  return 1000000000ULL * mts.tv_sec + mts.tv_nsec;
}

#else

static uint64_t clock_get() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return 1000000000 * ts.tv_sec + ts.tv_nsec;
}

#endif

Timer::Timer() {
  _last = clock_get();
}

Timer::~Timer() {
}

uint64_t Timer::last() {
  return _last;
}

uint64_t Timer::now() {
  return clock_get();
}

void Timer::reset() {
  _last = clock_get();
}

string Timer::getInterval(const char * msg) {
  uint64_t now = clock_get();
  uint64_t interval = now - _last;
  _last = now;
  return StringUtil::Format("%s time: %.5lfs", msg, (double)interval / 1000000000.0);
}

string Timer::getSpeed(const char * msg, uint64_t size) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double speed = size / interval;
  return StringUtil::Format("%s time:\t %3.5lfs size: %10llu speed: %12.0lf/s", msg, interval, size,
      speed);
}

string Timer::getSpeedM(const char * msg, uint64_t size) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double msize = size / (1024.0 * 1024.0);
  double speed = msize / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %.3lfM speed: %.2lfM/s", msg, interval, msize,
      speed);
}

string Timer::getSpeed2(const char * msg, uint64_t size1, uint64_t size2) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double speed1 = size1 / interval;
  double speed2 = size2 / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %llu/%llu speed: %.0lf/%.0lf", msg, interval,
      size1, size2, speed1, speed2);
}

string Timer::getSpeedM2(const char * msg, uint64_t size1, uint64_t size2) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double msize1 = size1 / (1024.0 * 1024.0);
  double speed1 = msize1 / interval;
  double msize2 = size2 / (1024.0 * 1024.0);
  double speed2 = msize2 / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %.3lfM/%.3lfM speed: %.2lfM/%.2lfM", msg,
      interval, msize1, msize2, speed1, speed2);
}

} // namespace NativeTask
