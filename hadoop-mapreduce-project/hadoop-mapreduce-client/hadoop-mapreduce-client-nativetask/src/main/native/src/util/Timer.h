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

#ifndef TIMER_H_
#define TIMER_H_

#include <stdint.h>
#include <stdio.h>
#include <string>

namespace NativeTask {

using std::string;

class Timer {
protected:
  uint64_t _last;
public:
  Timer();
  ~Timer();

  uint64_t last();

  uint64_t now();

  void reset();

  string getInterval(const char * msg);

  string getSpeed(const char * msg, uint64_t size);

  string getSpeed2(const char * msg, uint64_t size1, uint64_t size2);

  string getSpeedM(const char * msg, uint64_t size);

  string getSpeedM2(const char * msg, uint64_t size1, uint64_t size2);
};

} // namespace NativeTask

#endif /* TIMER_H_ */
