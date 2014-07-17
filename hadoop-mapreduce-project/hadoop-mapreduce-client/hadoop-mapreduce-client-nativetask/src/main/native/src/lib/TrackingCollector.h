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

#ifndef TRACKING_COLLECTOR_H
#define TRACKING_COLLECTOR_H

#include <stdint.h>
#include <string>

namespace NativeTask {

class TrackingCollector : public Collector {
protected:
  Collector * _collector;
  Counter * _counter;
public:
  TrackingCollector(Collector * collector, Counter * counter)
      : _collector(collector), _counter(counter) {
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
    _counter->increase();
    _collector->collect(key, keyLen, value, valueLen);
  }

  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen,
      int32_t partition) {
    _counter->increase();
    _collector->collect(key, keyLen, value, valueLen, partition);
  }
};

} //namespace NativeTask

#endif //TRACKING_COLLECTOR_H
