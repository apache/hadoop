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

#include "commons.h"
#include "lib/Combiner.h"
#include "test_commons.h"
#include <iostream>

namespace NativeTask {

TEST(TrackingCollector, read) {
  const std::string GROUP("group");
  const std::string KEY("key");
  Counter * counter = new Counter(GROUP, KEY);
  Collector * collector = new Collector();
  TrackingCollector tracking(collector, counter);
  tracking.collect(NULL, 0, NULL, 0);
  ASSERT_EQ(1, counter->get());
  delete counter;
  delete collector;
}
} /* namespace NativeTask */
