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

package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.yarn.event.Event;

class MockEvent implements Event<MockEventType> {
  private static final Random RANDOM = new Random();

  private final MockEventType type = Math.random() < 0.5
      ? MockEventType.TYPE_1
      : MockEventType.TYPE_2;

  private final long timestamp = System.currentTimeMillis();

  private final String lockKey = Arrays.asList(
      "APP1",
      "APP2",
      null
  ).get(RANDOM.nextInt(3));

  @Override
  public MockEventType getType() {
    return type;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getLockKey() {
    return lockKey;
  }
}
