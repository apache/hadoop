/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HDDS Id generator.
 */
public final class HddsIdFactory {
  private HddsIdFactory() {
  }

  private static final AtomicLong LONG_COUNTER = new AtomicLong(
      System.currentTimeMillis());

  /**
   * Returns an incrementing long. This class doesn't
   * persist initial value for long Id's, so incremental id's after restart
   * may collide with previously generated Id's.
   *
   * @return long
   */
  public static long getLongId() {
    return LONG_COUNTER.incrementAndGet();
  }

  /**
   * Returns a uuid.
   *
   * @return UUID.
   */
  public static UUID getUUId() {
    return UUID.randomUUID();
  }

}
