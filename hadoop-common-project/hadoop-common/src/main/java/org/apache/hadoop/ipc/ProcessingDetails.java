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

package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Stores the times that a call takes to be processed through each step.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class ProcessingDetails {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProcessingDetails.class);
  private final TimeUnit valueTimeUnit;

  /**
   * The different stages to track the time of.
   */
  public enum Timing {
    ENQUEUE,          // time for reader to insert in call queue.
    QUEUE,            // time in the call queue.
    HANDLER,          // handler overhead not spent in processing/response.
    PROCESSING,       // time handler spent processing the call. always equal to
                      // lock_free + lock_wait + lock_shared + lock_exclusive
    LOCKFREE,         // processing with no lock.
    LOCKWAIT,         // processing while waiting for lock.
    LOCKSHARED,       // processing with a read lock.
    LOCKEXCLUSIVE,    // processing with a write lock.
    RESPONSE;         // time to encode and send response.
  }

  private long[] timings = new long[Timing.values().length];

  ProcessingDetails(TimeUnit timeUnit) {
    this.valueTimeUnit = timeUnit;
  }

  public long get(Timing type) {
    // When using nanoTime to fetch timing information, it is possible to see
    // time "move backward" slightly under unusual/rare circumstances. To avoid
    // displaying a confusing number, round such timings to 0 here.
    long ret = timings[type.ordinal()];
    return ret < 0 ? 0 : ret;
  }

  public long get(Timing type, TimeUnit timeUnit) {
    return timeUnit.convert(get(type), valueTimeUnit);
  }

  public void set(Timing type, long value) {
    timings[type.ordinal()] = value;
  }

  public void set(Timing type, long value, TimeUnit timeUnit) {
    set(type, valueTimeUnit.convert(value, timeUnit));
  }

  public void add(Timing type, long value, TimeUnit timeUnit) {
    timings[type.ordinal()] += valueTimeUnit.convert(value, timeUnit);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    for (Timing type : Timing.values()) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append(type.name().toLowerCase())
          .append("Time=").append(get(type));
    }
    return sb.toString();
  }
}
