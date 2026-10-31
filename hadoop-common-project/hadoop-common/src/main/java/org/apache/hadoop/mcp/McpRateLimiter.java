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

package org.apache.hadoop.mcp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Time;

/**
 * Fixed-window limit on tool calls: at most {@code limit} calls per {@code windowMs}.
 * A limit of 0 or less disables limiting. Thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpRateLimiter {

  private final int limit;
  private final long windowMs;
  private long windowStartMs;
  private int count;

  McpRateLimiter(int limit, long windowMs) {
    Preconditions.checkArgument(limit <= 0 || windowMs > 0,
        "windowMs must be positive: %s", windowMs);
    this.limit = limit;
    this.windowMs = windowMs;
    this.windowStartMs = Time.monotonicNow();
  }

  /** @return true if the call is allowed, false if the limit for this window is reached */
  synchronized boolean tryAcquire() {
    if (limit <= 0) {
      return true;
    }
    long now = Time.monotonicNow();
    if (now - windowStartMs >= windowMs) {
      windowStartMs = now;
      count = 0;
    }
    if (count >= limit) {
      return false;
    }
    count++;
    return true;
  }
}
