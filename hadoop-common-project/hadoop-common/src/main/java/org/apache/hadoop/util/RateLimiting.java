/*
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

package org.apache.hadoop.util;

import java.time.Duration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Minimal subset of google rate limiter class.
 * Can be used to throttle use of object stores where excess load
 * will trigger cluster-wide throttling, backoff etc. and so collapse
 * performance.
 * The time waited is returned as a Duration type.
 * The google rate limiter implements this by allowing a caller to ask for
 * more capacity than is available. This will be granted
 * but the subsequent request will be blocked if the bucket of
 * capacity hasn't let refilled to the point where there is
 * capacity again.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface RateLimiting {

  /**
   * Acquire rate limiter capacity.
   * If there is not enough space, the permits will be acquired,
   * but the subsequent call will block until the capacity has been
   * refilled.
   * @param requestedCapacity capacity to acquire.
   * @return time spent waiting for output.
   */
  Duration acquire(int requestedCapacity);

}
