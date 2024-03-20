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

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.RateLimiting;
import org.apache.hadoop.util.RateLimitingFactory;

/**
 * An optional interface for classes that provide rate limiters.
 * For a filesystem source, the operation name SHOULD be one of
 * those listed in
 * {@link org.apache.hadoop.fs.statistics.StoreStatisticNames}
 * if the operation is listed there.
 * <p>
 * This interfaces is intended to be exported by FileSystems so that
 * applications wishing to perform bulk operations may request access
 * to a rate limiter <i>which is shared across all threads interacting
 * with the store.</i>.
 * That is: the rate limiting is global to the specific instance of the
 * object implementing this interface.
 * <p>
 * It is not expected to be shared with other instances of the same
 * class, or across processes.
 * <p>
 * This means it is primarily of benefit when limiting bulk operations
 * which can overload an (object) store from a small pool of threads.
 * Examples of this can include:
 * <ul>
 *   <li>Bulk delete operations</li>
 *   <li>Bulk rename operations</li>
 *   <li>Completing many in-progress uploads</li>
 *   <li>Deep and wide recursive treewalks</li>
 *   <li>Reading/prefetching many blocks within a file</li>
 * </ul>
 * In cluster applications, it is more likely that rate limiting is
 * useful during job commit operations, especially in processes such
 * a Spark Drivers, which may be committing work from multiple jobs over
 * a short period of time.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface RateLimiterSource {

  RateLimiting acquireReadRateLimiter(String operation);

  RateLimiting acquireWriteRateLimiter(String operation);

  /**
   * Get a rate limiter source which has no rate limiting.
   * @return a rate limiter source which has no rate limiting.
   */
  static RateLimiterSource unlimited() {
    return new RateLimiterSource() {
      @Override
      public RateLimiting acquireReadRateLimiter(String operation) {
        return RateLimitingFactory.unlimitedRate();
      }

      @Override
      public RateLimiting acquireWriteRateLimiter(String operation) {
        return RateLimitingFactory.unlimitedRate();
      }
    };
  }
}
