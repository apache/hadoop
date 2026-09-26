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

package org.apache.hadoop.fs.s3a.api;

import java.time.Duration;

/**
 * Interface for specific rate limiting of read and write operations.
 */
public interface IORateLimiting {

  /**
   * Acquire write capacity for operations.
   * This should be done within retry loops.
   * @param capacity capacity to acquire.
   * @return time spent waiting for output.
   */
  Duration acquireWriteCapacity(int capacity);

  /**
   * Acquire read capacity for operations.
   * This should be done within retry loops.
   * @param capacity capacity to acquire.
   * @return time spent waiting for output.
   */
  Duration acquireReadCapacity(int capacity);

}
