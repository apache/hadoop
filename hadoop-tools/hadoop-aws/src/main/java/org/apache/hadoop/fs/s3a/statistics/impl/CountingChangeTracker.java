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

package org.apache.hadoop.fs.s3a.statistics.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.s3a.statistics.ChangeTrackerStatistics;

/**
 * A change tracker which increments an atomic long.
 */
public class CountingChangeTracker implements
    ChangeTrackerStatistics {

  /**
   * The counter which is updated on every mismatch.
   */
  private final AtomicLong counter;

  public CountingChangeTracker(final AtomicLong counter) {
    this.counter = counter;
  }

  public CountingChangeTracker() {
    this(new AtomicLong());
  }

  @Override
  public void versionMismatchError() {
    counter.incrementAndGet();
  }

  @Override
  public long getVersionMismatches() {
    return counter.get();
  }
}
