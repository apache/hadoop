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

package org.apache.hadoop.ozone.container.common.impl;

/**
 * Storage location stats of datanodes that provide back store for containers.
 *
 */
public class StorageLocationReport {
  public static final StorageLocationReport[] EMPTY_ARRAY = {};

  private final String id;
  private final boolean failed;
  private final long capacity;
  private final long scmUsed;
  private final long remaining;

  public StorageLocationReport(String id, boolean failed,
      long capacity, long scmUsed, long remaining) {
    this.id = id;
    this.failed = failed;
    this.capacity = capacity;
    this.scmUsed = scmUsed;
    this.remaining = remaining;
  }

  public String getId() {
    return id;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getScmUsed() {
    return scmUsed;
  }

  public long getRemaining() {
    return remaining;
  }

}
