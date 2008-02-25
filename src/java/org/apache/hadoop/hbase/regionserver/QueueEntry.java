/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Delayed;

/** Queue entry passed to flusher, compactor and splitter threads */
class QueueEntry implements Delayed {
  private final HRegion region;
  private long expirationTime;

  QueueEntry(HRegion region, long expirationTime) {
    this.region = region;
    this.expirationTime = expirationTime;
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    QueueEntry other = (QueueEntry) o;
    return this.hashCode() == other.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return this.region.getRegionInfo().hashCode();
  }

  /** {@inheritDoc} */
  public long getDelay(TimeUnit unit) {
    return unit.convert(this.expirationTime - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
  }

  /** {@inheritDoc} */
  public int compareTo(Delayed o) {
    long delta = this.getDelay(TimeUnit.MILLISECONDS) -
      o.getDelay(TimeUnit.MILLISECONDS);

    int value = 0;
    if (delta > 0) {
      value = 1;
      
    } else if (delta < 0) {
      value = -1;
    }
    return value;
  }

  /** @return the region */
  public HRegion getRegion() {
    return region;
  }

  /** @param expirationTime the expirationTime to set */
  public void setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
  }
}