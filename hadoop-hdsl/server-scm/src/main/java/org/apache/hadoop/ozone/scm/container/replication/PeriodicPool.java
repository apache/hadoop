/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.container.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodic pool is a pool with a time stamp, this allows us to process pools
 * based on a cyclic clock.
 */
public class PeriodicPool implements Comparable<PeriodicPool> {
  private final String poolName;
  private long lastProcessedTime;
  private AtomicLong totalProcessedCount;

  /**
   * Constructs a periodic pool.
   *
   * @param poolName - Name of the pool
   */
  public PeriodicPool(String poolName) {
    this.poolName = poolName;
    lastProcessedTime = 0;
    totalProcessedCount = new AtomicLong(0);
  }

  /**
   * Get pool Name.
   * @return PoolName
   */
  public String getPoolName() {
    return poolName;
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(PeriodicPool o) {
    return Long.compare(this.lastProcessedTime, o.lastProcessedTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PeriodicPool that = (PeriodicPool) o;

    return poolName.equals(that.poolName);
  }

  @Override
  public int hashCode() {
    return poolName.hashCode();
  }

  /**
   * Returns the Total Times we have processed this pool.
   *
   * @return processed count.
   */
  public long getTotalProcessedCount() {
    return totalProcessedCount.get();
  }

  /**
   * Gets the last time we processed this pool.
   * @return time in milliseconds
   */
  public long getLastProcessedTime() {
    return this.lastProcessedTime;
  }


  /**
   * Sets the last processed time.
   *
   * @param lastProcessedTime - Long in milliseconds.
   */

  public void setLastProcessedTime(long lastProcessedTime) {
    this.lastProcessedTime = lastProcessedTime;
  }

  /*
   * Increments the total processed count.
   */
  public void incTotalProcessedCount() {
    this.totalProcessedCount.incrementAndGet();
  }
}