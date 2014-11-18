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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

/**
 * This represents the time duration of the reservation
 * 
 */
public class ReservationInterval implements Comparable<ReservationInterval> {

  private final long startTime;

  private final long endTime;

  public ReservationInterval(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Get the start time of the reservation interval
   * 
   * @return the startTime
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Get the end time of the reservation interval
   * 
   * @return the endTime
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * Returns whether the interval is active at the specified instant of time
   * 
   * @param tick the instance of the time to check
   * @return true if active, false otherwise
   */
  public boolean isOverlap(long tick) {
    return (startTime <= tick && tick <= endTime);
  }

  @Override
  public int compareTo(ReservationInterval anotherInterval) {
    long diff = 0;
    if (startTime == anotherInterval.getStartTime()) {
      diff = endTime - anotherInterval.getEndTime();
    } else {
      diff = startTime - anotherInterval.getStartTime();
    }
    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (endTime ^ (endTime >>> 32));
    result = prime * result + (int) (startTime ^ (startTime >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ReservationInterval)) {
      return false;
    }
    ReservationInterval other = (ReservationInterval) obj;
    if (endTime != other.endTime) {
      return false;
    }
    if (startTime != other.startTime) {
      return false;
    }
    return true;
  }

  public String toString() {
    return "[" + startTime + ", " + endTime + "]";
  }

}
