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
package org.apache.hadoop.hdfs;

/**
 * A utility class that maintains statistics for reading.
 */
public class ReadStatistics {
  private long totalBytesRead;
  private long totalLocalBytesRead;
  private long totalShortCircuitBytesRead;
  private long totalZeroCopyBytesRead;

  public ReadStatistics() {
    clear();
  }

  public ReadStatistics(ReadStatistics rhs) {
    this.totalBytesRead = rhs.getTotalBytesRead();
    this.totalLocalBytesRead = rhs.getTotalLocalBytesRead();
    this.totalShortCircuitBytesRead = rhs.getTotalShortCircuitBytesRead();
    this.totalZeroCopyBytesRead = rhs.getTotalZeroCopyBytesRead();
  }

  /**
   * @return The total bytes read.  This will always be at least as
   * high as the other numbers, since it includes all of them.
   */
  public synchronized long getTotalBytesRead() {
    return totalBytesRead;
  }

  /**
   * @return The total local bytes read.  This will always be at least
   * as high as totalShortCircuitBytesRead, since all short-circuit
   * reads are also local.
   */
  public synchronized long getTotalLocalBytesRead() {
    return totalLocalBytesRead;
  }

  /**
   * @return The total short-circuit local bytes read.
   */
  public synchronized long getTotalShortCircuitBytesRead() {
    return totalShortCircuitBytesRead;
  }

  /**
   * @return The total number of zero-copy bytes read.
   */
  public synchronized long getTotalZeroCopyBytesRead() {
    return totalZeroCopyBytesRead;
  }

  /**
   * @return The total number of bytes read which were not local.
   */
  public synchronized long getRemoteBytesRead() {
    return totalBytesRead - totalLocalBytesRead;
  }

  public synchronized void addRemoteBytes(long amt) {
    this.totalBytesRead += amt;
  }

  public synchronized void addLocalBytes(long amt) {
    this.totalBytesRead += amt;
    this.totalLocalBytesRead += amt;
  }

  public synchronized void addShortCircuitBytes(long amt) {
    this.totalBytesRead += amt;
    this.totalLocalBytesRead += amt;
    this.totalShortCircuitBytesRead += amt;
  }

  public synchronized void addZeroCopyBytes(long amt) {
    this.totalBytesRead += amt;
    this.totalLocalBytesRead += amt;
    this.totalShortCircuitBytesRead += amt;
    this.totalZeroCopyBytesRead += amt;
  }

  public synchronized void clear() {
    this.totalBytesRead = 0;
    this.totalLocalBytesRead = 0;
    this.totalShortCircuitBytesRead = 0;
    this.totalZeroCopyBytesRead = 0;
  }
}
