/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to track Volume IO stats for each HDDS Volume.
 */
public class VolumeIOStats {

  private final AtomicLong readBytes;
  private final AtomicLong readOpCount;
  private final AtomicLong writeBytes;
  private final AtomicLong writeOpCount;
  private final AtomicLong readTime;
  private final AtomicLong writeTime;

  public VolumeIOStats() {
    readBytes = new AtomicLong(0);
    readOpCount = new AtomicLong(0);
    writeBytes = new AtomicLong(0);
    writeOpCount = new AtomicLong(0);
    readTime = new AtomicLong(0);
    writeTime = new AtomicLong(0);
  }

  /**
   * Increment number of bytes read from the volume.
   * @param bytesRead
   */
  public void incReadBytes(long bytesRead) {
    readBytes.addAndGet(bytesRead);
  }

  /**
   * Increment the read operations performed on the volume.
   */
  public void incReadOpCount() {
    readOpCount.incrementAndGet();
  }

  /**
   * Increment number of bytes written on to the volume.
   * @param bytesWritten
   */
  public void incWriteBytes(long bytesWritten) {
    writeBytes.addAndGet(bytesWritten);
  }

  /**
   * Increment the write operations performed on the volume.
   */
  public void incWriteOpCount() {
    writeOpCount.incrementAndGet();
  }

  /**
   * Increment the time taken by read operation on the volume.
   * @param time
   */
  public void incReadTime(long time) {
    readTime.addAndGet(time);
  }

  /**
   * Increment the time taken by write operation on the volume.
   * @param time
   */
  public void incWriteTime(long time) {
    writeTime.addAndGet(time);
  }

  /**
   * Returns total number of bytes read from the volume.
   * @return long
   */
  public long getReadBytes() {
    return readBytes.get();
  }

  /**
   * Returns total number of bytes written to the volume.
   * @return long
   */
  public long getWriteBytes() {
    return writeBytes.get();
  }

  /**
   * Returns total number of read operations performed on the volume.
   * @return long
   */
  public long getReadOpCount() {
    return readOpCount.get();
  }

  /**
   * Returns total number of write operations performed on the volume.
   * @return long
   */
  public long getWriteOpCount() {
    return writeOpCount.get();
  }

  /**
   * Returns total read operations time on the volume.
   * @return long
   */
  public long getReadTime() {
    return readTime.get();
  }

  /**
   * Returns total write operations time on the volume.
   * @return long
   */
  public long getWriteTime() {
    return writeTime.get();
  }


}
