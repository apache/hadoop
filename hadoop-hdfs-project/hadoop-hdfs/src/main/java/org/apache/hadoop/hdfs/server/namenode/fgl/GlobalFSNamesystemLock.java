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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystemLock;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class GlobalFSNamesystemLock implements FSNLockManager {

  private final FSNamesystemLock lock;

  public GlobalFSNamesystemLock(Configuration conf, MutableRatesWithAggregation aggregation) {
    this.lock = new FSNamesystemLock(conf, "FSN", aggregation);
  }

  @Override
  public void readLock(RwLockMode lockMode) {
    this.lock.readLock();
  }

  public void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException  {
    this.lock.readLockInterruptibly();
  }

  @Override
  public void readUnlock(RwLockMode lockMode, String opName) {
    this.lock.readUnlock(opName);
  }

  public void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.lock.readUnlock(opName, lockReportInfoSupplier);
  }

  @Override
  public void writeLock(RwLockMode lockMode) {
    this.lock.writeLock();
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName) {
    this.lock.writeUnlock(opName);
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    this.lock.writeUnlock(opName, suppressWriteLockReport);
  }

  public void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    this.lock.writeUnlock(opName, lockReportInfoSupplier);
  }

  @Override
  public void writeLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    this.lock.writeLockInterruptibly();
  }

  @Override
  public boolean hasWriteLock(RwLockMode lockMode) {
    return this.lock.isWriteLockedByCurrentThread();
  }

  @Override
  public boolean hasReadLock(RwLockMode lockMode) {
    return this.lock.getReadHoldCount() > 0 || hasWriteLock(lockMode);
  }

  @Override
  public int getReadHoldCount(RwLockMode lockMode) {
    return this.lock.getReadHoldCount();
  }

  @Override
  public int getQueueLength(RwLockMode lockMode) {
    return this.lock.getQueueLength();
  }

  @Override
  public long getNumOfReadLockLongHold(RwLockMode lockMode) {
    return this.lock.getNumOfReadLockLongHold();
  }

  @Override
  public long getNumOfWriteLockLongHold(RwLockMode lockMode) {
    return this.lock.getNumOfWriteLockLongHold();
  }

  @Override
  public boolean isMetricsEnabled() {
    return this.lock.isMetricsEnabled();
  }

  public void setMetricsEnabled(boolean metricsEnabled) {
    this.lock.setMetricsEnabled(metricsEnabled);
  }

  @Override
  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.lock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  @Override
  public long getReadLockReportingThresholdMs() {
    return this.lock.getReadLockReportingThresholdMs();
  }

  @Override
  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.lock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  @Override
  public long getWriteLockReportingThresholdMs() {
    return this.lock.getWriteLockReportingThresholdMs();
  }

  @Override
  public void setLockForTests(ReentrantReadWriteLock testLock) {
    this.lock.setLockForTests(testLock);
  }

  @Override
  public ReentrantReadWriteLock getLockForTests() {
    return this.lock.getLockForTests();
  }
}
