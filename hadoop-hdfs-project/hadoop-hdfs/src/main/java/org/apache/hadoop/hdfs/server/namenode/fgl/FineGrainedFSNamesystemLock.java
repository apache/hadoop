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

/**
 * Splitting the global FSN lock into FSLock and BMLock.
 * FSLock is used to protect directory tree-related operations.
 * BMLock is used to protect block-related and dn-related operations.
 * The lock order should be: FSLock,BMLock.
 */
public class FineGrainedFSNamesystemLock implements FSNLockManager {
  private final FSNamesystemLock fsLock;
  private final FSNamesystemLock bmLock;

  public FineGrainedFSNamesystemLock(Configuration conf, MutableRatesWithAggregation aggregation) {
    this.fsLock = new FSNamesystemLock(conf, "FS", aggregation);
    this.bmLock = new FSNamesystemLock(conf, "BM", aggregation);
  }

  @Override
  public void readLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.readLock();
      this.bmLock.readLock();
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readLock();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readLock();
    }
  }

  public void readLockInterruptibly(RwLockMode lockMode) throws InterruptedException  {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.readLockInterruptibly();
      try {
        this.bmLock.readLockInterruptibly();
      } catch (InterruptedException e) {
        // The held FSLock should be released if the current thread is interrupted
        // while acquiring the BMLock.
        this.fsLock.readUnlock("BMReadLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readLockInterruptibly();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readLockInterruptibly();
    }
  }

  @Override
  public void readUnlock(RwLockMode lockMode, String opName) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName);
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readUnlock(opName);
    }
  }

  public void readUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
    }
  }

  @Override
  public void writeLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.writeLock();
      this.bmLock.writeLock();
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeLock();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeLock();
    }
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName);
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName);
    }
  }

  @Override
  public void writeUnlock(RwLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
    }
  }

  public void writeUnlock(RwLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
    }
  }

  @Override
  public void writeLockInterruptibly(RwLockMode lockMode)
      throws InterruptedException {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      this.fsLock.writeLockInterruptibly();
      try {
        this.bmLock.writeLockInterruptibly();
      } catch (InterruptedException e) {
        // The held FSLock should be released if the current thread is interrupted
        // while acquiring the BMLock.
        this.fsLock.writeUnlock("BMWriteLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(RwLockMode.FS)) {
      this.fsLock.writeLockInterruptibly();
    } else if (lockMode.equals(RwLockMode.BM)) {
      this.bmLock.writeLockInterruptibly();
    }
  }

  @Override
  public boolean hasWriteLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return this.fsLock.isWriteLockedByCurrentThread()
          && this.bmLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  @Override
  public boolean hasReadLock(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return hasWriteLock(RwLockMode.GLOBAL) ||
          (this.fsLock.getReadHoldCount() > 0 && this.bmLock.getReadHoldCount() > 0);
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getReadHoldCount() > 0 || this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getReadHoldCount() > 0 || this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  /**
   * This method is only used for ComputeDirectoryContentSummary.
   * For the GLOBAL mode, just return the FSLock's ReadHoldCount.
   */
  @Override
  public int getReadHoldCount(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getReadHoldCount();
    }
    return -1;
  }

  @Override
  public int getQueueLength(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getQueueLength();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getQueueLength();
    }
    return -1;
  }

  @Override
  public long getNumOfReadLockLongHold(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getNumOfReadLockLongHold();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getNumOfReadLockLongHold();
    }
    return -1;
  }

  @Override
  public long getNumOfWriteLockLongHold(RwLockMode lockMode) {
    if (lockMode.equals(RwLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(RwLockMode.FS)) {
      return this.fsLock.getNumOfWriteLockLongHold();
    } else if (lockMode.equals(RwLockMode.BM)) {
      return this.bmLock.getNumOfWriteLockLongHold();
    }
    return -1;
  }

  @Override
  public boolean isMetricsEnabled() {
    return this.fsLock.isMetricsEnabled();
  }

  public void setMetricsEnabled(boolean metricsEnabled) {
    this.fsLock.setMetricsEnabled(metricsEnabled);
    this.bmLock.setMetricsEnabled(metricsEnabled);
  }

  @Override
  public void setReadLockReportingThresholdMs(long readLockReportingThresholdMs) {
    this.fsLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
    this.bmLock.setReadLockReportingThresholdMs(readLockReportingThresholdMs);
  }

  @Override
  public long getReadLockReportingThresholdMs() {
    return this.fsLock.getReadLockReportingThresholdMs();
  }

  @Override
  public void setWriteLockReportingThresholdMs(long writeLockReportingThresholdMs) {
    this.fsLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
    this.bmLock.setWriteLockReportingThresholdMs(writeLockReportingThresholdMs);
  }

  @Override
  public long getWriteLockReportingThresholdMs() {
    return this.fsLock.getWriteLockReportingThresholdMs();
  }

  @Override
  public void setLockForTests(ReentrantReadWriteLock lock) {
    throw new UnsupportedOperationException("SetLockTests is unsupported");
  }

  @Override
  public ReentrantReadWriteLock getLockForTests() {
    throw new UnsupportedOperationException("SetLockTests is unsupported");
  }
}
