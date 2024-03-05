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
    this.fsLock = new FSNamesystemLock(conf, aggregation);
    this.bmLock = new FSNamesystemLock(conf, aggregation);
  }

  @Override
  public void readLock(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.fsLock.readLock();
      this.bmLock.readLock();
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.readLock();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.readLock();
    }
  }

  public void readLockInterruptibly(FSNamesystemLockMode lockMode) throws InterruptedException  {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.fsLock.readLockInterruptibly();
      try {
        this.bmLock.readLockInterruptibly();
      } catch (InterruptedException e) {
        // The held FSLock should be released if the current thread is interrupted
        // while acquiring the BMLock.
        this.fsLock.readUnlock("BMReadLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.readLockInterruptibly();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.readLockInterruptibly();
    }
  }

  @Override
  public void readUnlock(FSNamesystemLockMode lockMode, String opName) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName);
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.readUnlock(opName);
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.readUnlock(opName);
    }
  }

  public void readUnlock(FSNamesystemLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.readUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.readUnlock(opName, lockReportInfoSupplier);
    }
  }

  @Override
  public void writeLock(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.fsLock.writeLock();
      this.bmLock.writeLock();
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.writeLock();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.writeLock();
    }
  }

  @Override
  public void writeUnlock(FSNamesystemLockMode lockMode, String opName) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName);
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.writeUnlock(opName);
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.writeUnlock(opName);
    }
  }

  @Override
  public void writeUnlock(FSNamesystemLockMode lockMode, String opName,
      boolean suppressWriteLockReport) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.writeUnlock(opName, suppressWriteLockReport);
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.writeUnlock(opName, suppressWriteLockReport);
    }
  }

  public void writeUnlock(FSNamesystemLockMode lockMode, String opName,
      Supplier<String> lockReportInfoSupplier) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.writeUnlock(opName, lockReportInfoSupplier);
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.writeUnlock(opName, lockReportInfoSupplier);
    }
  }

  @Override
  public void writeLockInterruptibly(FSNamesystemLockMode lockMode)
      throws InterruptedException {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      this.fsLock.writeLockInterruptibly();
      try {
        this.bmLock.writeLockInterruptibly();
      } catch (InterruptedException e) {
        // The held FSLock should be released if the current thread is interrupted
        // while acquiring the BMLock.
        this.fsLock.writeUnlock("BMWriteLockInterruptiblyFailed");
        throw e;
      }
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      this.fsLock.writeLockInterruptibly();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      this.bmLock.writeLockInterruptibly();
    }
  }

  @Override
  public boolean hasWriteLock(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      if (this.fsLock.isWriteLockedByCurrentThread()) {
        // The bm writeLock should be held by the current thread.
        assert this.bmLock.isWriteLockedByCurrentThread();
        return true;
      } else {
        // The bm writeLock should not be held by the current thread.
        assert !this.bmLock.isWriteLockedByCurrentThread();
        return false;
      }
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      return this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  @Override
  public boolean hasReadLock(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      if (hasWriteLock(FSNamesystemLockMode.GLOBAL)) {
        return true;
      } else if (this.fsLock.getReadHoldCount() > 0) {
        // The bm readLock should be held by the current thread.
        assert this.bmLock.getReadHoldCount() > 0;
        return true;
      } else {
        // The bm readLock should not be held by the current thread.
        assert this.bmLock.getReadHoldCount() <= 0;
        return false;
      }
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.getReadHoldCount() > 0 || this.fsLock.isWriteLockedByCurrentThread();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      return this.bmLock.getReadHoldCount() > 0 || this.bmLock.isWriteLockedByCurrentThread();
    }
    return false;
  }

  @Override
  /**
   * This method is only used for ComputeDirectoryContentSummary.
   * For the GLOBAL mode, just return the FSLock's ReadHoldCount.
   */
  public int getReadHoldCount(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.getReadHoldCount();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      return this.bmLock.getReadHoldCount();
    }
    return -1;
  }

  @Override
  public int getQueueLength(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.getQueueLength();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      return this.bmLock.getQueueLength();
    }
    return -1;
  }

  @Override
  public long getNumOfReadLockLongHold(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.getNumOfReadLockLongHold();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
      return this.bmLock.getNumOfReadLockLongHold();
    }
    return -1;
  }

  @Override
  public long getNumOfWriteLockLongHold(FSNamesystemLockMode lockMode) {
    if (lockMode.equals(FSNamesystemLockMode.GLOBAL)) {
      return -1;
    } else if (lockMode.equals(FSNamesystemLockMode.FS)) {
      return this.fsLock.getNumOfWriteLockLongHold();
    } else if (lockMode.equals(FSNamesystemLockMode.BM)) {
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
