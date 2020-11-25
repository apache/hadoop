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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * PendingRecoveryBlocks tracks recovery attempts for each block and their
 * timeouts to ensure we do not have multiple recoveries at the same time
 * and retry only after the timeout for a recovery has expired.
 */
class PendingRecoveryBlocks {
  private static final Logger LOG = BlockManager.LOG;

  /** List of recovery attempts per block and the time they expire. */
  private final LightWeightHashSet<BlockRecoveryAttempt> recoveryTimeouts =
      new LightWeightHashSet<>();

  /** The timeout for issuing a block recovery again.
   * (it should be larger than the time to recover a block)
   */
  private long recoveryTimeoutInterval;

  PendingRecoveryBlocks(long timeout) {
    this.recoveryTimeoutInterval = timeout;
  }

  /**
   * Remove recovery attempt for the given block.
   * @param block whose recovery attempt to remove.
   */
  synchronized void remove(BlockInfo block) {
    recoveryTimeouts.remove(new BlockRecoveryAttempt(block));
  }

  /**
   * Checks whether a recovery attempt has been made for the given block.
   * If so, checks whether that attempt has timed out.
   * @param block block for which recovery is being attempted
   * @return true if no recovery attempt has been made or
   *         the previous attempt timed out
   */
  synchronized boolean add(BlockInfo block) {
    boolean added = false;
    long curTime = getTime();
    BlockRecoveryAttempt recoveryAttempt =
        recoveryTimeouts.getElement(new BlockRecoveryAttempt(block));

    if (recoveryAttempt == null) {
      BlockRecoveryAttempt newAttempt = new BlockRecoveryAttempt(
          block, curTime + recoveryTimeoutInterval);
      added = recoveryTimeouts.add(newAttempt);
    } else if (recoveryAttempt.hasTimedOut(curTime)) {
      // Previous attempt timed out, reset the timeout
      recoveryAttempt.setTimeout(curTime + recoveryTimeoutInterval);
      added = true;
    } else {
      long timeoutIn = TimeUnit.MILLISECONDS.toSeconds(
          recoveryAttempt.timeoutAt - curTime);
      LOG.info("Block recovery attempt for " + block + " rejected, as the " +
          "previous attempt times out in " + timeoutIn + " seconds.");
    }
    return added;
  }

  /**
   * Check whether the given block is under recovery.
   * @param b block for which to check
   * @return true if the given block is being recovered
   */
  synchronized boolean isUnderRecovery(BlockInfo b) {
    BlockRecoveryAttempt recoveryAttempt =
        recoveryTimeouts.getElement(new BlockRecoveryAttempt(b));
    return recoveryAttempt != null;
  }

  long getTime() {
    return Time.monotonicNow();
  }

  @VisibleForTesting
  synchronized void setRecoveryTimeoutInterval(long recoveryTimeoutInterval) {
    this.recoveryTimeoutInterval = recoveryTimeoutInterval;
  }

  /**
   * Tracks timeout for block recovery attempt of a given block.
   */
  private static class BlockRecoveryAttempt {
    private final BlockInfo blockInfo;
    private long timeoutAt;

    private BlockRecoveryAttempt(BlockInfo blockInfo) {
      this(blockInfo, 0);
    }

    BlockRecoveryAttempt(BlockInfo blockInfo, long timeoutAt) {
      this.blockInfo = blockInfo;
      this.timeoutAt = timeoutAt;
    }

    boolean hasTimedOut(long currentTime) {
      return currentTime > timeoutAt;
    }

    void setTimeout(long newTimeoutAt) {
      this.timeoutAt = newTimeoutAt;
    }

    @Override
    public int hashCode() {
      return blockInfo.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof BlockRecoveryAttempt) {
        return this.blockInfo.equals(((BlockRecoveryAttempt) obj).blockInfo);
      }
      return false;
    }
  }
}
