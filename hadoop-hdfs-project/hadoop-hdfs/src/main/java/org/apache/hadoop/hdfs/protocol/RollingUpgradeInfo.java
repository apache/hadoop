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
package org.apache.hadoop.hdfs.protocol;

import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Rolling upgrade information
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RollingUpgradeInfo extends RollingUpgradeStatus {
  private final long startTime;
  private long finalizeTime;
  private boolean createdRollbackImages;
  
  public RollingUpgradeInfo(String blockPoolId, boolean createdRollbackImages,
      long startTime, long finalizeTime) {
    super(blockPoolId, finalizeTime != 0);
    this.createdRollbackImages = createdRollbackImages;
    this.startTime = startTime;
    this.finalizeTime = finalizeTime;
  }
  
  public boolean createdRollbackImages() {
    return createdRollbackImages;
  }

  public void setCreatedRollbackImages(boolean created) {
    this.createdRollbackImages = created;
  }

  public boolean isStarted() {
    return startTime != 0;
  }
  
  /** @return The rolling upgrade starting time. */
  public long getStartTime() {
    return startTime;
  }

  @Override
  public boolean isFinalized() {
    return finalizeTime != 0;
  }

  /**
   * Finalize the upgrade if not already finalized
   * @param finalizeTime
   */
  public void finalize(long finalizeTime) {
    if (finalizeTime != 0) {
      this.finalizeTime = finalizeTime;
      createdRollbackImages = false;
    }
  }

  public long getFinalizeTime() {
    return finalizeTime;
  }

  @Override
  public int hashCode() {
    //only use lower 32 bits
    return super.hashCode() ^ (int)startTime ^ (int)finalizeTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof RollingUpgradeInfo)) {
      return false;
    }
    final RollingUpgradeInfo that = (RollingUpgradeInfo)obj;
    return super.equals(that)
        && this.startTime == that.startTime
        && this.finalizeTime == that.finalizeTime;
  }

  @Override
  public String toString() {
    return super.toString()
      +  "\n     Start Time: " + (startTime == 0? "<NOT STARTED>": timestamp2String(startTime))
      +  "\n  Finalize Time: " + (finalizeTime == 0? "<NOT FINALIZED>": timestamp2String(finalizeTime));
  }
  
  private static String timestamp2String(long timestamp) {
    return new Date(timestamp) + " (=" + timestamp + ")";
  }

  public static class Bean {
    private final String blockPoolId;
    private final long startTime;
    private final long finalizeTime;
    private final boolean createdRollbackImages;

    public Bean(RollingUpgradeInfo f) {
      this.blockPoolId = f.getBlockPoolId();
      this.startTime = f.startTime;
      this.finalizeTime = f.finalizeTime;
      this.createdRollbackImages = f.createdRollbackImages();
    }

    public String getBlockPoolId() {
      return blockPoolId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getFinalizeTime() {
      return finalizeTime;
    }

    public boolean isCreatedRollbackImages() {
      return createdRollbackImages;
    }
  }
}
