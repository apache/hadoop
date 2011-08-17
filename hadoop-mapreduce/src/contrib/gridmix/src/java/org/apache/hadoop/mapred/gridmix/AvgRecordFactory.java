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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Given byte and record targets, emit roughly equal-sized records satisfying
 * the contract.
 */
class AvgRecordFactory extends RecordFactory {

  /**
   * Percentage of record for key data.
   */
  public static final String GRIDMIX_KEY_FRC = "gridmix.key.fraction";
  public static final String GRIDMIX_MISSING_REC_SIZE = 
    "gridmix.missing.rec.size";


  private final long targetBytes;
  private final long targetRecords;
  private final long step;
  private final int avgrec;
  private final int keyLen;
  private long accBytes = 0L;
  private long accRecords = 0L;
  private int unspilledBytes = 0;
  private int minSpilledBytes = 0;

  /**
   * @param targetBytes Expected byte count.
   * @param targetRecords Expected record count.
   * @param conf Used to resolve edge cases @see #GRIDMIX_KEY_FRC
   */
  public AvgRecordFactory(long targetBytes, long targetRecords,
      Configuration conf) {
    this(targetBytes, targetRecords, conf, 0);
  }
  
  /**
   * @param minSpilledBytes Minimum amount of data expected per record
   */
  public AvgRecordFactory(long targetBytes, long targetRecords,
      Configuration conf, int minSpilledBytes) {
    this.targetBytes = targetBytes;
    this.targetRecords = targetRecords <= 0 && this.targetBytes >= 0
      ? Math.max(1,
          this.targetBytes / conf.getInt(GRIDMIX_MISSING_REC_SIZE, 64 * 1024))
      : targetRecords;
    final long tmp = this.targetBytes / this.targetRecords;
    step = this.targetBytes - this.targetRecords * tmp;
    avgrec = (int) Math.min(Integer.MAX_VALUE, tmp + 1);
    keyLen = Math.max(1,
        (int)(tmp * Math.min(1.0f, conf.getFloat(GRIDMIX_KEY_FRC, 0.1f))));
    this.minSpilledBytes = minSpilledBytes;
  }

  @Override
  public boolean next(GridmixKey key, GridmixRecord val) throws IOException {
    if (accBytes >= targetBytes) {
      return false;
    }
    final int reclen = accRecords++ >= step ? avgrec - 1 : avgrec;
    final int len = (int) Math.min(targetBytes - accBytes, reclen);
    
    unspilledBytes += len;
    
    // len != reclen?
    if (key != null) {
      if (unspilledBytes < minSpilledBytes && accRecords < targetRecords) {
        key.setSize(1);
        val.setSize(1);
        accBytes += key.getSize() + val.getSize();
        unspilledBytes -= (key.getSize() + val.getSize());
      } else {
        key.setSize(keyLen);
        val.setSize(unspilledBytes - key.getSize());
        accBytes += unspilledBytes;
        unspilledBytes = 0;
      }
    } else {
      if (unspilledBytes < minSpilledBytes && accRecords < targetRecords) {
        val.setSize(1);
        accBytes += val.getSize();
        unspilledBytes -= val.getSize();
      } else {
        val.setSize(unspilledBytes);
        accBytes += unspilledBytes;
        unspilledBytes = 0;
      }
    }
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(1.0f, accBytes / ((float)targetBytes));
  }

  @Override
  public void close() throws IOException {
    // noop
  }

}
