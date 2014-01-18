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
public class RollingUpgradeInfo {
  public static final RollingUpgradeInfo EMPTY_INFO = new RollingUpgradeInfo(0); 

  private long startTime;
  private long finalizeTime;
  
  public RollingUpgradeInfo(long startTime) {
    this(startTime, 0L);
  }

  public RollingUpgradeInfo(long startTime, long finalizeTime) {
    this.startTime = startTime;
    this.finalizeTime = finalizeTime;
  }
  
  public boolean isStarted() {
    return startTime != 0;
  }
  
  /** @return The rolling upgrade starting time. */
  public long getStartTime() {
    return startTime;
  }
  
  public boolean isFinalized() {
    return finalizeTime != 0;
  }

  public long getFinalizeTime() {
    return finalizeTime;
  }
  
  @Override
  public String toString() {
    return "     Start Time: " + (startTime == 0? "<NOT STARTED>": timestamp2String(startTime))
      +  "\n  Finalize Time: " + (finalizeTime == 0? "<NOT FINALIZED>": timestamp2String(finalizeTime));
  }
  
  private static String timestamp2String(long timestamp) {
    return new Date(timestamp) + " (=" + timestamp + ")";
  }
}
