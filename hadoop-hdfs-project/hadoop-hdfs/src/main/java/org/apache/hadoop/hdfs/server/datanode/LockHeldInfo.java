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
package org.apache.hadoop.hdfs.server.datanode;

public class LockHeldInfo {
  /** start lock time */
  private  Long startTimeMs;
  /** end lock time. */
  private  Long endTimeMs;
  /** The operation name. */
  private String opName;

  private boolean isReadLock;

  private boolean isBpLevel;

  private  Long startChildLockTimeMs;
  /** child lock held time duration. */
  private  Long endChildLockTimeMs;
  /** child lock name. */
  private String childLockName;

  public long getLockHeldInterval() {
    return endTimeMs - startTimeMs;
  }

  public void setStartTimeMs(Long startTimeMs) {
    this.startTimeMs = startTimeMs;
  }

  public void setEndTimeMs(Long endTimeMs) {
    this.endTimeMs = endTimeMs;
  }

  public void setOpName(String opName) {
    this.opName = opName;
  }

  public void setReadLock(boolean readLock) {
    isReadLock = readLock;
  }

  public void setBpLevel(boolean bpLevel) {
    isBpLevel = bpLevel;
  }

  public boolean isBpLevel() {
    return isBpLevel;
  }

  public void setStartChildLockTimeMs(Long startChildLockTimeMs) {
    this.startChildLockTimeMs = startChildLockTimeMs;
  }

  public void setEndChildLockTimeMs(Long endChildLockTimeMs) {
    this.endChildLockTimeMs = endChildLockTimeMs;
  }

  public void setChildLockName(String childLockName) {
    this.childLockName = childLockName;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("DataSetLockHeldInfo{");
    stringBuilder.append("timeDurationMs=" + (endTimeMs - startTimeMs));
    stringBuilder.append(", opName='" + opName + '\'');
    stringBuilder.append(", isReadLock=" + isReadLock);
    stringBuilder.append(", childLockInfo=" + (isBpLevel ?
        "{timeDurationMs=" + (endChildLockTimeMs - startChildLockTimeMs) +
            " lockName=" + childLockName + " }" : null));
    stringBuilder.append('}');
    return stringBuilder.toString();
  }
}
