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
package org.apache.hadoop.hdfs.server.namenode.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * ItemInfo is a file info object for which need to satisfy the policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ItemInfo {
  private long startPathId;
  private long fileId;
  private int retryCount;

  public ItemInfo(long startPathId, long fileId) {
    this.startPathId = startPathId;
    this.fileId = fileId;
    // set 0 when item is getting added first time in queue.
    this.retryCount = 0;
  }

  public ItemInfo(final long startPathId, final long fileId,
      final int retryCount) {
    this.startPathId = startPathId;
    this.fileId = fileId;
    this.retryCount = retryCount;
  }

  /**
   * Returns the start path of the current file. This indicates that SPS
   * was invoked on this path.
   */
  public long getStartPath() {
    return startPathId;
  }

  /**
   * Returns the file for which needs to satisfy the policy.
   */
  public long getFile() {
    return fileId;
  }

  /**
   * Returns true if the tracking path is a directory, false otherwise.
   */
  public boolean isDir() {
    return !(startPathId == fileId);
  }

  /**
   * Get the attempted retry count of the block for satisfy the policy.
   */
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Increments the retry count.
   */
  public void increRetryCount() {
    this.retryCount++;
  }
}