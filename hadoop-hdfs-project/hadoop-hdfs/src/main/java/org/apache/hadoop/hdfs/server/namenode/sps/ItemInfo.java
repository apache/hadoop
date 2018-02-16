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
 * ItemInfo is a file info object for which need to satisfy the policy. For
 * internal satisfier service, it uses inode id which is Long datatype. For the
 * external satisfier service, it uses the full string representation of the
 * path.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ItemInfo<T> {
  private T startPath;
  private T file;
  private int retryCount;

  public ItemInfo(T startPath, T file) {
    this.startPath = startPath;
    this.file = file;
    // set 0 when item is getting added first time in queue.
    this.retryCount = 0;
  }

  public ItemInfo(final T startPath, final T file, final int retryCount) {
    this.startPath = startPath;
    this.file = file;
    this.retryCount = retryCount;
  }

  /**
   * Returns the start path of the current file. This indicates that SPS
   * was invoked on this path.
   */
  public T getStartPath() {
    return startPath;
  }

  /**
   * Returns the file for which needs to satisfy the policy.
   */
  public T getFile() {
    return file;
  }

  /**
   * Returns true if the tracking path is a directory, false otherwise.
   */
  public boolean isDir() {
    return !startPath.equals(file);
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