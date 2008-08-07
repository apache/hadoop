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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

/**
 * Directory INode class that has a quota restriction
 */
class INodeDirectoryWithQuota extends INodeDirectory {
  private long quota;
  private long count;
  
  /** Convert an existing directory inode to one with the given quota
   * 
   * @param quota Quota to be assigned to this inode
   * @param other The other inode from which all other properties are copied
   */
  INodeDirectoryWithQuota(long quota, INodeDirectory other)
  throws QuotaExceededException {
    super(other);
    this.count = other.numItemsInTree();
    setQuota(quota);
  }
  
  /** constructor with no quota verification */
  INodeDirectoryWithQuota(
      PermissionStatus permissions, long modificationTime, long quota)
  {
    super(permissions, modificationTime);
    this.quota = quota;
  }
  
  /** constructor with no quota verification */
  INodeDirectoryWithQuota(String name, PermissionStatus permissions, long quota)
  {
    super(name, permissions);
    this.quota = quota;
  }
  
  /** Get this directory's quota
   * @return this directory's quota
   */
  long getQuota() {
    return quota;
  }
  
  /** Set this directory's quota
   * 
   * @param quota Quota to be set
   * @throws QuotaExceededException if the given quota is less than 
   *                                the size of the tree
   */
  void setQuota(long quota) throws QuotaExceededException {
    verifyQuota(quota, this.count);
    this.quota = quota;
  }
  
  /** Get the number of names in the subtree rooted at this directory
   * @return the size of the subtree rooted at this directory
   */
  long numItemsInTree() {
    return count;
  }
  
  /** Update the size of the tree
   * 
   * @param delta the change of the tree size
   * @throws QuotaExceededException if the changed size is greater 
   *                                than the quota
   */
  void updateNumItemsInTree(long delta) throws QuotaExceededException {
    long newCount = this.count + delta;
    if (delta>0) {
      verifyQuota(this.quota, newCount);
    }
    this.count = newCount;
  }
  
  /** Set the size of the tree rooted at this directory
   * 
   * @param count size of the directory to be set
   * @throws QuotaExceededException if the given count is greater than quota
   */
  void setCount(long count) throws QuotaExceededException {
    verifyQuota(this.quota, count);
    this.count = count;
  }
  
  /** Verify if the count satisfies the quota restriction 
   * @throws QuotaExceededException if the given quota is less than the count
   */
  private static void verifyQuota(long quota, long count)
  throws QuotaExceededException {
    if (quota < count) {
      throw new QuotaExceededException(quota, count);
    }
  }
}
