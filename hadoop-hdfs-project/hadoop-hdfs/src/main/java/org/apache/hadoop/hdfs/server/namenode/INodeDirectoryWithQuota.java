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
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Directory INode class that has a quota restriction
 */
public class INodeDirectoryWithQuota extends INodeDirectory {
  /** Name space quota */
  private long nsQuota = Long.MAX_VALUE;
  /** Name space count */
  private long namespace = 1L;
  /** Disk space quota */
  private long dsQuota = HdfsConstants.QUOTA_RESET;
  /** Disk space count */
  private long diskspace = 0L;
  
  /** Convert an existing directory inode to one with the given quota
   * 
   * @param nsQuota Namespace quota to be assigned to this inode
   * @param dsQuota Diskspace quota to be assigned to this indoe
   * @param other The other inode from which all other properties are copied
   */
  public INodeDirectoryWithQuota(INodeDirectory other, boolean adopt,
      long nsQuota, long dsQuota) {
    super(other, adopt);
    final Quota.Counts counts = other.computeQuotaUsage();
    this.namespace = counts.get(Quota.NAMESPACE);
    this.diskspace = counts.get(Quota.DISKSPACE);
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }
  
  /** constructor with no quota verification */
  INodeDirectoryWithQuota(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long nsQuota, long dsQuota) {
    super(id, name, permissions, modificationTime);
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }
  
  /** constructor with no quota verification */
  INodeDirectoryWithQuota(long id, byte[] name, PermissionStatus permissions) {
    super(id, name, permissions, 0L);
  }
  
  /** Get this directory's namespace quota
   * @return this directory's namespace quota
   */
  @Override
  public long getNsQuota() {
    return nsQuota;
  }
  
  /** Get this directory's diskspace quota
   * @return this directory's diskspace quota
   */
  @Override
  public long getDsQuota() {
    return dsQuota;
  }
  
  /** Set this directory's quota
   * 
   * @param nsQuota Namespace quota to be set
   * @param dsQuota diskspace quota to be set
   */
  public void setQuota(long nsQuota, long dsQuota) {
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }
  
  @Override
  public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache,
      int lastSnapshotId) {
    if (useCache && isQuotaSet()) {
      // use cache value
      counts.add(Quota.NAMESPACE, namespace);
      counts.add(Quota.DISKSPACE, diskspace);
    } else {
      super.computeQuotaUsage(counts, false, lastSnapshotId);
    }
    return counts;
  }

  @Override
  public ContentSummaryComputationContext computeContentSummary(
      final ContentSummaryComputationContext summary) {
    final long original = summary.getCounts().get(Content.DISKSPACE);
    long oldYieldCount = summary.getYieldCount();
    super.computeContentSummary(summary);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkDiskspace(summary.getCounts().get(Content.DISKSPACE) - original);
    }
    return summary;
  }
  
  private void checkDiskspace(final long computed) {
    if (-1 != getDsQuota() && diskspace != computed) {
      NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
          + getFullPathName() + ". Cached = " + diskspace
          + " != Computed = " + computed);
    }
  }

  /** Get the number of names in the subtree rooted at this directory
   * @return the size of the subtree rooted at this directory
   */
  long numItemsInTree() {
    return namespace;
  }
  
  @Override
  public final void addSpaceConsumed(final long nsDelta, final long dsDelta,
      boolean verify) throws QuotaExceededException {
    if (isQuotaSet()) { 
      // The following steps are important: 
      // check quotas in this inode and all ancestors before changing counts
      // so that no change is made if there is any quota violation.

      // (1) verify quota in this inode
      if (verify) {
        verifyQuota(nsDelta, dsDelta);
      }
      // (2) verify quota and then add count in ancestors 
      super.addSpaceConsumed(nsDelta, dsDelta, verify);
      // (3) add count in this inode
      addSpaceConsumed2Cache(nsDelta, dsDelta);
    } else {
      super.addSpaceConsumed(nsDelta, dsDelta, verify);
    }
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   */
  protected void addSpaceConsumed2Cache(long nsDelta, long dsDelta) {
    namespace += nsDelta;
    diskspace += dsDelta;
  }
  
  /** 
   * Sets namespace and diskspace take by the directory rooted 
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   * 
   * @param namespace size of the directory to be set
   * @param diskspace disk space take by all the nodes under this directory
   */
  void setSpaceConsumed(long namespace, long diskspace) {
    this.namespace = namespace;
    this.diskspace = diskspace;
  }
  
  /** Verify if the namespace quota is violated after applying delta. */
  void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
    if (Quota.isViolated(nsQuota, namespace, delta)) {
      throw new NSQuotaExceededException(nsQuota, namespace + delta);
    }
  }

  /** Verify if the namespace count disk space satisfies the quota restriction 
   * @throws QuotaExceededException if the given quota is less than the count
   */
  void verifyQuota(long nsDelta, long dsDelta) throws QuotaExceededException {
    verifyNamespaceQuota(nsDelta);

    if (Quota.isViolated(dsQuota, diskspace, dsDelta)) {
      throw new DSQuotaExceededException(dsQuota, diskspace + dsDelta);
    }
  }

  String namespaceString() {
    return "namespace: " + (nsQuota < 0? "-": namespace + "/" + nsQuota);
  }
  String diskspaceString() {
    return "diskspace: " + (dsQuota < 0? "-": diskspace + "/" + dsQuota);
  }
  String quotaString() {
    return ", Quota[" + namespaceString() + ", " + diskspaceString() + "]";
  }
  
  @VisibleForTesting
  public long getNamespace() {
    return this.namespace;
  }
  
  @VisibleForTesting
  public long getDiskspace() {
    return this.diskspace;
  }
}
