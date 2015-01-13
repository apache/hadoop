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

import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * Quota feature for {@link INodeDirectory}. 
 */
public final class DirectoryWithQuotaFeature implements INode.Feature {
  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE;
  public static final long DEFAULT_DISKSPACE_QUOTA = HdfsConstants.QUOTA_RESET;

  /** Name space quota */
  private long nsQuota = DEFAULT_NAMESPACE_QUOTA;
  /** Name space count */
  private long namespace = 1L;
  /** Disk space quota */
  private long dsQuota = DEFAULT_DISKSPACE_QUOTA;
  /** Disk space count */
  private long diskspace = 0L;
  
  DirectoryWithQuotaFeature(long nsQuota, long dsQuota) {
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }

  /** @return the quota set or -1 if it is not set. */
  Quota.Counts getQuota() {
    return Quota.Counts.newInstance(nsQuota, dsQuota);
  }
  
  /** Set this directory's quota
   * 
   * @param nsQuota Namespace quota to be set
   * @param dsQuota Diskspace quota to be set
   */
  void setQuota(long nsQuota, long dsQuota) {
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
  }
  
  Quota.Counts addNamespaceDiskspace(Quota.Counts counts) {
    counts.add(Quota.NAMESPACE, namespace);
    counts.add(Quota.DISKSPACE, diskspace);
    return counts;
  }

  ContentSummaryComputationContext computeContentSummary(final INodeDirectory dir,
      final ContentSummaryComputationContext summary) {
    final long original = summary.getCounts().get(Content.DISKSPACE);
    long oldYieldCount = summary.getYieldCount();
    dir.computeDirectoryContentSummary(summary, Snapshot.CURRENT_STATE_ID);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkDiskspace(dir, summary.getCounts().get(Content.DISKSPACE) - original);
    }
    return summary;
  }
  
  private void checkDiskspace(final INodeDirectory dir, final long computed) {
    if (-1 != getQuota().get(Quota.DISKSPACE) && diskspace != computed) {
      NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
          + dir.getFullPathName() + ". Cached = " + diskspace
          + " != Computed = " + computed);
    }
  }

  void addSpaceConsumed(final INodeDirectory dir, final long nsDelta,
      final long dsDelta, boolean verify) throws QuotaExceededException {
    if (dir.isQuotaSet()) { 
      // The following steps are important: 
      // check quotas in this inode and all ancestors before changing counts
      // so that no change is made if there is any quota violation.

      // (1) verify quota in this inode
      if (verify) {
        verifyQuota(nsDelta, dsDelta);
      }
      // (2) verify quota and then add count in ancestors 
      dir.addSpaceConsumed2Parent(nsDelta, dsDelta, verify);
      // (3) add count in this inode
      addSpaceConsumed2Cache(nsDelta, dsDelta);
    } else {
      dir.addSpaceConsumed2Parent(nsDelta, dsDelta, verify);
    }
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   */
  public void addSpaceConsumed2Cache(long nsDelta, long dsDelta) {
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
  
  /** @return the namespace and diskspace consumed. */
  public Quota.Counts getSpaceConsumed() {
    return Quota.Counts.newInstance(namespace, diskspace);
  }

  /** Verify if the namespace quota is violated after applying delta. */
  private void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
    if (Quota.isViolated(nsQuota, namespace, delta)) {
      throw new NSQuotaExceededException(nsQuota, namespace + delta);
    }
  }
  /** Verify if the diskspace quota is violated after applying delta. */
  private void verifyDiskspaceQuota(long delta) throws DSQuotaExceededException {
    if (Quota.isViolated(dsQuota, diskspace, delta)) {
      throw new DSQuotaExceededException(dsQuota, diskspace + delta);
    }
  }

  /**
   * @throws QuotaExceededException if namespace or diskspace quotas is
   *         violated after applying the deltas.
   */
  void verifyQuota(long nsDelta, long dsDelta) throws QuotaExceededException {
    verifyNamespaceQuota(nsDelta);
    verifyDiskspaceQuota(dsDelta);
  }
  
  boolean isQuotaSet() {
    return nsQuota >= 0 || dsQuota >= 0;
  }

  private String namespaceString() {
    return "namespace: " + (nsQuota < 0? "-": namespace + "/" + nsQuota);
  }
  private String diskspaceString() {
    return "diskspace: " + (dsQuota < 0? "-": diskspace + "/" + dsQuota);
  }
  
  @Override
  public String toString() {
    return "Quota[" + namespaceString() + ", " + diskspaceString() + "]";
  }
}
