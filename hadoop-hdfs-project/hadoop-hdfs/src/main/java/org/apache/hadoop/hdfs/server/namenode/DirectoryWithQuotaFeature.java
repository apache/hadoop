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
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.util.EnumCounters;

/**
 * Quota feature for {@link INodeDirectory}. 
 */
public final class DirectoryWithQuotaFeature implements INode.Feature {
  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE;
  public static final long DEFAULT_SPACE_QUOTA = HdfsConstants.QUOTA_RESET;

  private QuotaCounts quota;
  private QuotaCounts usage;

  public static class Builder {
    private QuotaCounts quota;
    private QuotaCounts usage;

    public Builder() {
      this.quota = new QuotaCounts.Builder().nameCount(DEFAULT_NAMESPACE_QUOTA).
          spaceCount(DEFAULT_SPACE_QUOTA).typeCounts(DEFAULT_SPACE_QUOTA).build();
      this.usage = new QuotaCounts.Builder().nameCount(1).build();
    }

    public Builder nameSpaceQuota(long nameSpaceQuota) {
      this.quota.setNameSpace(nameSpaceQuota);
      return this;
    }

    public Builder spaceQuota(long spaceQuota) {
      this.quota.setDiskSpace(spaceQuota);
      return this;
    }

    public Builder typeQuotas(EnumCounters<StorageType> typeQuotas) {
      this.quota.setTypeSpaces(typeQuotas);
      return this;
    }

    public Builder typeQuota(StorageType type, long quota) {
      this.quota.setTypeSpace(type, quota);
      return this;
    }

    public DirectoryWithQuotaFeature build() {
      return new DirectoryWithQuotaFeature(this);
    }
  }

  private DirectoryWithQuotaFeature(Builder builder) {
    this.quota = builder.quota;
    this.usage = builder.usage;
  }

  /** @return the quota set or -1 if it is not set. */
  QuotaCounts getQuota() {
    return new QuotaCounts.Builder().quotaCount(this.quota).build();
  }

  /** Set this directory's quota
   * 
   * @param nsQuota Namespace quota to be set
   * @param dsQuota Diskspace quota to be set
   * @param type Storage type quota to be set
   * * To set traditional space/namespace quota, type must be null
   */
  void setQuota(long nsQuota, long dsQuota, StorageType type) {
    if (type != null) {
      this.quota.setTypeSpace(type, dsQuota);
    } else {
      setQuota(nsQuota, dsQuota);
    }
  }

  void setQuota(long nsQuota, long dsQuota) {
    this.quota.setNameSpace(nsQuota);
    this.quota.setDiskSpace(dsQuota);
  }

  void setQuota(long dsQuota, StorageType type) {
    this.quota.setTypeSpace(type, dsQuota);
  }

  // Set in a batch only during FSImage load
  void setQuota(EnumCounters<StorageType> typeQuotas) {
    this.quota.setTypeSpaces(typeQuotas);
  }

  QuotaCounts addNamespaceDiskspace(QuotaCounts counts) {
    counts.add(this.usage);
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
    if (-1 != quota.getDiskSpace() && usage.getDiskSpace() != computed) {
      NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
          + dir.getFullPathName() + ". Cached = " + usage.getDiskSpace()
          + " != Computed = " + computed);
    }
  }

  void addSpaceConsumed(final INodeDirectory dir, final QuotaCounts counts,
      boolean verify) throws QuotaExceededException {
    if (dir.isQuotaSet()) {
      // The following steps are important:
      // check quotas in this inode and all ancestors before changing counts
      // so that no change is made if there is any quota violation.
      // (1) verify quota in this inode
      if (verify) {
        verifyQuota(counts);
      }
      // (2) verify quota and then add count in ancestors
      dir.addSpaceConsumed2Parent(counts, verify);
      // (3) add count in this inode
      addSpaceConsumed2Cache(counts);
    } else {
      dir.addSpaceConsumed2Parent(counts, verify);
    }
  }
  
  /** Update the space/namespace/type usage of the tree
   * 
   * @param delta the change of the namespace/space/type usage
   */
  public void addSpaceConsumed2Cache(QuotaCounts delta) {
    usage.add(delta);
  }

  /** 
   * Sets namespace and diskspace take by the directory rooted 
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   * 
   * @param namespace size of the directory to be set
   * @param diskspace disk space take by all the nodes under this directory
   * @param typeUsed counters of storage type usage
   */
  void setSpaceConsumed(long namespace, long diskspace,
      EnumCounters<StorageType> typeUsed) {
    usage.setNameSpace(namespace);
    usage.setDiskSpace(diskspace);
    usage.setTypeSpaces(typeUsed);
  }

  void setSpaceConsumed(QuotaCounts c) {
    usage.setNameSpace(c.getNameSpace());
    usage.setDiskSpace(c.getDiskSpace());
    usage.setTypeSpaces(c.getTypeSpaces());
  }

  /** @return the namespace and diskspace consumed. */
  public QuotaCounts getSpaceConsumed() {
    return new QuotaCounts.Builder().quotaCount(usage).build();
  }

  /** Verify if the namespace quota is violated after applying delta. */
  private void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
    if (Quota.isViolated(quota.getNameSpace(), usage.getNameSpace(), delta)) {
      throw new NSQuotaExceededException(quota.getNameSpace(),
          usage.getNameSpace() + delta);
    }
  }
  /** Verify if the diskspace quota is violated after applying delta. */
  private void verifyDiskspaceQuota(long delta) throws DSQuotaExceededException {
    if (Quota.isViolated(quota.getDiskSpace(), usage.getDiskSpace(), delta)) {
      throw new DSQuotaExceededException(quota.getDiskSpace(),
          usage.getDiskSpace() + delta);
    }
  }

  private void verifyQuotaByStorageType(EnumCounters<StorageType> typeDelta)
      throws QuotaByStorageTypeExceededException {
    if (!isQuotaByStorageTypeSet()) {
      return;
    }
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      if (!isQuotaByStorageTypeSet(t)) {
        continue;
      }
      if (Quota.isViolated(quota.getTypeSpace(t), usage.getTypeSpace(t),
          typeDelta.get(t))) {
        throw new QuotaByStorageTypeExceededException(
          quota.getTypeSpace(t), usage.getTypeSpace(t) + typeDelta.get(t), t);
      }
    }
  }

  /**
   * @throws QuotaExceededException if namespace, diskspace or storage type quotas
   * is violated after applying the deltas.
   */
  void verifyQuota(QuotaCounts counts) throws QuotaExceededException {
    verifyNamespaceQuota(counts.getNameSpace());
    verifyDiskspaceQuota(counts.getDiskSpace());
    verifyQuotaByStorageType(counts.getTypeSpaces());
  }

  boolean isQuotaSet() {
    return quota.anyNsSpCountGreaterOrEqual(0) ||
        quota.anyTypeCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet() {
    return quota.anyTypeCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet(StorageType t) {
    return quota.getTypeSpace(t) >= 0;
  }

  private String namespaceString() {
    return "namespace: " + (quota.getNameSpace() < 0? "-":
        usage.getNameSpace() + "/" + quota.getNameSpace());
  }
  private String diskspaceString() {
    return "diskspace: " + (quota.getDiskSpace() < 0? "-":
        usage.getDiskSpace() + "/" + quota.getDiskSpace());
  }

  private String quotaByStorageTypeString() {
    StringBuilder sb = new StringBuilder();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      sb.append("StorageType: " + t +
          (quota.getTypeSpace(t) < 0? "-":
          usage.getTypeSpace(t) + "/" + usage.getTypeSpace(t)));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "Quota[" + namespaceString() + ", " + diskspaceString() +
        ", " + quotaByStorageTypeString() + "]";
  }
}