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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;

/**
 * Quota feature for {@link INodeDirectory}. 
 */
public final class DirectoryWithQuotaFeature implements INode.Feature {
  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE;
  public static final long DEFAULT_STORAGE_SPACE_QUOTA = HdfsConstants.QUOTA_RESET;

  private QuotaCounts quota;
  private QuotaCounts usage;

  public static class Builder {
    private QuotaCounts quota;
    private QuotaCounts usage;

    public Builder() {
      this.quota = new QuotaCounts.Builder().nameSpace(DEFAULT_NAMESPACE_QUOTA).
          storageSpace(DEFAULT_STORAGE_SPACE_QUOTA).
          typeSpaces(DEFAULT_STORAGE_SPACE_QUOTA).build();
      this.usage = new QuotaCounts.Builder().nameSpace(1).build();
    }

    public Builder nameSpaceQuota(long nameSpaceQuota) {
      this.quota.setNameSpace(nameSpaceQuota);
      return this;
    }

    public Builder storageSpaceQuota(long spaceQuota) {
      this.quota.setStorageSpace(spaceQuota);
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
   * @param ssQuota Storagespace quota to be set
   * @param type Storage type of the storage space quota to be set.
   *             To set storagespace/namespace quota, type must be null.
   */
  void setQuota(long nsQuota, long ssQuota, StorageType type) {
    if (type != null) {
      this.quota.setTypeSpace(type, ssQuota);
    } else {
      setQuota(nsQuota, ssQuota);
    }
  }

  void setQuota(long nsQuota, long ssQuota) {
    this.quota.setNameSpace(nsQuota);
    this.quota.setStorageSpace(ssQuota);
  }

  void setQuota(long quota, StorageType type) {
    this.quota.setTypeSpace(type, quota);
  }

  /** Set storage type quota in a batch. (Only used by FSImage load)
   *
   * @param tsQuotas type space counts for all storage types supporting quota
   */
  void setQuota(EnumCounters<StorageType> tsQuotas) {
    this.quota.setTypeSpaces(tsQuotas);
  }

  /**
   * Add current quota usage to counts and return the updated counts
   * @param counts counts to be added with current quota usage
   * @return counts that have been added with the current qutoa usage
   */
  QuotaCounts AddCurrentSpaceUsage(QuotaCounts counts) {
    counts.add(this.usage);
    return counts;
  }

  ContentSummaryComputationContext computeContentSummary(final INodeDirectory dir,
      final ContentSummaryComputationContext summary)
      throws AccessControlException {
    final long original = summary.getCounts().getStoragespace();
    long oldYieldCount = summary.getYieldCount();
    dir.computeDirectoryContentSummary(summary, Snapshot.CURRENT_STATE_ID);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkStoragespace(dir, summary.getCounts().getStoragespace() - original);
    }
    return summary;
  }

  private void checkStoragespace(final INodeDirectory dir, final long computed) {
    if (-1 != quota.getStorageSpace() && usage.getStorageSpace() != computed) {
      NameNode.LOG.warn("BUG: Inconsistent storagespace for directory "
          + dir.getFullPathName() + ". Cached = " + usage.getStorageSpace()
          + " != Computed = " + computed);
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
   * Sets namespace and storagespace take by the directory rooted
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   * 
   * @param namespace size of the directory to be set
   * @param storagespace storage space take by all the nodes under this directory
   * @param typespaces counters of storage type usage
   */
  void setSpaceConsumed(long namespace, long storagespace,
      EnumCounters<StorageType> typespaces) {
    usage.setNameSpace(namespace);
    usage.setStorageSpace(storagespace);
    usage.setTypeSpaces(typespaces);
  }

  void setSpaceConsumed(QuotaCounts c) {
    usage.setNameSpace(c.getNameSpace());
    usage.setStorageSpace(c.getStorageSpace());
    usage.setTypeSpaces(c.getTypeSpaces());
  }

  /** @return the namespace and storagespace and typespace consumed. */
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
  /** Verify if the storagespace quota is violated after applying delta. */
  private void verifyStoragespaceQuota(long delta) throws DSQuotaExceededException {
    if (Quota.isViolated(quota.getStorageSpace(), usage.getStorageSpace(), delta)) {
      throw new DSQuotaExceededException(quota.getStorageSpace(),
          usage.getStorageSpace() + delta);
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
   * @throws QuotaExceededException if namespace, storagespace or storage type
   * space quota is violated after applying the deltas.
   */
  void verifyQuota(QuotaCounts counts) throws QuotaExceededException {
    verifyNamespaceQuota(counts.getNameSpace());
    verifyStoragespaceQuota(counts.getStorageSpace());
    verifyQuotaByStorageType(counts.getTypeSpaces());
  }

  boolean isQuotaSet() {
    return quota.anyNsSsCountGreaterOrEqual(0) ||
        quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet() {
    return quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet(StorageType t) {
    return quota.getTypeSpace(t) >= 0;
  }

  private String namespaceString() {
    return "namespace: " + (quota.getNameSpace() < 0? "-":
        usage.getNameSpace() + "/" + quota.getNameSpace());
  }
  private String storagespaceString() {
    return "storagespace: " + (quota.getStorageSpace() < 0? "-":
        usage.getStorageSpace() + "/" + quota.getStorageSpace());
  }

  private String typeSpaceString() {
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
    return "Quota[" + namespaceString() + ", " + storagespaceString() +
        ", " + typeSpaceString() + "]";
  }
}
