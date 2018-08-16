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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.util.StringUtils;

/**
 * The subclass of {@link QuotaUsage} used in Router-based federation.
 */
public final class RouterQuotaUsage extends QuotaUsage {

  /** Default quota usage count. */
  public static final long QUOTA_USAGE_COUNT_DEFAULT = 0;

  private RouterQuotaUsage(Builder builder) {
    super(builder);
  }

  /** Build the instance based on the builder. */
  public static class Builder extends QuotaUsage.Builder {

    public RouterQuotaUsage build() {
      return new RouterQuotaUsage(this);
    }

    @Override
    public Builder fileAndDirectoryCount(long count) {
      super.fileAndDirectoryCount(count);
      return this;
    }

    @Override
    public Builder quota(long quota) {
      super.quota(quota);
      return this;
    }

    @Override
    public Builder spaceConsumed(long spaceConsumed) {
      super.spaceConsumed(spaceConsumed);
      return this;
    }

    @Override
    public Builder spaceQuota(long spaceQuota) {
      super.spaceQuota(spaceQuota);
      return this;
    }
  }

  /**
   * Verify if namespace quota is violated once quota is set. Relevant
   * method {@link DirectoryWithQuotaFeature#verifyNamespaceQuota}.
   * @throws NSQuotaExceededException
   */
  public void verifyNamespaceQuota() throws NSQuotaExceededException {
    if (Quota.isViolated(getQuota(), getFileAndDirectoryCount())) {
      throw new NSQuotaExceededException(getQuota(),
          getFileAndDirectoryCount());
    }
  }

  /**
   * Verify if storage space quota is violated once quota is set. Relevant
   * method {@link DirectoryWithQuotaFeature#verifyStoragespaceQuota}.
   * @throws DSQuotaExceededException
   */
  public void verifyStoragespaceQuota() throws DSQuotaExceededException {
    if (Quota.isViolated(getSpaceQuota(), getSpaceConsumed())) {
      throw new DSQuotaExceededException(getSpaceQuota(), getSpaceConsumed());
    }
  }

  @Override
  public String toString() {
    String nsQuota = String.valueOf(getQuota());
    String nsCount = String.valueOf(getFileAndDirectoryCount());
    if (getQuota() == HdfsConstants.QUOTA_RESET) {
      nsQuota = "-";
      nsCount = "-";
    }

    String ssQuota = StringUtils.byteDesc(getSpaceQuota());
    String ssCount = StringUtils.byteDesc(getSpaceConsumed());
    if (getSpaceQuota() == HdfsConstants.QUOTA_RESET) {
      ssQuota = "-";
      ssCount = "-";
    }

    StringBuilder str = new StringBuilder();
    str.append("[NsQuota: ").append(nsQuota).append("/")
        .append(nsCount);
    str.append(", SsQuota: ").append(ssQuota)
        .append("/").append(ssCount)
        .append("]");
    return str.toString();
  }
}
