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

import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.util.EnumCounters;

/**
 * Counters for namespace, space and storage type quota and usage.
 */
public class QuotaCounts {

  private EnumCounters<Quota> nsSpCounts;
  private EnumCounters<StorageType> typeCounts;

  public static class Builder {
    private EnumCounters<Quota> nsSpCounts;
    private EnumCounters<StorageType> typeCounts;

    public Builder() {
      this.nsSpCounts = new EnumCounters<Quota>(Quota.class);
      this.typeCounts = new EnumCounters<StorageType>(StorageType.class);
    }

    public Builder nameCount(long val) {
      this.nsSpCounts.set(Quota.NAMESPACE, val);
      return this;
    }

    public Builder spaceCount(long val) {
      this.nsSpCounts.set(Quota.DISKSPACE, val);
      return this;
    }

    public Builder typeCounts(EnumCounters<StorageType> val) {
      if (val != null) {
        this.typeCounts.set(val);
      }
      return this;
    }

    public Builder typeCounts(long val) {
      this.typeCounts.reset(val);
      return this;
    }

    public Builder quotaCount(QuotaCounts that) {
      this.nsSpCounts.set(that.nsSpCounts);
      this.typeCounts.set(that.typeCounts);
      return this;
    }

    public QuotaCounts build() {
      return new QuotaCounts(this);
    }
  }

  private QuotaCounts(Builder builder) {
    this.nsSpCounts = builder.nsSpCounts;
    this.typeCounts = builder.typeCounts;
  }

  public void add(QuotaCounts that) {
    this.nsSpCounts.add(that.nsSpCounts);
    this.typeCounts.add(that.typeCounts);
  }

  public void subtract(QuotaCounts that) {
    this.nsSpCounts.subtract(that.nsSpCounts);
    this.typeCounts.subtract(that.typeCounts);
  }

  /**
   * Returns a QuotaCounts whose value is {@code (-this)}.
   *
   * @return {@code -this}
   */
  public QuotaCounts negation() {
    QuotaCounts ret = new QuotaCounts.Builder().quotaCount(this).build();
    ret.nsSpCounts.negation();
    ret.typeCounts.negation();
    return ret;
  }

  public long getNameSpace(){
    return nsSpCounts.get(Quota.NAMESPACE);
  }

  public void setNameSpace(long nameSpaceCount) {
    this.nsSpCounts.set(Quota.NAMESPACE, nameSpaceCount);
  }

  public void addNameSpace(long nsDelta) {
    this.nsSpCounts.add(Quota.NAMESPACE, nsDelta);
  }

  public long getDiskSpace(){
    return nsSpCounts.get(Quota.DISKSPACE);
  }

  public void setDiskSpace(long spaceCount) {
    this.nsSpCounts.set(Quota.DISKSPACE, spaceCount);
  }

  public void addDiskSpace(long dsDelta) {
    this.nsSpCounts.add(Quota.DISKSPACE, dsDelta);
  }

  public EnumCounters<StorageType> getTypeSpaces() {
    EnumCounters<StorageType> ret =
        new EnumCounters<StorageType>(StorageType.class);
    ret.set(typeCounts);
    return ret;
  }

  void setTypeSpaces(EnumCounters<StorageType> that) {
    if (that != null) {
      this.typeCounts.set(that);
    }
  }

  long getTypeSpace(StorageType type) {
    return this.typeCounts.get(type);
  }

  void setTypeSpace(StorageType type, long spaceCount) {
    this.typeCounts.set(type, spaceCount);
  }

  public void addTypeSpace(StorageType type, long delta) {
    this.typeCounts.add(type, delta);
  }

  public void addTypeSpaces(EnumCounters<StorageType> deltas) {
    this.typeCounts.add(deltas);
  }

  public boolean anyNsSpCountGreaterOrEqual(long val) {
    return nsSpCounts.anyGreaterOrEqual(val);
  }

  public boolean anyTypeCountGreaterOrEqual(long val) {
    return typeCounts.anyGreaterOrEqual(val);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof QuotaCounts)) {
      return false;
    }
    final QuotaCounts that = (QuotaCounts)obj;
    return this.nsSpCounts.equals(that.nsSpCounts)
        && this.typeCounts.equals(that.typeCounts);
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }
}