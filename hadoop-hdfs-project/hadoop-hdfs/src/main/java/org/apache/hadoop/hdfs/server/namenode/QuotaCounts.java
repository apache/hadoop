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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.ConstEnumCounters;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ConstEnumCounters.ConstEnumException;

import java.util.function.Consumer;

/**
 * Counters for namespace, storage space and storage type space quota and usage.
 */
public class QuotaCounts {

  /**
   * We pre-define 4 most common used EnumCounters objects. When the nsSsCounts
   * and tsCounts are set to the 4 most common used value, we just point them to
   * the pre-defined const EnumCounters objects instead of constructing many
   * objects with the same value. See HDFS-14547.
   */
  final static EnumCounters<Quota> QUOTA_RESET =
      new ConstEnumCounters<>(Quota.class, HdfsConstants.QUOTA_RESET);
  final static EnumCounters<Quota> QUOTA_DEFAULT =
      new ConstEnumCounters<>(Quota.class, 0);
  final static EnumCounters<StorageType> STORAGE_TYPE_RESET =
      new ConstEnumCounters<>(StorageType.class, HdfsConstants.QUOTA_RESET);
  final static EnumCounters<StorageType> STORAGE_TYPE_DEFAULT =
      new ConstEnumCounters<>(StorageType.class, 0);

  /**
   * Modify counter with action. If the counter is ConstEnumCounters, copy all
   * the values of it to a new EnumCounters object, and modify the new obj.
   *
   * @param counter the EnumCounters to be modified.
   * @param action the modifying action on counter.
   * @return the modified counter.
   */
  static <T extends Enum<T>> EnumCounters<T> modify(EnumCounters<T> counter,
      Consumer<EnumCounters<T>> action) {
    try {
      action.accept(counter);
    } catch (ConstEnumException cee) {
      // We don't call clone here because ConstEnumCounters.clone() will return
      // an object of class ConstEnumCounters. We want EnumCounters.
      counter = counter.deepCopyEnumCounter();
      action.accept(counter);
    }
    return counter;
  }

  // Name space and storage space counts (HDFS-7775 refactors the original disk
  // space count to storage space counts)
  @VisibleForTesting
  EnumCounters<Quota> nsSsCounts;
  // Storage type space counts
  @VisibleForTesting
  EnumCounters<StorageType> tsCounts;

  public static class Builder {
    private EnumCounters<Quota> nsSsCounts;
    private EnumCounters<StorageType> tsCounts;

    public Builder() {
      this.nsSsCounts = QUOTA_DEFAULT;
      this.tsCounts = STORAGE_TYPE_DEFAULT;
    }

    public Builder nameSpace(long val) {
      nsSsCounts =
          setQuotaCounter(nsSsCounts, Quota.NAMESPACE, Quota.STORAGESPACE, val);
      return this;
    }

    public Builder storageSpace(long val) {
      nsSsCounts =
          setQuotaCounter(nsSsCounts, Quota.STORAGESPACE, Quota.NAMESPACE, val);
      return this;
    }

    public Builder typeSpaces(EnumCounters<StorageType> val) {
      if (val != null) {
        if (val == STORAGE_TYPE_DEFAULT || val == STORAGE_TYPE_RESET) {
          tsCounts = val;
        } else {
          tsCounts = modify(tsCounts, ec -> ec.set(val));
        }
      }
      return this;
    }

    public Builder typeSpaces(long val) {
      if (val == HdfsConstants.QUOTA_RESET) {
        tsCounts = STORAGE_TYPE_RESET;
      } else if (val == 0) {
        tsCounts = STORAGE_TYPE_DEFAULT;
      } else {
        tsCounts = modify(tsCounts, ec -> ec.reset(val));
      }
      return this;
    }

    public Builder quotaCount(QuotaCounts that) {
      if (that.nsSsCounts == QUOTA_DEFAULT || that.nsSsCounts == QUOTA_RESET) {
        nsSsCounts = that.nsSsCounts;
      } else {
        nsSsCounts = modify(nsSsCounts, ec -> ec.set(that.nsSsCounts));
      }
      if (that.tsCounts == STORAGE_TYPE_DEFAULT
          || that.tsCounts == STORAGE_TYPE_RESET) {
        tsCounts = that.tsCounts;
      } else {
        tsCounts = modify(tsCounts, ec -> ec.set(that.tsCounts));
      }
      return this;
    }

    public QuotaCounts build() {
      return new QuotaCounts(this);
    }
  }

  private QuotaCounts(Builder builder) {
    this.nsSsCounts = builder.nsSsCounts;
    this.tsCounts = builder.tsCounts;
  }

  public QuotaCounts add(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec -> ec.add(that.tsCounts));
    return this;
  }

  public QuotaCounts subtract(QuotaCounts that) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.subtract(that.nsSsCounts));
    tsCounts = modify(tsCounts, ec -> ec.subtract(that.tsCounts));
    return this;
  }

  /**
   * Returns a QuotaCounts whose value is {@code (-this)}.
   *
   * @return {@code -this}
   */
  public QuotaCounts negation() {
    QuotaCounts ret = new QuotaCounts.Builder().quotaCount(this).build();
    ret.nsSsCounts = modify(ret.nsSsCounts, ec -> ec.negation());
    ret.tsCounts = modify(ret.tsCounts, ec -> ec.negation());
    return ret;
  }

  public long getNameSpace(){
    return nsSsCounts.get(Quota.NAMESPACE);
  }

  public void setNameSpace(long nameSpaceCount) {
    nsSsCounts =
        setQuotaCounter(nsSsCounts, Quota.NAMESPACE, Quota.STORAGESPACE,
            nameSpaceCount);
  }

  public void addNameSpace(long nsDelta) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(Quota.NAMESPACE, nsDelta));
  }

  public long getStorageSpace(){
    return nsSsCounts.get(Quota.STORAGESPACE);
  }

  public void setStorageSpace(long spaceCount) {
    nsSsCounts =
        setQuotaCounter(nsSsCounts, Quota.STORAGESPACE, Quota.NAMESPACE,
            spaceCount);
  }

  public void addStorageSpace(long dsDelta) {
    nsSsCounts = modify(nsSsCounts, ec -> ec.add(Quota.STORAGESPACE, dsDelta));
  }

  public EnumCounters<StorageType> getTypeSpaces() {
    EnumCounters<StorageType> ret =
        new EnumCounters<StorageType>(StorageType.class);
    ret.set(tsCounts);
    return ret;
  }

  void setTypeSpaces(EnumCounters<StorageType> that) {
    if (that == STORAGE_TYPE_DEFAULT || that == STORAGE_TYPE_RESET) {
      tsCounts = that;
    } else if (that != null) {
      tsCounts = modify(tsCounts, ec -> ec.set(that));
    }
  }

  long getTypeSpace(StorageType type) {
    return this.tsCounts.get(type);
  }

  void setTypeSpace(StorageType type, long spaceCount) {
    tsCounts = modify(tsCounts, ec -> ec.set(type, spaceCount));
  }

  public void addTypeSpace(StorageType type, long delta) {
    tsCounts = modify(tsCounts, ec -> ec.add(type, delta));
  }

  public boolean anyNsSsCountGreaterOrEqual(long val) {
  if (nsSsCounts == QUOTA_DEFAULT) {
      return val <= 0;
    } else if (nsSsCounts == QUOTA_RESET) {
      return val <= HdfsConstants.QUOTA_RESET;
    }
    return nsSsCounts.anyGreaterOrEqual(val);
  }

  public boolean anyTypeSpaceCountGreaterOrEqual(long val) {
    if (tsCounts == STORAGE_TYPE_DEFAULT) {
      return val <= 0;
    } else if (tsCounts == STORAGE_TYPE_RESET) {
      return val <= HdfsConstants.QUOTA_RESET;
    }
    return tsCounts.anyGreaterOrEqual(val);
  }

  /**
   * Set inputCounts' value of Quota type quotaToSet to val.
   * inputCounts should be the left side value of this method.
   *
   * @param inputCounts the EnumCounters instance.
   * @param quotaToSet the quota type to be set.
   * @param otherQuota the other quota type besides quotaToSet.
   * @param val the value to be set.
   * @return the modified inputCounts.
   */
  private static EnumCounters<Quota> setQuotaCounter(
      EnumCounters<Quota> inputCounts, Quota quotaToSet, Quota otherQuota,
      long val) {
    if (val == HdfsConstants.QUOTA_RESET
        && inputCounts.get(otherQuota) == HdfsConstants.QUOTA_RESET) {
      return QUOTA_RESET;
    } else if (val == 0 && inputCounts.get(otherQuota) == 0) {
      return QUOTA_DEFAULT;
    } else {
      return modify(inputCounts, ec -> ec.set(quotaToSet, val));
    }
  }

  @Override
  public String toString() {
    return "name space=" + getNameSpace() +
        "\nstorage space=" + getStorageSpace() +
        "\nstorage types=" + getTypeSpaces();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof QuotaCounts)) {
      return false;
    }
    final QuotaCounts that = (QuotaCounts)obj;
    return this.nsSsCounts.equals(that.nsSsCounts)
        && this.tsCounts.equals(that.tsCounts);
  }

  @Override
  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }

}
