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
package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/** Store the quota usage of a directory. */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QuotaUsage {
  private long fileAndDirectoryCount;
  // Make the followings protected so that
  // deprecated ContentSummary constructor can use them.
  private long quota;
  private long spaceConsumed;
  private long spaceQuota;
  private long[] typeConsumed;
  private long[] typeQuota;

  /** Builder class for QuotaUsage. */
  public static class Builder {
    public Builder() {
      this.quota = -1;
      this.spaceQuota = -1;

      typeConsumed = new long[StorageType.values().length];
      typeQuota = new long[StorageType.values().length];
      for (int i = 0; i < typeQuota.length; i++) {
        typeQuota[i] = -1;
      }
    }

    public Builder fileAndDirectoryCount(long count) {
      this.fileAndDirectoryCount = count;
      return this;
    }

    public Builder quota(long quota){
      this.quota = quota;
      return this;
    }

    public Builder spaceConsumed(long spaceConsumed) {
      this.spaceConsumed = spaceConsumed;
      return this;
    }

    public Builder spaceQuota(long spaceQuota) {
      this.spaceQuota = spaceQuota;
      return this;
    }

    public Builder typeConsumed(long[] typeConsumed) {
      for (int i = 0; i < typeConsumed.length; i++) {
        this.typeConsumed[i] = typeConsumed[i];
      }
      return this;
    }

    public Builder typeQuota(StorageType type, long quota) {
      this.typeQuota[type.ordinal()] = quota;
      return this;
    }

    public Builder typeConsumed(StorageType type, long consumed) {
      this.typeConsumed[type.ordinal()] = consumed;
      return this;
    }

    public Builder typeQuota(long[] typeQuota) {
      for (int i = 0; i < typeQuota.length; i++) {
        this.typeQuota[i] = typeQuota[i];
      }
      return this;
    }

    public QuotaUsage build() {
      return new QuotaUsage(this);
    }

    private long fileAndDirectoryCount;
    private long quota;
    private long spaceConsumed;
    private long spaceQuota;
    private long[] typeConsumed;
    private long[] typeQuota;
  }

  // Make it protected for the deprecated ContentSummary constructor.
  protected QuotaUsage() { }

  /** Build the instance based on the builder. */
  protected QuotaUsage(Builder builder) {
    this.fileAndDirectoryCount = builder.fileAndDirectoryCount;
    this.quota = builder.quota;
    this.spaceConsumed = builder.spaceConsumed;
    this.spaceQuota = builder.spaceQuota;
    this.typeConsumed = builder.typeConsumed;
    this.typeQuota = builder.typeQuota;
  }

  protected void setQuota(long quota) {
    this.quota = quota;
  }

  protected void setSpaceConsumed(long spaceConsumed) {
    this.spaceConsumed = spaceConsumed;
  }

  protected void setSpaceQuota(long spaceQuota) {
    this.spaceQuota = spaceQuota;
  }

  /** Return the directory count. */
  public long getFileAndDirectoryCount() {
    return fileAndDirectoryCount;
  }

  /** Return the directory quota. */
  public long getQuota() {
    return quota;
  }

  /** Return (disk) space consumed. */
  public long getSpaceConsumed() {
    return spaceConsumed;
  }

  /** Return (disk) space quota. */
  public long getSpaceQuota() {
    return spaceQuota;
  }

  /** Return storage type quota. */
  public long getTypeQuota(StorageType type) {
    return (typeQuota != null) ? typeQuota[type.ordinal()] : -1;
  }

  /** Return storage type consumed. */
  public long getTypeConsumed(StorageType type) {
    return (typeConsumed != null) ? typeConsumed[type.ordinal()] : 0;
  }

  /** Return storage type quota. */
  private long[] getTypesQuota() {
    return typeQuota;
  }

  /** Return storage type quota. */
  private long[] getTypesConsumed() {
    return typeConsumed;
  }

  /** Return true if any storage type quota has been set. */
  public boolean isTypeQuotaSet() {
    if (typeQuota == null) {
      return false;
    }
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      if (typeQuota[t.ordinal()] > 0) {
        return true;
      }
    }
    return false;
  }

  /** Return true if any storage type consumption information is available. */
  public boolean isTypeConsumedAvailable() {
    if (typeConsumed == null) {
      return false;
    }
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      if (typeConsumed[t.ordinal()] > 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object to) {
    return (this == to || (to instanceof QuotaUsage &&
        getFileAndDirectoryCount() ==
        ((QuotaUsage) to).getFileAndDirectoryCount() &&
        getQuota() == ((QuotaUsage) to).getQuota() &&
        getSpaceConsumed() == ((QuotaUsage) to).getSpaceConsumed() &&
        getSpaceQuota() == ((QuotaUsage) to).getSpaceQuota() &&
        Arrays.equals(getTypesQuota(), ((QuotaUsage) to).getTypesQuota()) &&
        Arrays.equals(getTypesConsumed(),
        ((QuotaUsage) to).getTypesConsumed())));
  }

  @Override
  public int hashCode() {
    long result = (getFileAndDirectoryCount() ^ getQuota() ^
        getSpaceConsumed() ^ getSpaceQuota());
    if (getTypesQuota() != null) {
      for (long quota : getTypesQuota()) {
        result ^= quota;
      }
    }
    if (getTypesConsumed() != null) {
      for (long consumed : getTypesConsumed()) {
        result ^= consumed;
      }
    }
    return (int)result;
  }

  /**
   * Output format:
   * <----12----> <----15----> <----15----> <----15----> <-------18------->
   *    QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM FILE_NAME
   */
  protected static final String QUOTA_STRING_FORMAT = "%12s %15s ";
  protected static final String SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";

  protected static final String[] QUOTA_HEADER_FIELDS = new String[] {"QUOTA",
      "REM_QUOTA", "SPACE_QUOTA", "REM_SPACE_QUOTA"};

  protected static final String QUOTA_HEADER = String.format(
      QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT,
      (Object[]) QUOTA_HEADER_FIELDS);

  /**
   * Output format:
   * <----12----> <------15-----> <------15-----> <------15----->
   *        QUOTA       REM_QUOTA     SPACE_QUOTA REM_SPACE_QUOTA
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE
   */
  private static final String STORAGE_TYPE_SUMMARY_FORMAT = "%13s %17s ";

  /** Return the header of the output.
   * @return the header of the output
   */
  public static String getHeader() {
    return QUOTA_HEADER;
  }

  /** default quota display string */
  private static final String QUOTA_NONE = "none";
  private static final String QUOTA_INF = "inf";

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean hOption) {
    return toString(hOption, false, null);
  }

  /** Return the string representation of the object in the output format.
   * if hOption is false file sizes are returned in bytes
   * if hOption is true file sizes are returned in human readable
   *
   * @param hOption a flag indicating if human readable output if to be used
   * @return the string representation of the object
   */
  public String toString(boolean hOption,
      boolean tOption, List<StorageType> types) {
    if (tOption) {
      return getTypesQuotaUsage(hOption, types);
    }
    return getQuotaUsage(hOption);
  }

  protected String getQuotaUsage(boolean hOption) {
    String quotaStr = QUOTA_NONE;
    String quotaRem = QUOTA_INF;
    String spaceQuotaStr = QUOTA_NONE;
    String spaceQuotaRem = QUOTA_INF;

    if (quota > 0) {
      quotaStr = formatSize(quota, hOption);
      quotaRem = formatSize(quota-fileAndDirectoryCount, hOption);
    }
    if (spaceQuota >= 0) {
      spaceQuotaStr = formatSize(spaceQuota, hOption);
      spaceQuotaRem = formatSize(spaceQuota - spaceConsumed, hOption);
    }

    return String.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT,
        quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
  }

  protected String getTypesQuotaUsage(boolean hOption,
      List<StorageType> types) {
    StringBuffer content = new StringBuffer();
    for (StorageType st : types) {
      long typeQuota = getTypeQuota(st);
      long typeConsumed = getTypeConsumed(st);
      String quotaStr = QUOTA_NONE;
      String quotaRem = QUOTA_INF;

      if (typeQuota >= 0) {
        quotaStr = formatSize(typeQuota, hOption);
        quotaRem = formatSize(typeQuota - typeConsumed, hOption);
      }

      content.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT,
          quotaStr, quotaRem));
    }
    return content.toString();
  }

  /**
   * return the header of with the StorageTypes.
   *
   * @param storageTypes
   * @return storage header string
   */
  public static String getStorageTypeHeader(List<StorageType> storageTypes) {
    StringBuffer header = new StringBuffer();

    for (StorageType st : storageTypes) {
      /* the field length is 13/17 for quota and remain quota
       * as the max length for quota name is ARCHIVE_QUOTA
        * and remain quota name REM_ARCHIVE_QUOTA */
      String storageName = st.toString();
      header.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT,
          storageName + "_QUOTA", "REM_" + storageName + "_QUOTA"));
    }
    return header.toString();
  }

  /**
   * Formats a size to be human readable or in bytes.
   * @param size value to be formatted
   * @param humanReadable flag indicating human readable or not
   * @return String representation of the size
  */
  private String formatSize(long size, boolean humanReadable) {
    return humanReadable
      ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
      : String.valueOf(size);
  }
}
