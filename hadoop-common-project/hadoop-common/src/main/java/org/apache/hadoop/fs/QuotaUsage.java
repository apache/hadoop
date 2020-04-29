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
      this.quota = -1L;
      this.spaceQuota = -1L;

      typeConsumed = new long[StorageType.values().length];
      typeQuota = new long[StorageType.values().length];

      Arrays.fill(typeQuota, -1L);
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
      System.arraycopy(typeConsumed, 0, this.typeConsumed, 0,
          typeConsumed.length);
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
      System.arraycopy(typeQuota, 0, this.typeQuota, 0, typeQuota.length);
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
    return (typeQuota != null) ? typeQuota[type.ordinal()] : -1L;
  }

  /** Return storage type consumed. */
  public long getTypeConsumed(StorageType type) {
    return (typeConsumed != null) ? typeConsumed[type.ordinal()] : 0L;
  }

  /** Return true if any storage type quota has been set. */
  public boolean isTypeQuotaSet() {
    if (typeQuota != null) {
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        if (typeQuota[t.ordinal()] > 0L) {
          return true;
        }
      }
    }
    return false;
  }

  /** Return true if any storage type consumption information is available. */
  public boolean isTypeConsumedAvailable() {
    if (typeConsumed != null) {
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        if (typeConsumed[t.ordinal()] > 0L) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + (int) (fileAndDirectoryCount ^ (fileAndDirectoryCount >>> 32));
    result = prime * result + (int) (quota ^ (quota >>> 32));
    result = prime * result + (int) (spaceConsumed ^ (spaceConsumed >>> 32));
    result = prime * result + (int) (spaceQuota ^ (spaceQuota >>> 32));
    result = prime * result + Arrays.hashCode(typeConsumed);
    result = prime * result + Arrays.hashCode(typeQuota);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof QuotaUsage)) {
      return false;
    }
    QuotaUsage other = (QuotaUsage) obj;
    if (fileAndDirectoryCount != other.fileAndDirectoryCount) {
      return false;
    }
    if (quota != other.quota) {
      return false;
    }
    if (spaceConsumed != other.spaceConsumed) {
      return false;
    }
    if (spaceQuota != other.spaceQuota) {
      return false;
    }
    if (!Arrays.equals(typeConsumed, other.typeConsumed)) {
      return false;
    }
    if (!Arrays.equals(typeQuota, other.typeQuota)) {
      return false;
    }
    return true;
  }

  /**
   * Output format:
   * |----12----| |----15----| |----15----| |----15----| |-------18-------|
   *    QUOTA   REMAINING_QUOTA SPACE_QUOTA SPACE_QUOTA_REM FILE_NAME
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
   * |----12----| |------15-----| |------15-----| |------15-----|
   *        QUOTA       REM_QUOTA     SPACE_QUOTA REM_SPACE_QUOTA
   * |----12----| |----12----| |-------18-------|
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

    if (quota > 0L) {
      quotaStr = formatSize(quota, hOption);
      quotaRem = formatSize(quota-fileAndDirectoryCount, hOption);
    }
    if (spaceQuota >= 0L) {
      spaceQuotaStr = formatSize(spaceQuota, hOption);
      spaceQuotaRem = formatSize(spaceQuota - spaceConsumed, hOption);
    }

    return String.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT,
        quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
  }

  protected String getTypesQuotaUsage(boolean hOption,
      List<StorageType> types) {
    StringBuilder content = new StringBuilder();
    for (StorageType st : types) {
      long typeQuota = getTypeQuota(st);
      long typeConsumed = getTypeConsumed(st);
      String quotaStr = QUOTA_NONE;
      String quotaRem = QUOTA_INF;

      if (typeQuota >= 0L) {
        quotaStr = formatSize(typeQuota, hOption);
        quotaRem = formatSize(typeQuota - typeConsumed, hOption);
      }

      content.append(
          String.format(STORAGE_TYPE_SUMMARY_FORMAT, quotaStr, quotaRem));
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
    StringBuilder header = new StringBuilder();

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
