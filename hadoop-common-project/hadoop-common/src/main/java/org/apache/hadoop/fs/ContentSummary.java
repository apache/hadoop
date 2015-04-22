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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/** Store the summary of a content (a directory or a file). */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ContentSummary implements Writable{
  private long length;
  private long fileCount;
  private long directoryCount;
  private long quota;
  private long spaceConsumed;
  private long spaceQuota;
  private long typeConsumed[];
  private long typeQuota[];

  public static class Builder{
    public Builder() {
      this.quota = -1;
      this.spaceQuota = -1;

      typeConsumed = new long[StorageType.values().length];
      typeQuota = new long[StorageType.values().length];
      for (int i = 0; i < typeQuota.length; i++) {
        typeQuota[i] = -1;
      }
    }

    public Builder length(long length) {
      this.length = length;
      return this;
    }

    public Builder fileCount(long fileCount) {
      this.fileCount = fileCount;
      return this;
    }

    public Builder directoryCount(long directoryCount) {
      this.directoryCount = directoryCount;
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

    public Builder typeConsumed(long typeConsumed[]) {
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

    public Builder typeQuota(long typeQuota[]) {
      for (int i = 0; i < typeQuota.length; i++) {
        this.typeQuota[i] = typeQuota[i];
      }
      return this;
    }

    public ContentSummary build() {
      return new ContentSummary(length, fileCount, directoryCount, quota,
          spaceConsumed, spaceQuota, typeConsumed, typeQuota);
    }

    private long length;
    private long fileCount;
    private long directoryCount;
    private long quota;
    private long spaceConsumed;
    private long spaceQuota;
    private long typeConsumed[];
    private long typeQuota[];
  }

  /** Constructor deprecated by ContentSummary.Builder*/
  @Deprecated
  public ContentSummary() {}
  
  /** Constructor, deprecated by ContentSummary.Builder
   *  This constructor implicitly set spaceConsumed the same as length.
   *  spaceConsumed and length must be set explicitly with
   *  ContentSummary.Builder
   * */
  @Deprecated
  public ContentSummary(long length, long fileCount, long directoryCount) {
    this(length, fileCount, directoryCount, -1L, length, -1L);
  }

  /** Constructor, deprecated by ContentSummary.Builder */
  @Deprecated
  public ContentSummary(
      long length, long fileCount, long directoryCount, long quota,
      long spaceConsumed, long spaceQuota) {
    this.length = length;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
    this.quota = quota;
    this.spaceConsumed = spaceConsumed;
    this.spaceQuota = spaceQuota;
  }

  /** Constructor for ContentSummary.Builder*/
  private ContentSummary(
      long length, long fileCount, long directoryCount, long quota,
      long spaceConsumed, long spaceQuota, long typeConsumed[],
      long typeQuota[]) {
    this.length = length;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
    this.quota = quota;
    this.spaceConsumed = spaceConsumed;
    this.spaceQuota = spaceQuota;
    this.typeConsumed = typeConsumed;
    this.typeQuota = typeQuota;
  }

  /** @return the length */
  public long getLength() {return length;}

  /** @return the directory count */
  public long getDirectoryCount() {return directoryCount;}

  /** @return the file count */
  public long getFileCount() {return fileCount;}
  
  /** Return the directory quota */
  public long getQuota() {return quota;}
  
  /** Returns storage space consumed */
  public long getSpaceConsumed() {return spaceConsumed;}

  /** Returns storage space quota */
  public long getSpaceQuota() {return spaceQuota;}

  /** Returns storage type quota */
  public long getTypeQuota(StorageType type) {
    return (typeQuota != null) ? typeQuota[type.ordinal()] : -1;
  }

  /** Returns storage type consumed*/
  public long getTypeConsumed(StorageType type) {
    return (typeConsumed != null) ? typeConsumed[type.ordinal()] : 0;
  }

  /** Returns true if any storage type quota has been set*/
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

  /** Returns true if any storage type consumption information is available*/
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
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(fileCount);
    out.writeLong(directoryCount);
    out.writeLong(quota);
    out.writeLong(spaceConsumed);
    out.writeLong(spaceQuota);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    this.length = in.readLong();
    this.fileCount = in.readLong();
    this.directoryCount = in.readLong();
    this.quota = in.readLong();
    this.spaceConsumed = in.readLong();
    this.spaceQuota = in.readLong();
  }

  /**
   * Output format:
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE
   */
  private static final String SUMMARY_FORMAT = "%12s %12s %18s ";
  /**
   * Output format:
   * <----12----> <------15-----> <------15-----> <------15----->
   *        QUOTA       REM_QUOTA     SPACE_QUOTA REM_SPACE_QUOTA
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE
   */
  private static final String QUOTA_SUMMARY_FORMAT = "%12s %15s ";
  private static final String SPACE_QUOTA_SUMMARY_FORMAT = "%15s %15s ";

  private static final String STORAGE_TYPE_SUMMARY_FORMAT = "%13s %17s ";

  private static final String[] HEADER_FIELDS = new String[] { "DIR_COUNT",
      "FILE_COUNT", "CONTENT_SIZE"};
  private static final String[] QUOTA_HEADER_FIELDS = new String[] { "QUOTA",
      "REM_QUOTA", "SPACE_QUOTA", "REM_SPACE_QUOTA" };

  /** The header string */
  private static final String HEADER = String.format(
      SUMMARY_FORMAT, (Object[]) HEADER_FIELDS);

  private static final String QUOTA_HEADER = String.format(
      QUOTA_SUMMARY_FORMAT + SPACE_QUOTA_SUMMARY_FORMAT,
      (Object[]) QUOTA_HEADER_FIELDS) +
      HEADER;

  /** default quota display string */
  private static final String QUOTA_NONE = "none";
  private static final String QUOTA_INF = "inf";

  /** Return the header of the output.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the header of the output
   */
  public static String getHeader(boolean qOption) {
    return qOption ? QUOTA_HEADER : HEADER;
  }

  /**
   * return the header of with the StorageTypes
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
      header.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT, storageName + "_QUOTA",
          "REM_" + storageName + "_QUOTA"));
    }
    return header.toString();
  }

  /**
   * Returns the names of the fields from the summary header.
   * 
   * @return names of fields as displayed in the header
   */
  public static String[] getHeaderFields() {
    return HEADER_FIELDS;
  }

  /**
   * Returns the names of the fields used in the quota summary.
   * 
   * @return names of quota fields as displayed in the header
   */
  public static String[] getQuotaHeaderFields() {
    return QUOTA_HEADER_FIELDS;
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the string representation of the object
  */
  public String toString(boolean qOption) {
    return toString(qOption, false);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * if hOption is false file sizes are returned in bytes
   * if hOption is true file sizes are returned in human readable 
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption) {
    return toString(qOption, hOption, false, null);
  }

  /**
   * Return the string representation of the object in the output format.
   * if tOption is true, display the quota by storage types,
   * Otherwise, same logic with #toString(boolean,boolean)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @param tOption a flag indicating if display quota by storage types
   * @param types Storage types to display
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption,
                         boolean tOption, List<StorageType> types) {
    String prefix = "";

    if (tOption) {
      StringBuffer content = new StringBuffer();
      for (StorageType st : types) {
        long typeQuota = getTypeQuota(st);
        long typeConsumed = getTypeConsumed(st);
        String quotaStr = QUOTA_NONE;
        String quotaRem = QUOTA_INF;

        if (typeQuota > 0) {
          quotaStr = formatSize(typeQuota, hOption);
          quotaRem = formatSize(typeQuota - typeConsumed, hOption);
        }

        content.append(String.format(STORAGE_TYPE_SUMMARY_FORMAT,
            quotaStr, quotaRem));
      }
      return content.toString();
    }

    if (qOption) {
      String quotaStr = QUOTA_NONE;
      String quotaRem = QUOTA_INF;
      String spaceQuotaStr = QUOTA_NONE;
      String spaceQuotaRem = QUOTA_INF;

      if (quota>0) {
        quotaStr = formatSize(quota, hOption);
        quotaRem = formatSize(quota-(directoryCount+fileCount), hOption);
      }
      if (spaceQuota>0) {
        spaceQuotaStr = formatSize(spaceQuota, hOption);
        spaceQuotaRem = formatSize(spaceQuota - spaceConsumed, hOption);
      }

      prefix = String.format(QUOTA_SUMMARY_FORMAT + SPACE_QUOTA_SUMMARY_FORMAT,
          quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
    }

    return prefix + String.format(SUMMARY_FORMAT,
        formatSize(directoryCount, hOption),
        formatSize(fileCount, hOption),
        formatSize(length, hOption));
  }

  /**
   * Formats a size to be human readable or in bytes
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
