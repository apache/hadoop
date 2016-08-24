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
public class ContentSummary extends QuotaUsage implements Writable{
  private long length;
  private long fileCount;
  private long directoryCount;
  // These fields are to track the snapshot-related portion of the values.
  private long snapshotLength;
  private long snapshotFileCount;
  private long snapshotDirectoryCount;
  private long snapshotSpaceConsumed;

  /** We don't use generics. Instead override spaceConsumed and other methods
      in order to keep backward compatibility. */
  public static class Builder extends QuotaUsage.Builder {
    public Builder() {
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

    public Builder snapshotLength(long snapshotLength) {
      this.snapshotLength = snapshotLength;
      return this;
    }

    public Builder snapshotFileCount(long snapshotFileCount) {
      this.snapshotFileCount = snapshotFileCount;
      return this;
    }

    public Builder snapshotDirectoryCount(long snapshotDirectoryCount) {
      this.snapshotDirectoryCount = snapshotDirectoryCount;
      return this;
    }

    public Builder snapshotSpaceConsumed(long snapshotSpaceConsumed) {
      this.snapshotSpaceConsumed = snapshotSpaceConsumed;
      return this;
    }

    @Override
    public Builder quota(long quota){
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

    @Override
    public Builder typeConsumed(long typeConsumed[]) {
      super.typeConsumed(typeConsumed);
      return this;
    }

    @Override
    public Builder typeQuota(StorageType type, long quota) {
      super.typeQuota(type, quota);
      return this;
    }

    @Override
    public Builder typeConsumed(StorageType type, long consumed) {
      super.typeConsumed(type, consumed);
      return this;
    }

    @Override
    public Builder typeQuota(long typeQuota[]) {
      super.typeQuota(typeQuota);
      return this;
    }

    public ContentSummary build() {
      // Set it in case applications call QuotaUsage#getFileAndDirectoryCount.
      super.fileAndDirectoryCount(this.fileCount + this.directoryCount);
      return new ContentSummary(this);
    }

    private long length;
    private long fileCount;
    private long directoryCount;
    private long snapshotLength;
    private long snapshotFileCount;
    private long snapshotDirectoryCount;
    private long snapshotSpaceConsumed;
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
    setQuota(quota);
    setSpaceConsumed(spaceConsumed);
    setSpaceQuota(spaceQuota);
  }

  /** Constructor for ContentSummary.Builder*/
  private ContentSummary(Builder builder) {
    super(builder);
    this.length = builder.length;
    this.fileCount = builder.fileCount;
    this.directoryCount = builder.directoryCount;
    this.snapshotLength = builder.snapshotLength;
    this.snapshotFileCount = builder.snapshotFileCount;
    this.snapshotDirectoryCount = builder.snapshotDirectoryCount;
    this.snapshotSpaceConsumed = builder.snapshotSpaceConsumed;
  }

  /** @return the length */
  public long getLength() {return length;}

  public long getSnapshotLength() {
    return snapshotLength;
  }

  /** @return the directory count */
  public long getDirectoryCount() {return directoryCount;}

  public long getSnapshotDirectoryCount() {
    return snapshotDirectoryCount;
  }

  /** @return the file count */
  public long getFileCount() {return fileCount;}

  public long getSnapshotFileCount() {
    return snapshotFileCount;
  }

  public long getSnapshotSpaceConsumed() {
    return snapshotSpaceConsumed;
  }

  @Override
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(fileCount);
    out.writeLong(directoryCount);
    out.writeLong(getQuota());
    out.writeLong(getSpaceConsumed());
    out.writeLong(getSpaceQuota());
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    this.length = in.readLong();
    this.fileCount = in.readLong();
    this.directoryCount = in.readLong();
    setQuota(in.readLong());
    setSpaceConsumed(in.readLong());
    setSpaceQuota(in.readLong());
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    } else if (to instanceof ContentSummary) {
      ContentSummary right = (ContentSummary) to;
      return getLength() == right.getLength() &&
          getFileCount() == right.getFileCount() &&
          getDirectoryCount() == right.getDirectoryCount() &&
          getSnapshotLength() == right.getSnapshotLength() &&
          getSnapshotFileCount() == right.getSnapshotFileCount() &&
          getSnapshotDirectoryCount() == right.getSnapshotDirectoryCount() &&
          getSnapshotSpaceConsumed() == right.getSnapshotSpaceConsumed() &&
          super.equals(to);
    } else {
      return super.equals(to);
    }
  }

  @Override
  public int hashCode() {
    long result = getLength() ^ getFileCount() ^ getDirectoryCount()
        ^ getSnapshotLength() ^ getSnapshotFileCount()
        ^ getSnapshotDirectoryCount() ^ getSnapshotSpaceConsumed();
    return ((int) result) ^ super.hashCode();
  }

  /**
   * Output format:
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE
   */
  private static final String SUMMARY_FORMAT = "%12s %12s %18s ";

  private static final String[] SUMMARY_HEADER_FIELDS =
      new String[] {"DIR_COUNT", "FILE_COUNT", "CONTENT_SIZE"};

  /** The header string */
  private static final String SUMMARY_HEADER = String.format(
      SUMMARY_FORMAT, (Object[]) SUMMARY_HEADER_FIELDS);

  private static final String ALL_HEADER = QUOTA_HEADER + SUMMARY_HEADER;


  /** Return the header of the output.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @return the header of the output
   */
  public static String getHeader(boolean qOption) {
    return qOption ? ALL_HEADER : SUMMARY_HEADER;
  }



  /**
   * Returns the names of the fields from the summary header.
   * 
   * @return names of fields as displayed in the header
   */
  public static String[] getHeaderFields() {
    return SUMMARY_HEADER_FIELDS;
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
  @Override
  public String toString(boolean qOption) {
    return toString(qOption, false);
  }

  /** Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   * 
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption) {
    return toString(qOption, hOption, false, null);
  }

  /** Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output is to be used
   * @param xOption a flag indicating if calculation from snapshots is to be
   *                included in the output
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption, boolean xOption) {
    return toString(qOption, hOption, false, xOption, null);
  }

  /**
   * Return the string representation of the object in the output format.
   * For description of the options,
   * @see #toString(boolean, boolean, boolean, boolean, List)
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output if to be used
   * @param tOption a flag indicating if display quota by storage types
   * @param types Storage types to display
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption,
                         boolean tOption, List<StorageType> types) {
    return toString(qOption, hOption, tOption, false, types);
  }

  /** Return the string representation of the object in the output format.
   * if qOption is false, output directory count, file count, and content size;
   * if qOption is true, output quota and remaining quota as well.
   * if hOption is false, file sizes are returned in bytes
   * if hOption is true, file sizes are returned in human readable
   * if tOption is true, display the quota by storage types
   * if tOption is false, same logic with #toString(boolean,boolean)
   * if xOption is false, output includes the calculation from snapshots
   * if xOption is true, output excludes the calculation from snapshots
   *
   * @param qOption a flag indicating if quota needs to be printed or not
   * @param hOption a flag indicating if human readable output is to be used
   * @param tOption a flag indicating if display quota by storage types
   * @param xOption a flag indicating if calculation from snapshots is to be
   *                included in the output
   * @param types Storage types to display
   * @return the string representation of the object
   */
  public String toString(boolean qOption, boolean hOption, boolean tOption,
      boolean xOption, List<StorageType> types) {
    String prefix = "";

    if (tOption) {
      return getTypesQuotaUsage(hOption, types);
    }

    if (qOption) {
      prefix = getQuotaUsage(hOption);
    }

    if (xOption) {
      return prefix + String.format(SUMMARY_FORMAT,
          formatSize(directoryCount - snapshotDirectoryCount, hOption),
          formatSize(fileCount - snapshotFileCount, hOption),
          formatSize(length - snapshotLength, hOption));
    } else {
      return prefix + String.format(SUMMARY_FORMAT,
          formatSize(directoryCount, hOption),
          formatSize(fileCount, hOption),
          formatSize(length, hOption));
    }
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
