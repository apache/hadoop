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
  

  /** Constructor */
  public ContentSummary() {}
  
  /** Constructor */
  public ContentSummary(long length, long fileCount, long directoryCount) {
    this(length, fileCount, directoryCount, -1L, length, -1L);
  }

  /** Constructor */
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

  /** @return the length */
  public long getLength() {return length;}

  /** @return the directory count */
  public long getDirectoryCount() {return directoryCount;}

  /** @return the file count */
  public long getFileCount() {return fileCount;}
  
  /** Return the directory quota */
  public long getQuota() {return quota;}
  
  /** Retuns (disk) space consumed */ 
  public long getSpaceConsumed() {return spaceConsumed;}

  /** Returns (disk) space quota */
  public long getSpaceQuota() {return spaceQuota;}
  
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
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME    
   */
  private static final String STRING_FORMAT = "%12s %12s %18s ";
  /** 
   * Output format:
   * <----12----> <----15----> <----15----> <----15----> <----12----> <----12----> <-------18------->
   *    QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM DIR_COUNT   FILE_COUNT   CONTENT_SIZE     FILE_NAME    
   */
  private static final String QUOTA_STRING_FORMAT = "%12s %15s ";
  private static final String SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";
  
  /** The header string */
  private static final String HEADER = String.format(
      STRING_FORMAT.replace('d', 's'), "directories", "files", "bytes");

  private static final String QUOTA_HEADER = String.format(
      QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
      "name quota", "rem name quota", "space quota", "rem space quota") +
      HEADER;
  
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
    String prefix = "";
    if (qOption) {
      String quotaStr = "none";
      String quotaRem = "inf";
      String spaceQuotaStr = "none";
      String spaceQuotaRem = "inf";
      
      if (quota>0) {
        quotaStr = formatSize(quota, hOption);
        quotaRem = formatSize(quota-(directoryCount+fileCount), hOption);
      }
      if (spaceQuota>0) {
        spaceQuotaStr = formatSize(spaceQuota, hOption);
        spaceQuotaRem = formatSize(spaceQuota - spaceConsumed, hOption);
      }
      
      prefix = String.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
                             quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
    }
    
    return prefix + String.format(STRING_FORMAT,
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
