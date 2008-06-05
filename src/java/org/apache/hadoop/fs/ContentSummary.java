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

import org.apache.hadoop.io.Writable;

/** Store the summary of a content (a directory or a file). */
public class ContentSummary implements Writable{
  private long length;
  private long fileCount;
  private long directoryCount;

  /** Constructor */
  public ContentSummary() {}
  
  /** Constructor */
  public ContentSummary(long length, long fileCount, long directoryCount) {
    this.length = length;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
  }

  /** @return the length */
  public long getLength() {return length;}

  /** @return the directory count */
  public long getDirectoryCount() {return directoryCount;}

  /** @return the file count */
  public long getFileCount() {return fileCount;}
  
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(fileCount);
    out.writeLong(directoryCount);
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.length = in.readLong();
    this.fileCount = in.readLong();
    this.directoryCount = in.readLong();
  }
  
  /** 
   * Output format:
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME    
   */
  private static final String STRING_FORMAT = "%12d %12d %18d ";

  /** The header string */
  static String HEADER = String.format(
      STRING_FORMAT.replace('d', 's'), "directories", "files", "bytes");

  /** {@inheritDoc} */
  public String toString() {
    return String.format(STRING_FORMAT, directoryCount, fileCount, length);
  }
}