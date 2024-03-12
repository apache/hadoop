/*
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

package org.apache.hadoop.fs.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileRange;

import java.util.ArrayList;
import java.util.List;

/**
 * A file range that represents a set of underlying file ranges.
 * This is used when we combine the user's FileRange objects
 * together into a single read for efficiency.
 * <p>
 * This class is not part of the public API; it MAY BE used as a parameter
 * to vector IO operations in FileSystem implementation code (and is)
 */
@InterfaceAudience.Private
public class CombinedFileRange extends FileRangeImpl {
  private final List<FileRange> underlying = new ArrayList<>();

  /**
   * Total size of the data in the underlying ranges.
   */
  private long dataSize;

  public CombinedFileRange(long offset, long end, FileRange original) {
    super(offset, (int) (end - offset), null);
    append(original);
  }

  /**
   * Add a range to the underlying list; update
   * the {@link #dataSize} field in the process.
   * @param range range.
   */
  private void append(final FileRange range) {
    this.underlying.add(range);
    dataSize += range.getLength();
  }

  /**
   * Get the list of ranges that were merged together to form this one.
   * @return the list of input ranges
   */
  public List<FileRange> getUnderlying() {
    return underlying;
  }

  /**
   * Merge this input range into the current one, if it is compatible.
   * It is assumed that otherOffset is greater or equal the current offset,
   * which typically happens by sorting the input ranges on offset.
   * @param otherOffset the offset to consider merging
   * @param otherEnd the end to consider merging
   * @param other the underlying FileRange to add if we merge
   * @param minSeek the minimum distance that we'll seek without merging the
   *                ranges together
   * @param maxSize the maximum size that we'll merge into a single range
   * @return true if we have merged the range into this one
   */
  public boolean merge(long otherOffset, long otherEnd, FileRange other,
                       int minSeek, int maxSize) {
    long end = this.getOffset() + this.getLength();
    long newEnd = Math.max(end, otherEnd);
    if (otherOffset - end >= minSeek || newEnd - this.getOffset() > maxSize) {
      return false;
    }
    this.setLength((int) (newEnd - this.getOffset()));
    append(other);
    return true;
  }

  @Override
  public String toString() {
    return super.toString()
        + String.format("; range count=%d, data size=%,d",
          underlying.size(), dataSize);
  }

  /**
   * Get the total amount of data which is actually useful;
   * the difference between this and {@link #getLength()} records
   * how much data which will be discarded.
   * @return a number greater than 0 and less than or equal to {@link #getLength()}.
   */
  public long getDataSize() {
    return dataSize;
  }
}
