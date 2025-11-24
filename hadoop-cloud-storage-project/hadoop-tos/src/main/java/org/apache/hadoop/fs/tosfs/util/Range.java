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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;

import java.util.List;
import java.util.Objects;

public final class Range {
  private final long off;
  private final long len;

  private Range(long off, long len) {
    this.off = off;
    this.len = len;
  }

  public static Range of(long off, long len) {
    return new Range(off, len);
  }

  public long off() {
    return off;
  }

  public long len() {
    return len;
  }

  public long end() {
    return off + len;
  }

  public boolean include(long pos) {
    return pos >= off && pos < off + len;
  }

  public boolean overlap(Range r) {
    return r.off() < end() && off() < r.end();
  }

  @Override
  public int hashCode() {
    return Objects.hash(off, len);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Range)) {
      return false;
    }

    Range that = (Range) o;
    return Objects.equals(off, that.off)
        && Objects.equals(len, that.len);
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offset", off)
        .add("length", len)
        .toString();
  }

  public static List<Range> split(long totalSize, long width) {
    Preconditions.checkArgument(totalSize >= 0, "Size %s must be >= 0", totalSize);
    Preconditions.checkArgument(width > 0, "Width %s must be positive", width);

    long remain = totalSize % width;
    long rangeNum = totalSize / width;

    List<Range> ranges = Lists.newArrayListWithCapacity((int) rangeNum + (remain == 0 ? 0 : 1));
    for (int i = 0; i < rangeNum; i++) {
      ranges.add(Range.of(i * width, width));
    }

    if (remain > 0) {
      ranges.add(Range.of(totalSize - remain, remain));
    }

    return ranges;
  }
}
