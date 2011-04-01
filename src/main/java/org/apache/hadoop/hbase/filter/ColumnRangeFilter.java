/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

/**
 * This filter is used for selecting only those keys with columns that are
 * between minColumn to maxColumn. For example, if minColumn is 'an', and
 * maxColumn is 'be', it will pass keys with columns like 'ana', 'bad', but not
 * keys with columns like 'bed', 'eye'
 *
 * If minColumn is null, there is no lower bound. If maxColumn is null, there is
 * no upper bound.
 *
 * minColumnInclusive and maxColumnInclusive specify if the ranges are inclusive
 * or not.
 */
public class ColumnRangeFilter extends FilterBase {
  protected byte[] minColumn = null;
  protected boolean minColumnInclusive = true;
  protected byte[] maxColumn = null;
  protected boolean maxColumnInclusive = false;

  public ColumnRangeFilter() {
    super();
  }
  /**
   * Create a filter to select those keys with columns that are between minColumn
   * and maxColumn.
   * @param minColumn minimum value for the column range. If if it's null,
   * there is no lower bound.
   * @param minColumnInclusive if true, include minColumn in the range.
   * @param maxColumn maximum value for the column range. If it's null,
   * @param maxColumnInclusive if true, include maxColumn in the range.
   * there is no upper bound.
   */
  public ColumnRangeFilter(final byte[] minColumn, boolean minColumnInclusive,
      final byte[] maxColumn, boolean maxColumnInclusive) {
    this.minColumn = minColumn;
    this.minColumnInclusive = minColumnInclusive;
    this.maxColumn = maxColumn;
    this.maxColumnInclusive = maxColumnInclusive;
  }

  /**
   * @return if min column range is inclusive.
   */
  public boolean isMinColumnInclusive() {
    return minColumnInclusive;
  }

  /**
   * @return if max column range is inclusive.
   */
  public boolean isMaxColumnInclusive() {
    return maxColumnInclusive;
  }

  /**
   * @return the min column range for the filter
   */
  public byte[] getMinColumn() {
    return this.minColumn;
  }

  /**
   * @return the max column range for the filter
   */
  public byte[] getMaxColumn() {
    return this.maxColumn;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    byte[] buffer = kv.getBuffer();
    int qualifierOffset = kv.getQualifierOffset();
    int qualifierLength = kv.getQualifierLength();
    int cmpMin = 1;

    if (this.minColumn != null) {
      cmpMin = Bytes.compareTo(buffer, qualifierOffset, qualifierLength,
          this.minColumn, 0, this.minColumn.length);
    }

    if (cmpMin < 0) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    if (!this.minColumnInclusive && cmpMin == 0) {
      return ReturnCode.SKIP;
    }

    if (this.maxColumn == null) {
      return ReturnCode.INCLUDE;
    }

    int cmpMax = Bytes.compareTo(buffer, qualifierOffset, qualifierLength,
        this.maxColumn, 0, this.maxColumn.length);

    if (this.maxColumnInclusive && cmpMax <= 0 ||
        !this.maxColumnInclusive && cmpMax < 0) {
      return ReturnCode.INCLUDE;
    }

    return ReturnCode.NEXT_ROW;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // need to write out a flag for null value separately. Otherwise,
    // we will not be able to differentiate empty string and null
    out.writeBoolean(this.minColumn == null);
    Bytes.writeByteArray(out, this.minColumn);
    out.writeBoolean(this.minColumnInclusive);

    out.writeBoolean(this.maxColumn == null);
    Bytes.writeByteArray(out, this.maxColumn);
    out.writeBoolean(this.maxColumnInclusive);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    boolean isMinColumnNull = in.readBoolean();
    this.minColumn = Bytes.readByteArray(in);

    if (isMinColumnNull) {
      this.minColumn = null;
    }

    this.minColumnInclusive = in.readBoolean();

    boolean isMaxColumnNull = in.readBoolean();
    this.maxColumn = Bytes.readByteArray(in);
    if (isMaxColumnNull) {
      this.maxColumn = null;
    }
    this.maxColumnInclusive = in.readBoolean();
  }


  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    return KeyValue.createFirstOnRow(kv.getBuffer(), kv.getRowOffset(), kv
        .getRowLength(), kv.getBuffer(), kv.getFamilyOffset(), kv
        .getFamilyLength(), this.minColumn, 0, this.minColumn == null ? 0
        : this.minColumn.length);
  }
}
