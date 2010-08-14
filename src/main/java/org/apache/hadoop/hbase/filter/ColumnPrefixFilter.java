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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;

/**
 * This filter is used for selecting only those keys with columns that matches
 * a particular prefix. For example, if prefix is 'an', it will pass keys will
 * columns like 'and', 'anti' but not keys with columns like 'ball', 'act'.
 */
public class ColumnPrefixFilter extends FilterBase {
  protected byte [] prefix = null;

  public ColumnPrefixFilter() {
    super();
  }

  public ColumnPrefixFilter(final byte [] prefix) {
    this.prefix = prefix;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (this.prefix == null || kv.getBuffer() == null) {
      return ReturnCode.INCLUDE;
    } else {
      return filterColumn(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    }
  }

  public ReturnCode filterColumn(byte[] buffer, int qualifierOffset, int qualifierLength) {
    if (qualifierLength < prefix.length) {
      int cmp = Bytes.compareTo(buffer, qualifierOffset, qualifierLength, this.prefix, 0,
          qualifierLength);
      if (cmp <= 0) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      } else {
        return ReturnCode.NEXT_ROW;
      }
    } else {
      int cmp = Bytes.compareTo(buffer, qualifierOffset, this.prefix.length, this.prefix, 0,
          this.prefix.length);
      if (cmp < 0) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      } else if (cmp > 0) {
        return ReturnCode.NEXT_ROW;
      } else {
        return ReturnCode.INCLUDE;
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.prefix);
  }

  public void readFields(DataInput in) throws IOException {
    this.prefix = Bytes.readByteArray(in);
  }

  public KeyValue getNextKeyHint(KeyValue kv) {
    return KeyValue.createFirstOnRow(
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
        kv.getFamilyOffset(), kv.getFamilyLength(), prefix, 0, prefix.length);
  }
}