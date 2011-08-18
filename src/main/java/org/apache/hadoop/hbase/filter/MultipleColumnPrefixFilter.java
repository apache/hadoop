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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.ArrayList;

/**
 * This filter is used for selecting only those keys with columns that matches
 * a particular prefix. For example, if prefix is 'an', it will pass keys will
 * columns like 'and', 'anti' but not keys with columns like 'ball', 'act'.
 */
public class MultipleColumnPrefixFilter extends FilterBase {
  protected byte [] hint = null;
  protected TreeSet<byte []> sortedPrefixes = createTreeSet();

  public MultipleColumnPrefixFilter() {
    super();
  }

  public MultipleColumnPrefixFilter(final byte [][] prefixes) {
    if (prefixes != null) {
      for (int i = 0; i < prefixes.length; i++) {
        if (!sortedPrefixes.add(prefixes[i]))
          throw new IllegalArgumentException ("prefixes must be distinct");
      }
    }
  }

  public byte [][] getPrefix() {
    int count = 0;
    byte [][] temp = new byte [sortedPrefixes.size()][];
    for (byte [] prefixes : sortedPrefixes) {
      temp [count++] = prefixes;
    }
    return temp;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (sortedPrefixes.size() == 0 || kv.getBuffer() == null) {
      return ReturnCode.INCLUDE;
    } else {
      return filterColumn(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    }
  }

  public ReturnCode filterColumn(byte[] buffer, int qualifierOffset, int qualifierLength) {
    byte [] qualifier = Arrays.copyOfRange(buffer, qualifierOffset,
                                           qualifierLength + qualifierOffset);
    TreeSet<byte []> lesserOrEqualPrefixes =
      (TreeSet<byte []>) sortedPrefixes.headSet(qualifier, true);

    if (lesserOrEqualPrefixes.size() != 0) {
      byte [] largestPrefixSmallerThanQualifier = lesserOrEqualPrefixes.last();
      
      if (Bytes.startsWith(qualifier, largestPrefixSmallerThanQualifier)) {
        return ReturnCode.INCLUDE;
      }
      
      if (lesserOrEqualPrefixes.size() == sortedPrefixes.size()) {
        return ReturnCode.NEXT_ROW;
      } else {
        hint = sortedPrefixes.higher(largestPrefixSmallerThanQualifier);
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
    } else {
      hint = sortedPrefixes.first();
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    byte [][] prefixes = new byte [filterArguments.size()][];
    for (int i = 0 ; i < filterArguments.size(); i++) {
      byte [] columnPrefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(i));
      prefixes[i] = columnPrefix;
    }
    return new MultipleColumnPrefixFilter(prefixes);
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(sortedPrefixes.size());
    for (byte [] element : sortedPrefixes) {
      Bytes.writeByteArray(out, element);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int x = in.readInt();
    this.sortedPrefixes = createTreeSet();
    for (int j = 0; j < x; j++) {
      sortedPrefixes.add(Bytes.readByteArray(in));
    }
  }

  public KeyValue getNextKeyHint(KeyValue kv) {
    return KeyValue.createFirstOnRow(
      kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
      kv.getFamilyOffset(), kv.getFamilyLength(), hint, 0, hint.length);
  }

  public TreeSet<byte []> createTreeSet() {
    return new TreeSet<byte []>(new Comparator<Object>() {
        @Override
          public int compare (Object o1, Object o2) {
          if (o1 == null || o2 == null)
            throw new IllegalArgumentException ("prefixes can't be null");

          byte [] b1 = (byte []) o1;
          byte [] b2 = (byte []) o2;
          return Bytes.compareTo (b1, 0, b1.length, b2, 0, b2.length);
        }
      });
  }
}
