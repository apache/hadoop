/**
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

import java.util.ArrayList;

/**
 * This filter is used to filter based on the column family. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * column family portion of a key.
 * <p/>
 * This filter can be wrapped with {@link org.apache.hadoop.hbase.filter.WhileMatchFilter} and {@link org.apache.hadoop.hbase.filter.SkipFilter}
 * to add more control.
 * <p/>
 * Multiple filters can be combined using {@link org.apache.hadoop.hbase.filter.FilterList}.
 * <p/>
 * If an already known column family is looked for, use {@link org.apache.hadoop.hbase.client.Get#addFamily(byte[])}
 * directly rather than a filter.
 */
public class FamilyFilter extends CompareFilter {
  /**
   * Writable constructor, do not use.
   */
  public FamilyFilter() {
  }

  /**
   * Constructor.
   *
   * @param familyCompareOp  the compare op for column family matching
   * @param familyComparator the comparator for column family matching
   */
  public FamilyFilter(final CompareOp familyCompareOp,
                      final WritableByteArrayComparable familyComparator) {
      super(familyCompareOp, familyComparator);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    int familyLength = v.getFamilyLength();
    if (familyLength > 0) {
      if (doCompare(this.compareOp, this.comparator, v.getBuffer(),
          v.getFamilyOffset(), familyLength)) {
        return ReturnCode.SKIP;
      }
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public Filter createFilterFromArguments (ArrayList<byte []> filterArguments) {
    return super.createFilterFromArguments(filterArguments);
  }
}
