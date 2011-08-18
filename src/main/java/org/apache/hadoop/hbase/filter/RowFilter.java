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

import java.util.List;
import java.util.ArrayList;

/**
 * This filter is used to filter based on the key. It takes an operator
 * (equal, greater, not equal, etc) and a byte [] comparator for the row,
 * and column qualifier portions of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known row range needs to be scanned, use {@link Scan} start
 * and stop rows directly rather than a filter.
 */
public class RowFilter extends CompareFilter {

  private boolean filterOutRow = false;

  /**
   * Writable constructor, do not use.
   */
  public RowFilter() {
    super();
  }

  /**
   * Constructor.
   * @param rowCompareOp the compare op for row matching
   * @param rowComparator the comparator for row matching
   */
  public RowFilter(final CompareOp rowCompareOp,
      final WritableByteArrayComparable rowComparator) {
    super(rowCompareOp, rowComparator);
  }

  @Override
  public void reset() {
    this.filterOutRow = false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if(this.filterOutRow) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRowKey(byte[] data, int offset, int length) {
    if(doCompare(this.compareOp, this.comparator, data, offset, length)) {
      this.filterOutRow = true;
    }
    return this.filterOutRow;
  }

  @Override
  public boolean filterRow() {
    return this.filterOutRow;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList arguments = CompareFilter.extractArguments(filterArguments);
    CompareOp compareOp = (CompareOp)arguments.get(0);
    WritableByteArrayComparable comparator = (WritableByteArrayComparable)arguments.get(1);
    return new RowFilter(compareOp, comparator);
  }
}