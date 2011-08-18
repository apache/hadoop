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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import java.util.ArrayList;

/**
 * A {@link Filter} that checks a single column value, but does not emit the
 * tested column. This will enable a performance boost over
 * {@link SingleColumnValueFilter}, if the tested column value is not actually
 * needed as input (besides for the filtering itself).
 */
public class SingleColumnValueExcludeFilter extends SingleColumnValueFilter {

  /**
   * Writable constructor, do not use.
   */
  public SingleColumnValueExcludeFilter() {
    super();
  }

  /**
   * Constructor for binary compare of the value of a single column. If the
   * column is found and the condition passes, all columns of the row will be
   * emitted; except for the tested column value. If the column is not found or
   * the condition fails, the row will not be emitted.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public SingleColumnValueExcludeFilter(byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value) {
    super(family, qualifier, compareOp, value);
  }

  /**
   * Constructor for binary compare of the value of a single column. If the
   * column is found and the condition passes, all columns of the row will be
   * emitted; except for the tested column value. If the condition fails, the
   * row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   */
  public SingleColumnValueExcludeFilter(byte[] family, byte[] qualifier,
      CompareOp compareOp, WritableByteArrayComparable comparator) {
    super(family, qualifier, compareOp, comparator);
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    ReturnCode superRetCode = super.filterKeyValue(keyValue);
    if (superRetCode == ReturnCode.INCLUDE) {
      // If the current column is actually the tested column,
      // we will skip it instead.
      if (keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
        return ReturnCode.SKIP;
      }
    }
    return superRetCode;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    SingleColumnValueFilter tempFilter = (SingleColumnValueFilter)
      SingleColumnValueFilter.createFilterFromArguments(filterArguments);
    SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter (
      tempFilter.getFamily(), tempFilter.getQualifier(),
      tempFilter.getOperator(), tempFilter.getComparator());

    if (filterArguments.size() == 6) {
      filter.setFilterIfMissing(tempFilter.getFilterIfMissing());
      filter.setLatestVersionOnly(tempFilter.getLatestVersionOnly());
}
    return filter;
  }
}
