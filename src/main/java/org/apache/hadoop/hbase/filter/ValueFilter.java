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

/**
 * This filter is used to filter based on column value. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * cell value.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} and {@link SkipFilter}
 * to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * To test the value of a single qualifier when scanning multiple qualifiers,
 * use {@link SingleColumnValueFilter}.
 */
public class ValueFilter extends CompareFilter {

  /**
   * Writable constructor, do not use.
   */
  public ValueFilter() {
  }

  /**
   * Constructor.
   * @param valueCompareOp the compare op for value matching
   * @param valueComparator the comparator for value matching
   */
  public ValueFilter(final CompareOp valueCompareOp,
      final WritableByteArrayComparable valueComparator) {
    super(valueCompareOp, valueComparator);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (doCompare(this.compareOp, this.comparator, v.getBuffer(),
        v.getValueOffset(), v.getValueLength())) {
      return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }
}
