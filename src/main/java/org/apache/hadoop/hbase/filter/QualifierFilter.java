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
import org.apache.hadoop.hbase.client.Get;

import java.util.ArrayList;

/**
 * This filter is used to filter based on the column qualifier. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * column qualifier portion of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} and {@link SkipFilter}
 * to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known column qualifier is looked for, use {@link Get#addColumn}
 * directly rather than a filter.
 */
public class QualifierFilter extends CompareFilter {

  /**
   * Writable constructor, do not use.
   */
  public QualifierFilter() {
  }

  /**
   * Constructor.
   * @param op the compare op for column qualifier matching
   * @param qualifierComparator the comparator for column qualifier matching
   */
  public QualifierFilter(final CompareOp op,
      final WritableByteArrayComparable qualifierComparator) {
    super(op, qualifierComparator);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    int qualifierLength = v.getQualifierLength();
    if (qualifierLength > 0) {
      if (doCompare(this.compareOp, this.comparator, v.getBuffer(),
          v.getQualifierOffset(), qualifierLength)) {
        return ReturnCode.SKIP;
      }
    }
    return ReturnCode.INCLUDE;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList arguments = CompareFilter.extractArguments(filterArguments);
    CompareOp compareOp = (CompareOp)arguments.get(0);
    WritableByteArrayComparable comparator = (WritableByteArrayComparable)arguments.get(1);
    return new QualifierFilter(compareOp, comparator);
}
}
