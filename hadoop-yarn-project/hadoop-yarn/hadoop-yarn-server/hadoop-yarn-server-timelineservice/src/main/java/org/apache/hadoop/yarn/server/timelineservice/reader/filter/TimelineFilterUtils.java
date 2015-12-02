/**
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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.hbase.filter.QualifierFilter;

/**
 * Set of utility methods used by timeline filter classes.
 */
public final class TimelineFilterUtils {

  private TimelineFilterUtils() {
  }

  /**
   * Returns the equivalent HBase filter list's {@link Operator}.
   * @param op
   * @return HBase filter list's Operator.
   */
  private static Operator getHBaseOperator(TimelineFilterList.Operator op) {
    switch (op) {
    case AND:
      return Operator.MUST_PASS_ALL;
    case OR:
      return Operator.MUST_PASS_ONE;
    default:
      throw new IllegalArgumentException("Invalid operator");
    }
  }

  /**
   * Returns the equivalent HBase compare filter's {@link CompareOp}.
   * @param op
   * @return HBase compare filter's CompareOp.
   */
  private static CompareOp getHBaseCompareOp(
      TimelineCompareOp op) {
    switch (op) {
    case LESS_THAN:
      return CompareOp.LESS;
    case LESS_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    case EQUAL:
      return CompareOp.EQUAL;
    case NOT_EQUAL:
      return CompareOp.NOT_EQUAL;
    case GREATER_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case GREATER_THAN:
      return CompareOp.GREATER;
    default:
      throw new IllegalArgumentException("Invalid compare operator");
    }
  }

  /**
   * Converts a {@link TimelinePrefixFilter} to an equivalent HBase
   * {@link QualifierFilter}.
   * @param colPrefix
   * @param filter
   * @return a {@link QualifierFilter} object
   */
  private static <T> Filter createHBaseColQualPrefixFilter(
      ColumnPrefix<T> colPrefix, TimelinePrefixFilter filter) {
    return new QualifierFilter(getHBaseCompareOp(filter.getCompareOp()),
        new BinaryPrefixComparator(
            colPrefix.getColumnPrefixBytes(filter.getPrefix())));
  }

  /**
   * Creates equivalent HBase {@link FilterList} from {@link TimelineFilterList}
   * while converting different timeline filters(of type {@link TimelineFilter})
   * into their equivalent HBase filters.
   * @param colPrefix
   * @param filterList
   * @return a {@link FilterList} object
   */
  public static <T> FilterList createHBaseFilterList(ColumnPrefix<T> colPrefix,
      TimelineFilterList filterList) {
    FilterList list =
        new FilterList(getHBaseOperator(filterList.getOperator()));
    for (TimelineFilter filter : filterList.getFilterList()) {
      switch(filter.getFilterType()) {
      case LIST:
        list.addFilter(
            createHBaseFilterList(colPrefix, (TimelineFilterList)filter));
        break;
      case PREFIX:
        list.addFilter(createHBaseColQualPrefixFilter(
            colPrefix, (TimelinePrefixFilter)filter));
        break;
      default:
        break;
      }
    }
    return list;
  }
}
