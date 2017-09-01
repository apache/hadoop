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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Column;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnPrefix;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of utility methods used by timeline filter classes.
 */
public final class TimelineFilterUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineFilterUtils.class);

  private TimelineFilterUtils() {
  }

  /**
   * Returns the equivalent HBase filter list's {@link Operator}.
   *
   * @param op timeline filter list operator.
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
   *
   * @param op timeline compare op.
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
   * Create a HBase {@link QualifierFilter} for the passed column prefix and
   * compare op.
   *
   * @param <T> Describes the type of column prefix.
   * @param compareOp compare op.
   * @param columnPrefix column prefix.
   * @return a column qualifier filter.
   */
  public static <T> Filter createHBaseQualifierFilter(CompareOp compareOp,
      ColumnPrefix<T> columnPrefix) {
    return new QualifierFilter(compareOp,
        new BinaryPrefixComparator(
            columnPrefix.getColumnPrefixBytes("")));
  }

  /**
   * Create filters for confs or metrics to retrieve. This list includes a
   * configs/metrics family filter and relevant filters for confs/metrics to
   * retrieve, if present.
   *
   * @param <T> Describes the type of column prefix.
   * @param confsOrMetricToRetrieve configs/metrics to retrieve.
   * @param columnFamily config or metric column family.
   * @param columnPrefix config or metric column prefix.
   * @return a filter list.
   * @throws IOException if any problem occurs while creating the filters.
   */
  public static <T> Filter createFilterForConfsOrMetricsToRetrieve(
      TimelineFilterList confsOrMetricToRetrieve, ColumnFamily<T> columnFamily,
      ColumnPrefix<T> columnPrefix) throws IOException {
    Filter familyFilter = new FamilyFilter(CompareOp.EQUAL,
        new BinaryComparator(columnFamily.getBytes()));
    if (confsOrMetricToRetrieve != null &&
        !confsOrMetricToRetrieve.getFilterList().isEmpty()) {
      // If confsOrMetricsToRetrive are specified, create a filter list based
      // on it and family filter.
      FilterList filter = new FilterList(familyFilter);
      filter.addFilter(
          createHBaseFilterList(columnPrefix, confsOrMetricToRetrieve));
      return filter;
    } else {
      // Only the family filter needs to be added.
      return familyFilter;
    }
  }

  /**
   * Create 2 HBase {@link SingleColumnValueFilter} filters for the specified
   * value range represented by start and end value and wraps them inside a
   * filter list. Start and end value should not be null.
   *
   * @param <T> Describes the type of column prefix.
   * @param column Column for which single column value filter is to be created.
   * @param startValue Start value.
   * @param endValue End value.
   * @return 2 single column value filters wrapped in a filter list.
   * @throws IOException if any problem is encountered while encoding value.
   */
  public static <T> FilterList createSingleColValueFiltersByRange(
      Column<T> column, Object startValue, Object endValue) throws IOException {
    FilterList list = new FilterList();
    Filter singleColValFilterStart = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(startValue),
        CompareOp.GREATER_OR_EQUAL, true);
    list.addFilter(singleColValFilterStart);

    Filter singleColValFilterEnd = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(endValue),
        CompareOp.LESS_OR_EQUAL, true);
    list.addFilter(singleColValFilterEnd);
    return list;
  }

  /**
   * Creates a HBase {@link SingleColumnValueFilter} with specified column.
   * @param <T> Describes the type of column prefix.
   * @param column Column which value to be filtered.
   * @param value Value to be filtered.
   * @param op Compare operator
   * @return a SingleColumnValue Filter
   * @throws IOException if any exception.
   */
  public static <T> Filter createHBaseSingleColValueFilter(Column<T> column,
      Object value, CompareOp op) throws IOException {
    Filter singleColValFilter = createHBaseSingleColValueFilter(
        column.getColumnFamilyBytes(), column.getColumnQualifierBytes(),
        column.getValueConverter().encodeValue(value), op, true);
    return singleColValFilter;
  }

  /**
   * Creates a HBase {@link SingleColumnValueFilter}.
   *
   * @param columnFamily Column Family represented as bytes.
   * @param columnQualifier Column Qualifier represented as bytes.
   * @param value Value.
   * @param compareOp Compare operator.
   * @param filterIfMissing This flag decides if we should filter the row if the
   *     specified column is missing. This is based on the filter's keyMustExist
   *     field.
   * @return a {@link SingleColumnValueFilter} object
   * @throws IOException
   */
  private static SingleColumnValueFilter createHBaseSingleColValueFilter(
      byte[] columnFamily, byte[] columnQualifier, byte[] value,
      CompareOp compareOp, boolean filterIfMissing) throws IOException {
    SingleColumnValueFilter singleColValFilter =
        new SingleColumnValueFilter(columnFamily, columnQualifier, compareOp,
        new BinaryComparator(value));
    singleColValFilter.setLatestVersionOnly(true);
    singleColValFilter.setFilterIfMissing(filterIfMissing);
    return singleColValFilter;
  }

  /**
   * Fetch columns from filter list containing exists and multivalue equality
   * filters. This is done to fetch only required columns from back-end and
   * then match event filters or relationships in reader.
   *
   * @param filterList filter list.
   * @return set of columns.
   */
  public static Set<String> fetchColumnsFromFilterList(
      TimelineFilterList filterList) {
    Set<String> strSet = new HashSet<String>();
    for (TimelineFilter filter : filterList.getFilterList()) {
      switch(filter.getFilterType()) {
      case LIST:
        strSet.addAll(fetchColumnsFromFilterList((TimelineFilterList)filter));
        break;
      case KEY_VALUES:
        strSet.add(((TimelineKeyValuesFilter)filter).getKey());
        break;
      case EXISTS:
        strSet.add(((TimelineExistsFilter)filter).getValue());
        break;
      default:
        LOG.info("Unexpected filter type " + filter.getFilterType());
        break;
      }
    }
    return strSet;
  }

  /**
   * Creates equivalent HBase {@link FilterList} from {@link TimelineFilterList}
   * while converting different timeline filters(of type {@link TimelineFilter})
   * into their equivalent HBase filters.
   *
   * @param <T> Describes the type of column prefix.
   * @param colPrefix column prefix which will be used for conversion.
   * @param filterList timeline filter list which has to be converted.
   * @return A {@link FilterList} object.
   * @throws IOException if any problem occurs while creating the filter list.
   */
  public static <T> FilterList createHBaseFilterList(ColumnPrefix<T> colPrefix,
      TimelineFilterList filterList) throws IOException {
    FilterList list =
        new FilterList(getHBaseOperator(filterList.getOperator()));
    for (TimelineFilter filter : filterList.getFilterList()) {
      switch(filter.getFilterType()) {
      case LIST:
        list.addFilter(createHBaseFilterList(colPrefix,
            (TimelineFilterList)filter));
        break;
      case PREFIX:
        list.addFilter(createHBaseColQualPrefixFilter(colPrefix,
            (TimelinePrefixFilter)filter));
        break;
      case COMPARE:
        TimelineCompareFilter compareFilter = (TimelineCompareFilter)filter;
        list.addFilter(
            createHBaseSingleColValueFilter(
                colPrefix.getColumnFamilyBytes(),
                colPrefix.getColumnPrefixBytes(compareFilter.getKey()),
                colPrefix.getValueConverter().
                    encodeValue(compareFilter.getValue()),
                getHBaseCompareOp(compareFilter.getCompareOp()),
                compareFilter.getKeyMustExist()));
        break;
      case KEY_VALUE:
        TimelineKeyValueFilter kvFilter = (TimelineKeyValueFilter)filter;
        list.addFilter(
            createHBaseSingleColValueFilter(
                colPrefix.getColumnFamilyBytes(),
                colPrefix.getColumnPrefixBytes(kvFilter.getKey()),
                colPrefix.getValueConverter().encodeValue(kvFilter.getValue()),
                getHBaseCompareOp(kvFilter.getCompareOp()),
                kvFilter.getKeyMustExist()));
        break;
      default:
        LOG.info("Unexpected filter type " + filter.getFilterType());
        break;
      }
    }
    return list;
  }
}
