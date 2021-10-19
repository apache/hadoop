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
package org.apache.hadoop.yarn.server.timelineservice.storage.reader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Timeline entity reader for flow run entities that are stored in the flow run
 * table.
 */
class FlowRunEntityReader extends TimelineEntityReader {
  private static final FlowRunTableRW FLOW_RUN_TABLE = new FlowRunTableRW();

  FlowRunEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  FlowRunEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * Uses the {@link FlowRunTableRW}.
   */
  @Override
  protected BaseTableRW<?> getTable() {
    return FLOW_RUN_TABLE;
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext(), "context shouldn't be null");
    Preconditions.checkNotNull(getDataToRetrieve(),
        "data to retrieve shouldn't be null");
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
    Preconditions.checkNotNull(getContext().getUserId(),
        "userId shouldn't be null");
    Preconditions.checkNotNull(getContext().getFlowName(),
        "flowName shouldn't be null");
    if (isSingleEntityRead()) {
      Preconditions.checkNotNull(getContext().getFlowRunId(),
          "flowRunId shouldn't be null");
    }
    EnumSet<Field> fieldsToRetrieve = getDataToRetrieve().getFieldsToRetrieve();
    if (!isSingleEntityRead() && fieldsToRetrieve != null) {
      for (Field field : fieldsToRetrieve) {
        if (field != Field.ALL && field != Field.METRICS) {
          throw new BadRequestException("Invalid field " + field
              + " specified while querying flow runs.");
        }
      }
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn) {
    // Add metrics to fields to retrieve if metricsToRetrieve is specified.
    getDataToRetrieve().addFieldsBasedOnConfsAndMetricsToRetrieve();
    if (!isSingleEntityRead()) {
      createFiltersIfNull();
    }
  }

  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    FilterList listBasedOnFilters = new FilterList();
    // Filter based on created time range.
    Long createdTimeBegin = getFilters().getCreatedTimeBegin();
    Long createdTimeEnd = getFilters().getCreatedTimeEnd();
    if (createdTimeBegin != 0 || createdTimeEnd != Long.MAX_VALUE) {
      listBasedOnFilters.addFilter(TimelineFilterUtils
          .createSingleColValueFiltersByRange(FlowRunColumn.MIN_START_TIME,
              createdTimeBegin, createdTimeEnd));
    }
    // Filter based on metric filters.
    TimelineFilterList metricFilters = getFilters().getMetricFilters();
    if (metricFilters != null && !metricFilters.getFilterList().isEmpty()) {
      listBasedOnFilters.addFilter(TimelineFilterUtils.createHBaseFilterList(
          FlowRunColumnPrefix.METRIC, metricFilters));
    }
    return listBasedOnFilters;
  }

  /**
   * Add {@link QualifierFilter} filters to filter list for each column of flow
   * run table.
   *
   * @return filter list to which qualifier filters have been added.
   */
  private FilterList updateFixedColumns() {
    FilterList columnsList = new FilterList(Operator.MUST_PASS_ONE);
    for (FlowRunColumn column : FlowRunColumn.values()) {
      columnsList.addFilter(new QualifierFilter(CompareOp.EQUAL,
          new BinaryComparator(column.getColumnQualifierBytes())));
    }
    return columnsList;
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(
      Set<String> cfsInFields) throws IOException {
    FilterList list = new FilterList(Operator.MUST_PASS_ONE);
    // By default fetch everything in INFO column family.
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(
            FlowRunColumnFamily.INFO.getBytes()));
    TimelineDataToRetrieve dataToRetrieve = getDataToRetrieve();
    // If multiple entities have to be retrieved, check if metrics have to be
    // retrieved and if not, add a filter so that metrics can be excluded.
    // Metrics are always returned if we are reading a single entity.
    if (!isSingleEntityRead()
        && !hasField(dataToRetrieve.getFieldsToRetrieve(), Field.METRICS)) {
      FilterList infoColFamilyList = new FilterList(Operator.MUST_PASS_ONE);
      infoColFamilyList.addFilter(infoColumnFamily);
      cfsInFields.add(Bytes.toString(FlowRunColumnFamily.INFO.getBytes()));
      infoColFamilyList.addFilter(new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(FlowRunColumnPrefix.METRIC
              .getColumnPrefixBytes(""))));
      list.addFilter(infoColFamilyList);
    } else {
      // Check if metricsToRetrieve are specified and if they are, create a
      // filter list for info column family by adding flow run tables columns
      // and a list for metrics to retrieve. Pls note that fieldsToRetrieve
      // will have METRICS added to it if metricsToRetrieve are specified
      // (in augmentParams()).
      TimelineFilterList metricsToRetrieve =
          dataToRetrieve.getMetricsToRetrieve();
      if (metricsToRetrieve != null
          && !metricsToRetrieve.getFilterList().isEmpty()) {
        FilterList infoColFamilyList = new FilterList();
        infoColFamilyList.addFilter(infoColumnFamily);
        cfsInFields.add(Bytes.toString(FlowRunColumnFamily.INFO.getBytes()));
        FilterList columnsList = updateFixedColumns();
        columnsList.addFilter(TimelineFilterUtils.createHBaseFilterList(
            FlowRunColumnPrefix.METRIC, metricsToRetrieve));
        infoColFamilyList.addFilter(columnsList);
        list.addFilter(infoColFamilyList);
      }
    }
    return list;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    TimelineReaderContext context = getContext();
    FlowRunRowKey flowRunRowKey =
        new FlowRunRowKey(context.getClusterId(), context.getUserId(),
            context.getFlowName(), context.getFlowRunId());
    byte[] rowKey = flowRunRowKey.getRowKey();
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return getTable().getResult(hbaseConf, conn, get);
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    Scan scan = new Scan();
    TimelineReaderContext context = getContext();
    RowKeyPrefix<FlowRunRowKey> flowRunRowKeyPrefix = null;
    if (getFilters().getFromId() == null) {
      flowRunRowKeyPrefix = new FlowRunRowKeyPrefix(context.getClusterId(),
          context.getUserId(), context.getFlowName());
      scan.setRowPrefixFilter(flowRunRowKeyPrefix.getRowKeyPrefix());
    } else {
      FlowRunRowKey flowRunRowKey = null;
      try {
        flowRunRowKey =
            FlowRunRowKey.parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      if (!context.getClusterId().equals(flowRunRowKey.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + context.getClusterId());
      }
      // set start row
      scan.setStartRow(flowRunRowKey.getRowKey());

      // get the bytes for stop row
      flowRunRowKeyPrefix = new FlowRunRowKeyPrefix(context.getClusterId(),
          context.getUserId(), context.getFlowName());

      // set stop row
      scan.setStopRow(
          HBaseTimelineStorageUtils.calculateTheClosestNextRowKeyForPrefix(
              flowRunRowKeyPrefix.getRowKeyPrefix()));
    }

    FilterList newList = new FilterList();
    newList.addFilter(new PageFilter(getFilters().getLimit()));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);
    scan.setMaxVersions(Integer.MAX_VALUE);
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    FlowRunEntity flowRun = new FlowRunEntity();
    FlowRunRowKey rowKey = FlowRunRowKey.parseRowKey(result.getRow());
    flowRun.setRunId(rowKey.getFlowRunId());
    flowRun.setUser(rowKey.getUserId());
    flowRun.setName(rowKey.getFlowName());

    // read the start time
    Long startTime = (Long) ColumnRWHelper.readResult(result,
        FlowRunColumn.MIN_START_TIME);
    if (startTime != null) {
      flowRun.setStartTime(startTime.longValue());
    }

    // read the end time if available
    Long endTime = (Long) ColumnRWHelper.readResult(result,
        FlowRunColumn.MAX_END_TIME);
    if (endTime != null) {
      flowRun.setMaxEndTime(endTime.longValue());
    }

    // read the flow version
    String version = (String) ColumnRWHelper.readResult(result,
        FlowRunColumn.FLOW_VERSION);
    if (version != null) {
      flowRun.setVersion(version);
    }

    // read metrics if its a single entity query or if METRICS are part of
    // fieldsToRetrieve.
    if (isSingleEntityRead()
        || hasField(getDataToRetrieve().getFieldsToRetrieve(), Field.METRICS)) {
      readMetrics(flowRun, result, FlowRunColumnPrefix.METRIC);
    }

    // set the id
    flowRun.setId(flowRun.getId());
    flowRun.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        rowKey.getRowKeyAsString());
    return flowRun;
  }
}
