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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunTable;

import com.google.common.base.Preconditions;

/**
 * Timeline entity reader for flow run entities that are stored in the flow run
 * table.
 */
class FlowRunEntityReader extends TimelineEntityReader {
  private static final FlowRunTable FLOW_RUN_TABLE = new FlowRunTable();

  public FlowRunEntityReader(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      TimelineFilterList confsToRetrieve, TimelineFilterList metricsToRetrieve,
      EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowName, flowRunId, appId, entityType, limit,
        createdTimeBegin, createdTimeEnd, modifiedTimeBegin, modifiedTimeEnd,
        relatesTo, isRelatedTo, infoFilters, configFilters, metricFilters,
        eventFilters, null, metricsToRetrieve, fieldsToRetrieve, true);
  }

  public FlowRunEntityReader(String userId, String clusterId,
      String flowName, Long flowRunId, String appId, String entityType,
      String entityId, TimelineFilterList confsToRetrieve,
      TimelineFilterList metricsToRetrieve, EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowName, flowRunId, appId, entityType, entityId,
        null, metricsToRetrieve, fieldsToRetrieve);
  }

  /**
   * Uses the {@link FlowRunTable}.
   */
  @Override
  protected BaseTable<?> getTable() {
    return FLOW_RUN_TABLE;
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(clusterId, "clusterId shouldn't be null");
    Preconditions.checkNotNull(userId, "userId shouldn't be null");
    Preconditions.checkNotNull(flowName, "flowName shouldn't be null");
    if (singleEntityRead) {
      Preconditions.checkNotNull(flowRunId, "flowRunId shouldn't be null");
    }
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn) {
    if (!singleEntityRead) {
      if (fieldsToRetrieve == null) {
        fieldsToRetrieve = EnumSet.noneOf(Field.class);
      }
      if (limit == null || limit < 0) {
        limit = TimelineReader.DEFAULT_LIMIT;
      }
      if (createdTimeBegin == null) {
        createdTimeBegin = DEFAULT_BEGIN_TIME;
      }
      if (createdTimeEnd == null) {
        createdTimeEnd = DEFAULT_END_TIME;
      }
      if (!fieldsToRetrieve.contains(Field.METRICS) &&
          metricsToRetrieve != null &&
          !metricsToRetrieve.getFilterList().isEmpty()) {
        fieldsToRetrieve.add(Field.METRICS);
      }
    }
  }

  @Override
  protected FilterList constructFilterListBasedOnFields() {
    FilterList list = new FilterList(Operator.MUST_PASS_ONE);

    // By default fetch everything in INFO column family.
    FamilyFilter infoColumnFamily =
        new FamilyFilter(CompareOp.EQUAL,
           new BinaryComparator(FlowRunColumnFamily.INFO.getBytes()));
    // Metrics not required.
    if (!singleEntityRead && !fieldsToRetrieve.contains(Field.METRICS) &&
        !fieldsToRetrieve.contains(Field.ALL)) {
      FilterList infoColFamilyList = new FilterList(Operator.MUST_PASS_ONE);
      infoColFamilyList.addFilter(infoColumnFamily);
      infoColFamilyList.addFilter(
          new QualifierFilter(CompareOp.NOT_EQUAL,
          new BinaryPrefixComparator(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(""))));
      list.addFilter(infoColFamilyList);
    }
    if (metricsToRetrieve != null &&
        !metricsToRetrieve.getFilterList().isEmpty()) {
      FilterList infoColFamilyList = new FilterList();
      infoColFamilyList.addFilter(infoColumnFamily);
      infoColFamilyList.addFilter(TimelineFilterUtils.createHBaseFilterList(
          FlowRunColumnPrefix.METRIC, metricsToRetrieve));
      list.addFilter(infoColFamilyList);
    }
    return list;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    byte[] rowKey =
        FlowRunRowKey.getRowKey(clusterId, userId, flowName, flowRunId);
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      get.setFilter(filterList);
    }
    return table.getResult(hbaseConf, conn, get);
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    scan.setRowPrefixFilter(
        FlowRunRowKey.getRowKeyPrefix(clusterId, userId, flowName));
    FilterList newList = new FilterList();
    newList.addFilter(new PageFilter(limit));
    if (filterList != null && !filterList.getFilters().isEmpty()) {
      newList.addFilter(filterList);
    }
    scan.setFilter(newList);
    return table.getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    FlowRunEntity flowRun = new FlowRunEntity();
    flowRun.setUser(userId);
    flowRun.setName(flowName);
    if (singleEntityRead) {
      flowRun.setRunId(flowRunId);
    } else {
      FlowRunRowKey rowKey = FlowRunRowKey.parseRowKey(result.getRow());
      flowRun.setRunId(rowKey.getFlowRunId());
    }

    // read the start time
    Long startTime = (Long)FlowRunColumn.MIN_START_TIME.readResult(result);
    if (startTime != null) {
      flowRun.setStartTime(startTime.longValue());
    }
    if (!singleEntityRead && (flowRun.getStartTime() < createdTimeBegin ||
        flowRun.getStartTime() > createdTimeEnd)) {
      return null;
    }

    // read the end time if available
    Long endTime = (Long)FlowRunColumn.MAX_END_TIME.readResult(result);
    if (endTime != null) {
      flowRun.setMaxEndTime(endTime.longValue());
    }

    // read the flow version
    String version = (String)FlowRunColumn.FLOW_VERSION.readResult(result);
    if (version != null) {
      flowRun.setVersion(version);
    }

    // read metrics
    if (singleEntityRead || fieldsToRetrieve.contains(Field.METRICS)) {
      readMetrics(flowRun, result, FlowRunColumnPrefix.METRIC);
    }

    // set the id
    flowRun.setId(flowRun.getId());
    return flowRun;
  }
}
