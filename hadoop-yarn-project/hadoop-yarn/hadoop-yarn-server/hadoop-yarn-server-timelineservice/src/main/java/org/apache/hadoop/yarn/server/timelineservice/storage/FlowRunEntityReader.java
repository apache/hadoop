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
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowRunColumn;
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
      String flowId, Long flowRunId, String appId, String entityType,
      Long limit, Long createdTimeBegin, Long createdTimeEnd,
      Long modifiedTimeBegin, Long modifiedTimeEnd,
      Map<String, Set<String>> relatesTo, Map<String, Set<String>> isRelatedTo,
      Map<String, Object> infoFilters, Map<String, String> configFilters,
      Set<String> metricFilters, Set<String> eventFilters,
      EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowId, flowRunId, appId, entityType, limit,
        createdTimeBegin, createdTimeEnd, modifiedTimeBegin, modifiedTimeEnd,
        relatesTo, isRelatedTo, infoFilters, configFilters, metricFilters,
        eventFilters, fieldsToRetrieve);
  }

  public FlowRunEntityReader(String userId, String clusterId,
      String flowId, Long flowRunId, String appId, String entityType,
      String entityId, EnumSet<Field> fieldsToRetrieve) {
    super(userId, clusterId, flowId, flowRunId, appId, entityType, entityId,
        fieldsToRetrieve);
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
    Preconditions.checkNotNull(flowId, "flowId shouldn't be null");
    Preconditions.checkNotNull(flowRunId, "flowRunId shouldn't be null");
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn) {
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn)
      throws IOException {
    byte[] rowKey =
        FlowRunRowKey.getRowKey(clusterId, userId, flowId, flowRunId);
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return table.getResult(hbaseConf, conn, get);
  }

  @Override
  protected Iterable<Result> getResults(Configuration hbaseConf,
      Connection conn) throws IOException {
    throw new UnsupportedOperationException(
        "multiple entity query is not supported");
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    FlowRunEntity flowRun = new FlowRunEntity();
    flowRun.setUser(userId);
    flowRun.setName(flowId);
    flowRun.setRunId(flowRunId);

    // read the start time
    Long startTime = (Long)FlowRunColumn.MIN_START_TIME.readResult(result);
    if (startTime != null) {
      flowRun.setStartTime(startTime);
    }
    // read the end time if available
    Long endTime = (Long)FlowRunColumn.MAX_END_TIME.readResult(result);
    if (endTime != null) {
      flowRun.setMaxEndTime(endTime);
    }

    // read the flow version
    String version = (String)FlowRunColumn.FLOW_VERSION.readResult(result);
    if (version != null) {
      flowRun.setVersion(version);
    }

    // read metrics
    readMetrics(flowRun, result, FlowRunColumnPrefix.METRIC);

    // set the id
    flowRun.setId(flowRun.getId());
    return flowRun;
  }
}
