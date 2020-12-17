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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.FlowActivityTableRW;
import org.apache.hadoop.yarn.webapp.BadRequestException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Timeline entity reader for flow activity entities that are stored in the
 * flow activity table.
 */
class FlowActivityEntityReader extends TimelineEntityReader {
  private static final FlowActivityTableRW FLOW_ACTIVITY_TABLE =
      new FlowActivityTableRW();

  /**
   * Used to convert Long key components to and from storage format.
   */
  private final KeyConverter<Long> longKeyConverter = new LongKeyConverter();


  FlowActivityEntityReader(TimelineReaderContext ctxt,
      TimelineEntityFilters entityFilters, TimelineDataToRetrieve toRetrieve) {
    super(ctxt, entityFilters, toRetrieve);
  }

  FlowActivityEntityReader(TimelineReaderContext ctxt,
      TimelineDataToRetrieve toRetrieve) {
    super(ctxt, toRetrieve);
  }

  /**
   * Uses the {@link FlowActivityTableRW}.
   */
  @Override
  protected BaseTableRW<?> getTable() {
    return FLOW_ACTIVITY_TABLE;
  }

  @Override
  protected void validateParams() {
    Preconditions.checkNotNull(getContext().getClusterId(),
        "clusterId shouldn't be null");
  }

  @Override
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    createFiltersIfNull();
  }

  @Override
  protected FilterList constructFilterListBasedOnFilters() throws IOException {
    return null;
  }

  @Override
  protected FilterList constructFilterListBasedOnFields(
      Set<String> cfsInFields) {
    return null;
  }

  @Override
  protected Result getResult(Configuration hbaseConf, Connection conn,
      FilterList filterList) throws IOException {
    throw new UnsupportedOperationException(
        "we don't support a single entity query");
  }

  @Override
  protected ResultScanner getResults(Configuration hbaseConf,
      Connection conn, FilterList filterList) throws IOException {
    Scan scan = new Scan();
    String clusterId = getContext().getClusterId();
    if (getFilters().getFromId() == null
        && getFilters().getCreatedTimeBegin() == 0L
        && getFilters().getCreatedTimeEnd() == Long.MAX_VALUE) {
       // All records have to be chosen.
      scan.setRowPrefixFilter(new FlowActivityRowKeyPrefix(clusterId)
          .getRowKeyPrefix());
    } else if (getFilters().getFromId() != null) {
      FlowActivityRowKey key = null;
      try {
        key =
            FlowActivityRowKey.parseRowKeyFromString(getFilters().getFromId());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Invalid filter fromid is provided.");
      }
      if (!clusterId.equals(key.getClusterId())) {
        throw new BadRequestException(
            "fromid doesn't belong to clusterId=" + clusterId);
      }
      scan.setStartRow(key.getRowKey());
      scan.setStopRow(
          new FlowActivityRowKeyPrefix(clusterId,
              (getFilters().getCreatedTimeBegin() <= 0 ? 0
                  : (getFilters().getCreatedTimeBegin() - 1)))
                      .getRowKeyPrefix());
    } else {
      scan.setStartRow(new FlowActivityRowKeyPrefix(clusterId, getFilters()
          .getCreatedTimeEnd()).getRowKeyPrefix());
      scan.setStopRow(new FlowActivityRowKeyPrefix(clusterId, (getFilters()
          .getCreatedTimeBegin() <= 0 ? 0
          : (getFilters().getCreatedTimeBegin() - 1))).getRowKeyPrefix());
    }
    // use the page filter to limit the result to the page size
    // the scanner may still return more than the limit; therefore we need to
    // read the right number as we iterate
    scan.setFilter(new PageFilter(getFilters().getLimit()));
    return getTable().getResultScanner(hbaseConf, conn, scan);
  }

  @Override
  protected TimelineEntity parseEntity(Result result) throws IOException {
    FlowActivityRowKey rowKey = FlowActivityRowKey.parseRowKey(result.getRow());

    Long time = rowKey.getDayTimestamp();
    String user = rowKey.getUserId();
    String flowName = rowKey.getFlowName();

    FlowActivityEntity flowActivity = new FlowActivityEntity(
        getContext().getClusterId(), time, user, flowName);
    // set the id
    flowActivity.setId(flowActivity.getId());
    // get the list of run ids along with the version that are associated with
    // this flow on this day
    Map<Long, Object> runIdsMap = ColumnRWHelper.readResults(result,
        FlowActivityColumnPrefix.RUN_ID, longKeyConverter);
    for (Map.Entry<Long, Object> e : runIdsMap.entrySet()) {
      Long runId = e.getKey();
      String version = (String)e.getValue();
      FlowRunEntity flowRun = new FlowRunEntity();
      flowRun.setUser(user);
      flowRun.setName(flowName);
      flowRun.setRunId(runId);
      flowRun.setVersion(version);
      // set the id
      flowRun.setId(flowRun.getId());
      flowActivity.addFlowRun(flowRun);
    }
    flowActivity.getInfo().put(TimelineReaderUtils.FROMID_KEY,
        rowKey.getRowKeyAsString());
    return flowActivity;
  }
}
