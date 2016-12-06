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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow.AppToFlowTable;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import java.io.IOException;

/**
 * The base class for reading timeline data from the HBase storage. This class
 * provides basic support to validate and augment reader context.
 */
public abstract class AbstractTimelineStorageReader {

  private final TimelineReaderContext context;
  /**
   * Used to look up the flow context.
   */
  private final AppToFlowTable appToFlowTable = new AppToFlowTable();

  public AbstractTimelineStorageReader(TimelineReaderContext ctxt) {
    context = ctxt;
  }

  protected TimelineReaderContext getContext() {
    return context;
  }

  /**
   * Looks up flow context from AppToFlow table.
   *
   * @param appToFlowRowKey to identify Cluster and App Ids.
   * @param hbaseConf HBase configuration.
   * @param conn HBase Connection.
   * @return flow context information.
   * @throws IOException if any problem occurs while fetching flow information.
   */
  protected FlowContext lookupFlowContext(AppToFlowRowKey appToFlowRowKey,
      Configuration hbaseConf, Connection conn) throws IOException {
    byte[] rowKey = appToFlowRowKey.getRowKey();
    Get get = new Get(rowKey);
    Result result = appToFlowTable.getResult(hbaseConf, conn, get);
    if (result != null && !result.isEmpty()) {
      return new FlowContext(AppToFlowColumn.USER_ID.readResult(result)
          .toString(), AppToFlowColumn.FLOW_ID.readResult(result).toString(),
          ((Number) AppToFlowColumn.FLOW_RUN_ID.readResult(result))
          .longValue());
    } else {
      throw new NotFoundException(
          "Unable to find the context flow ID and flow run ID for clusterId="
              + appToFlowRowKey.getClusterId() + ", appId="
              + appToFlowRowKey.getAppId());
    }
  }

  /**
    * Sets certain parameters to defaults if the values are not provided.
    *
    * @param hbaseConf HBase Configuration.
    * @param conn HBase Connection.
    * @throws IOException if any exception is encountered while setting params.
    */
  protected void augmentParams(Configuration hbaseConf, Connection conn)
      throws IOException {
    defaultAugmentParams(hbaseConf, conn);
  }

  /**
   * Default behavior for all timeline readers to augment parameters.
   *
   * @param hbaseConf HBase Configuration.
   * @param conn HBase Connection.
   * @throws IOException if any exception is encountered while setting params.
   */
  final protected void defaultAugmentParams(Configuration hbaseConf,
      Connection conn) throws IOException {
    // In reality all three should be null or neither should be null
    if (context.getFlowName() == null || context.getFlowRunId() == null
        || context.getUserId() == null) {
      // Get flow context information from AppToFlow table.
      AppToFlowRowKey appToFlowRowKey =
          new AppToFlowRowKey(context.getClusterId(), context.getAppId());
      FlowContext flowContext =
          lookupFlowContext(appToFlowRowKey, hbaseConf, conn);
      context.setFlowName(flowContext.flowName);
      context.setFlowRunId(flowContext.flowRunId);
      context.setUserId(flowContext.userId);
    }
  }

  /**
   * Validates the required parameters to read the entities.
   */
  protected abstract void validateParams();

  /**
   * Encapsulates flow context information.
   */
  protected static class FlowContext {
    private final String userId;
    private final String flowName;
    private final Long flowRunId;

    public FlowContext(String user, String flowName, Long flowRunId) {
      this.userId = user;
      this.flowName = flowName;
      this.flowRunId = flowRunId;
    }

    protected String getUserId() {
      return userId;
    }

    protected String getFlowName() {
      return flowName;
    }

    protected Long getFlowRunId() {
      return flowRunId;
    }
  }
}
