/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.server.report;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * This class is responsible for dispatching heartbeat from datanode to
 * appropriate ReportHandlers at SCM.
 * Only one handler per report is supported now, it's very easy to support
 * multiple handlers for a report.
 */
public final class SCMDatanodeHeartbeatDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeHeartbeatDispatcher.class);

  /**
   * This stores Report to Handler mapping.
   */
  private final Map<Class<? extends GeneratedMessage>,
      SCMDatanodeReportHandler<? extends GeneratedMessage>> handlers;

  /**
   * Executor service which will be used for processing reports.
   */
  private final ExecutorService executorService;

  /**
   * Constructs SCMDatanodeHeartbeatDispatcher instance with the given
   * handlers.
   *
   * @param handlers report to report handler mapping
   */
  private SCMDatanodeHeartbeatDispatcher(Map<Class<? extends GeneratedMessage>,
      SCMDatanodeReportHandler<? extends GeneratedMessage>> handlers) {
    this.handlers = handlers;
    this.executorService = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("SCMDatanode Heartbeat Dispatcher Thread - %d")
            .build());
  }

  /**
   * Dispatches heartbeat to registered handlers.
   *
   * @param heartbeat heartbeat to be dispatched.
   */
  public void dispatch(SCMHeartbeatRequestProto heartbeat) {
    DatanodeDetails datanodeDetails = DatanodeDetails
        .getFromProtoBuf(heartbeat.getDatanodeDetails());
    if (heartbeat.hasNodeReport()) {
      processReport(datanodeDetails, heartbeat.getNodeReport());
    }
    if (heartbeat.hasContainerReport()) {
      processReport(datanodeDetails, heartbeat.getContainerReport());
    }
  }

  /**
   * Invokes appropriate ReportHandler and submits the task to executor
   * service for processing.
   *
   * @param datanodeDetails Datanode Information
   * @param report Report to be processed
   */
  @SuppressWarnings("unchecked")
  private void processReport(DatanodeDetails datanodeDetails,
                             GeneratedMessage report) {
    executorService.submit(() -> {
      try {
        SCMDatanodeReportHandler handler = handlers.get(report.getClass());
        handler.processReport(datanodeDetails, report);
      } catch (IOException ex) {
        LOG.error("Exception wile processing report {}, from {}",
            report.getClass(), datanodeDetails, ex);
      }
    });
  }

  /**
   * Shuts down SCMDatanodeHeartbeatDispatcher.
   */
  public void shutdown() {
    executorService.shutdown();
  }

  /**
   * Returns a new Builder to construct {@link SCMDatanodeHeartbeatDispatcher}.
   *
   * @param conf Configuration to be used by SCMDatanodeHeartbeatDispatcher
   * @param scm {@link StorageContainerManager} instance to be used by report
   *            handlers
   *
   * @return {@link SCMDatanodeHeartbeatDispatcher.Builder} instance
   */
  public static Builder newBuilder(Configuration conf,
                                   StorageContainerManager scm) {
    return new Builder(conf, scm);
  }

  /**
   * Builder for SCMDatanodeHeartbeatDispatcher.
   */
  public static class Builder {

    private final SCMDatanodeReportHandlerFactory reportHandlerFactory;
    private final Map<Class<? extends GeneratedMessage>,
        SCMDatanodeReportHandler<? extends GeneratedMessage>> report2handler;

    /**
     * Constructs SCMDatanodeHeartbeatDispatcher.Builder instance.
     *
     * @param conf Configuration object to be used.
     * @param scm StorageContainerManager instance to be used for report
     *            handler initialization.
     */
    private Builder(Configuration conf, StorageContainerManager scm) {
      this.report2handler = new HashMap<>();
      this.reportHandlerFactory =
          new SCMDatanodeReportHandlerFactory(conf, scm);
    }

    /**
     * Adds new report handler for the given report.
     *
     * @param report Report for which handler has to be added
     *
     * @return Builder
     */
    public Builder addHandlerFor(Class<? extends GeneratedMessage> report) {
      report2handler.put(report, reportHandlerFactory.getHandlerFor(report));
      return this;
    }

    /**
     * Associates the given report handler for the given report.
     *
     * @param report Report to be associated with
     * @param handler Handler to be used for the report
     *
     * @return Builder
     */
    public Builder addHandler(Class<? extends GeneratedMessage> report,
        SCMDatanodeReportHandler<? extends GeneratedMessage> handler) {
      report2handler.put(report, handler);
      return this;
    }

    /**
     * Builds and returns {@link SCMDatanodeHeartbeatDispatcher} instance.
     *
     * @return SCMDatanodeHeartbeatDispatcher
     */
    public SCMDatanodeHeartbeatDispatcher build() {
      return new SCMDatanodeHeartbeatDispatcher(report2handler);
    }
  }

}
