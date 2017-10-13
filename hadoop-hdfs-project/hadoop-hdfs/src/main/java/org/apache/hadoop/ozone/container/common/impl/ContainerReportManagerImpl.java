/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.impl;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerReportManager;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.util.Time;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class wraps the container report operations on datanode.
 * // TODO: support incremental/delta container report
 */
public class ContainerReportManagerImpl implements ContainerReportManager {
  private Configuration config;
  // Last non-empty container report time
  private long lastContainerReportTime;
  private final long containerReportInterval;
  private AtomicLong reportCount;
  private static final ReportState NO_CONTAINER_REPORTSTATE =
      ReportState.newBuilder()
          .setState(ReportState.states.noContainerReports)
          .setCount(0).build();

  public ContainerReportManagerImpl(Configuration config) {
    this.config = config;
    this.lastContainerReportTime = -1;
    this.reportCount = new AtomicLong(0L);
    this.containerReportInterval = config.getLong(
        OzoneConfigKeys.OZONE_CONTAINER_REPORT_INTERVAL_MS,
        OzoneConfigKeys.OZONE_CONTAINER_REPORT_INTERVAL_MS_DEFAULT);
  }

  public ReportState getContainerReportState() {
    if (lastContainerReportTime < 0) {
      return getFullContainerReportState();
    } else {
      // Add a random delay (0~30s) on top of the container report
      // interval (60s) so tha the SCM is overwhelmed by the container reports
      // sent in sync.
      if (Time.monotonicNow() - lastContainerReportTime >
          (containerReportInterval + getRandomReportDelay())) {
        return getFullContainerReportState();
      } else {
        return getNoContainerReportState();
      }
    }
  }

  private ReportState getFullContainerReportState() {
    ReportState.Builder rsBuilder = ReportState.newBuilder();
    rsBuilder.setState(ReportState.states.completeContinerReport);
    rsBuilder.setCount(reportCount.incrementAndGet());
    this.lastContainerReportTime = Time.monotonicNow();
    return rsBuilder.build();
  }

  private ReportState getNoContainerReportState() {
    return NO_CONTAINER_REPORTSTATE;
  }

  private long getRandomReportDelay() {
    return RandomUtils.nextLong(0,
        OzoneClientUtils.getScmHeartbeatInterval(config));
  }
}
