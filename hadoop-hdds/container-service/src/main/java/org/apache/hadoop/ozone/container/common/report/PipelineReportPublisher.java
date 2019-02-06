/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.report;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.HddsServerUtil;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT;


/**
 * Publishes Pipeline which will be sent to SCM as part of heartbeat.
 * PipelineReport consist of the following information about each containers:
 *   - pipelineID
 *
 */
public class PipelineReportPublisher extends
    ReportPublisher<PipelineReportsProto> {

  private Long pipelineReportInterval = null;

  @Override
  protected long getReportFrequency() {
    if (pipelineReportInterval == null) {
      pipelineReportInterval = getConf().getTimeDuration(
          HDDS_PIPELINE_REPORT_INTERVAL,
          HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= pipelineReportInterval,
              HDDS_PIPELINE_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency.");
    }
    // Add a random delay (0~30s) on top of the pipeline report
    // interval (60s) so tha the SCM is overwhelmed by the pipeline reports
    // sent in sync.
    return pipelineReportInterval + getRandomReportDelay();
  }

  private long getRandomReportDelay() {
    return RandomUtils.nextLong(0, pipelineReportInterval);
  }

  @Override
  protected PipelineReportsProto getReport() {
    return getContext().getParent().getContainer().getPipelineReport();
  }
}
