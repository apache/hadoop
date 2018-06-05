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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import java.util.concurrent.TimeUnit;


/**
 * Publishes ContainerReport which will be sent to SCM as part of heartbeat.
 * ContainerReport consist of the following information about each containers:
 *   - containerID
 *   - size
 *   - used
 *   - keyCount
 *   - readCount
 *   - writeCount
 *   - readBytes
 *   - writeBytes
 *   - finalHash
 *   - LifeCycleState
 *
 */
public class ContainerReportPublisher extends
    ReportPublisher<ContainerReportsProto> {

  private Long containerReportInterval = null;

  @Override
  protected long getReportFrequency() {
    if (containerReportInterval == null) {
      containerReportInterval = getConf().getTimeDuration(
          OzoneConfigKeys.OZONE_CONTAINER_REPORT_INTERVAL,
          OzoneConfigKeys.OZONE_CONTAINER_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
    }
    // Add a random delay (0~30s) on top of the container report
    // interval (60s) so tha the SCM is overwhelmed by the container reports
    // sent in sync.
    return containerReportInterval + getRandomReportDelay();
  }

  private long getRandomReportDelay() {
    return RandomUtils.nextLong(0, containerReportInterval);
  }

  @Override
  protected ContainerReportsProto getReport() {
    return ContainerReportsProto.getDefaultInstance();
  }
}
