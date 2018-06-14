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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handler for Datanode Container Report.
 */
public class SCMDatanodeContainerReportHandler extends
    SCMDatanodeReportHandler<ContainerReportsProto> {

  private static final Logger LOG = LoggerFactory.getLogger(
      SCMDatanodeContainerReportHandler.class);

  @Override
  public void processReport(DatanodeDetails datanodeDetails,
                            ContainerReportsProto report) throws IOException {
    LOG.trace("Processing container report from {}.", datanodeDetails);
    updateContainerReportMetrics(datanodeDetails, report);
    getSCM().getScmContainerManager()
        .processContainerReports(datanodeDetails, report);
  }

  /**
   * Updates container report metrics in SCM.
   *
   * @param datanodeDetails Datanode Information
   * @param reports Container Reports
   */
  private void updateContainerReportMetrics(DatanodeDetails datanodeDetails,
                                            ContainerReportsProto reports) {
    ContainerStat newStat = new ContainerStat();
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo info : reports
        .getReportsList()) {
      newStat.add(new ContainerStat(info.getSize(), info.getUsed(),
          info.getKeyCount(), info.getReadBytes(), info.getWriteBytes(),
          info.getReadCount(), info.getWriteCount()));
    }
    // update container metrics
    StorageContainerManager.getMetrics().setLastContainerStat(newStat);

    // Update container stat entry, this will trigger a removal operation if it
    // exists in cache.
    String datanodeUuid = datanodeDetails.getUuidString();
    getSCM().getContainerReportCache().put(datanodeUuid, newStat);
    // update global view container metrics
    StorageContainerManager.getMetrics().incrContainerStat(newStat);
  }

}
