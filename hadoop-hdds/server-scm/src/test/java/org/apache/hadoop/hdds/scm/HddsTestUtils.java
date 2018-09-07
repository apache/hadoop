/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer
    .NodeRegistrationContainerReport;

/**
 * Stateless helper functions for Hdds tests.
 */
public final class HddsTestUtils {

  private HddsTestUtils() {
  }

  /**
   * Create Command Status report object.
   *
   * @param numOfContainers number of containers to be included in report.
   * @return CommandStatusReportsProto
   */
  public static NodeRegistrationContainerReport
      createNodeRegistrationContainerReport(int numOfContainers) {
    return new NodeRegistrationContainerReport(
        TestUtils.randomDatanodeDetails(),
        TestUtils.getRandomContainerReports(numOfContainers));
  }

  /**
   * Create NodeRegistrationContainerReport object.
   *
   * @param dnContainers List of containers to be included in report
   * @return NodeRegistrationContainerReport
   */
  public static NodeRegistrationContainerReport
      createNodeRegistrationContainerReport(List<ContainerInfo> dnContainers) {
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo>
        containers = new ArrayList<>();
    dnContainers.forEach(c -> {
      containers.add(TestUtils.getRandomContainerInfo(c.getContainerID()));
    });
    return new NodeRegistrationContainerReport(
        TestUtils.randomDatanodeDetails(),
        TestUtils.getContainerReports(containers));
  }

  /**
   * Creates list of ContainerInfo.
   *
   * @param numContainers number of ContainerInfo to be included in list.
   * @return List<ContainerInfo>
   */
  public static List<ContainerInfo> getContainerInfo(int numContainers) {
    List<ContainerInfo> containerInfoList = new ArrayList<>();
    for (int i = 0; i < numContainers; i++) {
      ContainerInfo.Builder builder = new ContainerInfo.Builder();
      containerInfoList.add(builder
          .setContainerID(RandomUtils.nextLong())
          .build());
    }
    return containerInfoList;
  }

}
