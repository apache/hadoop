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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * This class tests the behavior of SCMDatanodeHeartbeatDispatcher.
 */
public class TestSCMDatanodeHeartbeatDispatcher {

  @Test
  public void testSCMDatanodeHeartbeatDispatcherBuilder() {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeHeartbeatDispatcher dispatcher =
        SCMDatanodeHeartbeatDispatcher.newBuilder(conf, null)
        .addHandlerFor(NodeReportProto.class)
        .addHandlerFor(ContainerReportsProto.class)
        .build();
    Assert.assertNotNull(dispatcher);
  }

  @Test
  public void testNodeReportDispatcher() throws IOException {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeNodeReportHandler nodeReportHandler =
        Mockito.mock(SCMDatanodeNodeReportHandler.class);
    SCMDatanodeHeartbeatDispatcher dispatcher =
        SCMDatanodeHeartbeatDispatcher.newBuilder(conf, null)
            .addHandler(NodeReportProto.class, nodeReportHandler)
            .build();

    DatanodeDetails datanodeDetails = TestUtils.getDatanodeDetails();
    NodeReportProto nodeReport = NodeReportProto.getDefaultInstance();
    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
        .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
        .setNodeReport(nodeReport)
        .build();
    dispatcher.dispatch(heartbeat);
    verify(nodeReportHandler,
        times(1))
        .processReport(any(DatanodeDetails.class), eq(nodeReport));
  }

  @Test
  public void testContainerReportDispatcher() throws IOException {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeContainerReportHandler containerReportHandler =
        Mockito.mock(SCMDatanodeContainerReportHandler.class);
    SCMDatanodeHeartbeatDispatcher dispatcher =
        SCMDatanodeHeartbeatDispatcher.newBuilder(conf, null)
            .addHandler(ContainerReportsProto.class, containerReportHandler)
            .build();

    DatanodeDetails datanodeDetails = TestUtils.getDatanodeDetails();
    ContainerReportsProto containerReport =
        ContainerReportsProto.getDefaultInstance();
    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
            .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
            .setContainerReport(containerReport)
            .build();
    dispatcher.dispatch(heartbeat);
    verify(containerReportHandler,
        times(1))
        .processReport(any(DatanodeDetails.class),
            any(ContainerReportsProto.class));
  }

  @Test
  public void testNodeAndContainerReportDispatcher() throws IOException {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeNodeReportHandler nodeReportHandler =
        Mockito.mock(SCMDatanodeNodeReportHandler.class);
    SCMDatanodeContainerReportHandler containerReportHandler =
        Mockito.mock(SCMDatanodeContainerReportHandler.class);
    SCMDatanodeHeartbeatDispatcher dispatcher =
        SCMDatanodeHeartbeatDispatcher.newBuilder(conf, null)
            .addHandler(NodeReportProto.class, nodeReportHandler)
            .addHandler(ContainerReportsProto.class, containerReportHandler)
            .build();

    DatanodeDetails datanodeDetails = TestUtils.getDatanodeDetails();
    NodeReportProto nodeReport = NodeReportProto.getDefaultInstance();
    ContainerReportsProto containerReport =
        ContainerReportsProto.getDefaultInstance();
    SCMHeartbeatRequestProto heartbeat =
        SCMHeartbeatRequestProto.newBuilder()
            .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
            .setNodeReport(nodeReport)
            .setContainerReport(containerReport)
            .build();
    dispatcher.dispatch(heartbeat);
    verify(nodeReportHandler,
        times(1))
        .processReport(any(DatanodeDetails.class), any(NodeReportProto.class));
    verify(containerReportHandler,
        times(1))
        .processReport(any(DatanodeDetails.class),
            any(ContainerReportsProto.class));
  }

}
