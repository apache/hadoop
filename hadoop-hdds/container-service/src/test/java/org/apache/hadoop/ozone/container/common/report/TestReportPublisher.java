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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test cases to test {@link ReportPublisher}.
 */
public class TestReportPublisher {

  /**
   * Dummy report publisher for testing.
   */
  private class DummyReportPublisher extends ReportPublisher {

    private final long frequency;
    private int getReportCount = 0;

    DummyReportPublisher(long frequency) {
      this.frequency = frequency;
    }

    @Override
    protected long getReportFrequency() {
      return frequency;
    }

    @Override
    protected GeneratedMessage getReport() {
      getReportCount++;
      return null;
    }
  }

  @Test
  public void testReportPublisherInit() {
    ReportPublisher publisher = new DummyReportPublisher(0);
    StateContext dummyContext = Mockito.mock(StateContext.class);
    ScheduledExecutorService dummyExecutorService = Mockito.mock(
        ScheduledExecutorService.class);
    publisher.init(dummyContext, dummyExecutorService);
    verify(dummyExecutorService, times(1)).schedule(publisher,
        0, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testScheduledReport() throws InterruptedException {
    ReportPublisher publisher = new DummyReportPublisher(100);
    StateContext dummyContext = Mockito.mock(StateContext.class);
    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Unit test ReportManager Thread - %d").build());
    publisher.init(dummyContext, executorService);
    Thread.sleep(150);
    Assert.assertEquals(1, ((DummyReportPublisher)publisher).getReportCount);
    Thread.sleep(150);
    Assert.assertEquals(2, ((DummyReportPublisher)publisher).getReportCount);
    executorService.shutdown();
  }

  @Test
  public void testPublishReport() throws InterruptedException {
    ReportPublisher publisher = new DummyReportPublisher(100);
    StateContext dummyContext = Mockito.mock(StateContext.class);
    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Unit test ReportManager Thread - %d").build());
    publisher.init(dummyContext, executorService);
    Thread.sleep(150);
    executorService.shutdown();
    Assert.assertEquals(1, ((DummyReportPublisher)publisher).getReportCount);
    verify(dummyContext, times(1)).addReport(null);

  }

  @Test
  public void testAddingReportToHeartbeat() {
    Configuration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    ReportPublisher nodeReportPublisher = factory.getPublisherFor(
        NodeReportProto.class);
    ReportPublisher containerReportPubliser = factory.getPublisherFor(
        ContainerReportsProto.class);
    GeneratedMessage nodeReport = nodeReportPublisher.getReport();
    GeneratedMessage containerReport = containerReportPubliser.getReport();
    SCMHeartbeatRequestProto.Builder heartbeatBuilder =
        SCMHeartbeatRequestProto.newBuilder();
    heartbeatBuilder.setDatanodeDetails(
        getDatanodeDetails().getProtoBufMessage());
    addReport(heartbeatBuilder, nodeReport);
    addReport(heartbeatBuilder, containerReport);
    SCMHeartbeatRequestProto heartbeat = heartbeatBuilder.build();
    Assert.assertTrue(heartbeat.hasNodeReport());
    Assert.assertTrue(heartbeat.hasContainerReport());
  }

  /**
   * Get a datanode details.
   *
   * @return DatanodeDetails
   */
  private static DatanodeDetails getDatanodeDetails() {
    String uuid = UUID.randomUUID().toString();
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  /**
   * Adds the report to heartbeat.
   *
   * @param requestBuilder builder to which the report has to be added.
   * @param report the report to be added.
   */
  private static void addReport(SCMHeartbeatRequestProto.Builder requestBuilder,
                          GeneratedMessage report) {
    String reportName = report.getDescriptorForType().getFullName();
    for (Descriptors.FieldDescriptor descriptor :
        SCMHeartbeatRequestProto.getDescriptor().getFields()) {
      String heartbeatFieldName = descriptor.getMessageType().getFullName();
      if (heartbeatFieldName.equals(reportName)) {
        requestBuilder.setField(descriptor, report);
      }
    }
  }

}
