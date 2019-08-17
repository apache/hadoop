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
import com.google.protobuf.GeneratedMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases to test {@link ReportPublisher}.
 */
public class TestReportPublisher {

  private static Configuration config;

  @BeforeClass
  public static void setup() {
    config = new OzoneConfiguration();
  }

  /**
   * Dummy report publisher for testing.
   */
  private static class DummyReportPublisher extends ReportPublisher {

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
    Assert.assertEquals(1, ((DummyReportPublisher) publisher).getReportCount);
    Thread.sleep(100);
    Assert.assertEquals(2, ((DummyReportPublisher) publisher).getReportCount);
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
    Assert.assertEquals(1, ((DummyReportPublisher) publisher).getReportCount);
    verify(dummyContext, times(1)).addReport(null);

  }

  @Test
  public void testCommandStatusPublisher() throws InterruptedException {
    StateContext dummyContext = Mockito.mock(StateContext.class);
    ReportPublisher publisher = new CommandStatusReportPublisher();
    final Map<Long, CommandStatus> cmdStatusMap = new ConcurrentHashMap<>();
    when(dummyContext.getCommandStatusMap()).thenReturn(cmdStatusMap);
    publisher.setConf(config);

    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Unit test ReportManager Thread - %d").build());
    publisher.init(dummyContext, executorService);
    Assert.assertNull(((CommandStatusReportPublisher) publisher).getReport());

    // Insert to status object to state context map and then get the report.
    CommandStatus obj1 = CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(HddsIdFactory.getLongId())
        .setType(Type.deleteBlocksCommand)
        .setStatus(Status.PENDING)
        .build();
    CommandStatus obj2 = CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(HddsIdFactory.getLongId())
        .setType(Type.closeContainerCommand)
        .setStatus(Status.EXECUTED)
        .build();
    cmdStatusMap.put(obj1.getCmdId(), obj1);
    cmdStatusMap.put(obj2.getCmdId(), obj2);
    // We are not sending the commands whose status is PENDING.
    Assert.assertEquals("Should publish report with 2 status objects", 1,
        ((CommandStatusReportPublisher) publisher).getReport()
            .getCmdStatusCount());
    executorService.shutdown();
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

}
