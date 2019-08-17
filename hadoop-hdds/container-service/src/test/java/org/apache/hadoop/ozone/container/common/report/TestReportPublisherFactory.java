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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test cases to test ReportPublisherFactory.
 */
public class TestReportPublisherFactory {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testGetContainerReportPublisher() {
    Configuration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    ReportPublisher publisher = factory
        .getPublisherFor(ContainerReportsProto.class);
    Assert.assertEquals(ContainerReportPublisher.class, publisher.getClass());
    Assert.assertEquals(conf, publisher.getConf());
  }

  @Test
  public void testGetNodeReportPublisher() {
    Configuration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    ReportPublisher publisher = factory
        .getPublisherFor(NodeReportProto.class);
    Assert.assertEquals(NodeReportPublisher.class, publisher.getClass());
    Assert.assertEquals(conf, publisher.getConf());
  }

  @Test
  public void testInvalidReportPublisher() {
    Configuration conf = new OzoneConfiguration();
    ReportPublisherFactory factory = new ReportPublisherFactory(conf);
    exception.expect(RuntimeException.class);
    exception.expectMessage("No publisher found for report");
    factory.getPublisherFor(HddsProtos.DatanodeDetailsProto.class);
  }
}
