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
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases to verify the functionality of SCMDatanodeReportHandlerFactory.
 */
public class TestSCMDatanodeReportHandlerFactory {

  @Test
  public void testNodeReportHandlerConstruction() {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeReportHandlerFactory factory =
        new SCMDatanodeReportHandlerFactory(conf, null);
    Assert.assertTrue(factory.getHandlerFor(NodeReportProto.class)
        instanceof SCMDatanodeNodeReportHandler);
  }

  @Test
  public void testContainerReporttHandlerConstruction() {
    Configuration conf = new OzoneConfiguration();
    SCMDatanodeReportHandlerFactory factory =
        new SCMDatanodeReportHandlerFactory(conf, null);
    Assert.assertTrue(factory.getHandlerFor(ContainerReportsProto.class)
        instanceof SCMDatanodeContainerReportHandler);
  }
}
