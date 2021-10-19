/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.util.ArrayList;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests Application Master Protocol with timeline service v2 enabled.
 */
public class TestApplicationMasterServiceProtocolForTimelineV2
    extends ApplicationMasterServiceProtoTestBase {

  public Timeout timeout = new Timeout(180, TimeUnit.SECONDS);

  @Before
  public void initialize() throws Exception {
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE + 200, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE + 200, conf);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
        FileSystemTimelineWriterImpl.class, TimelineWriter.class);
    startHACluster(0, false, false, true);
    super.startupHAAndSetupClient();
  }

  @Test
  public void testAllocateForTimelineV2OnHA()
      throws YarnException, IOException {
    AllocateRequest request = AllocateRequest.newInstance(0, 50f,
        new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>(),
        ResourceBlacklistRequest.newInstance(new ArrayList<String>(),
            new ArrayList<String>()));
    AllocateResponse response = getAMClient().allocate(request);
    Assert.assertEquals(response, this.cluster.createFakeAllocateResponse());
    Assert.assertNotNull(response.getCollectorInfo());
    Assert.assertEquals("host:port",
        response.getCollectorInfo().getCollectorAddr());
    Assert.assertNotNull(response.getCollectorInfo().getCollectorToken());
  }
}
