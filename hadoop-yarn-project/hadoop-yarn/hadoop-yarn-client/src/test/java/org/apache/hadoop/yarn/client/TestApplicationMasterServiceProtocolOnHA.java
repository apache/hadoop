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

import org.junit.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Before;
import org.junit.Test;


public class TestApplicationMasterServiceProtocolOnHA
    extends ApplicationMasterServiceProtoTestBase {
  @Before
  public void initialize() throws Exception {
    startHACluster(0, false, false, true);
    super.startupHAAndSetupClient();
  }

  @Test(timeout = 15000)
  public void testRegisterApplicationMasterOnHA() throws YarnException,
      IOException {
    RegisterApplicationMasterRequest request =
        RegisterApplicationMasterRequest.newInstance("localhost", 0, "");
    RegisterApplicationMasterResponse response =
        getAMClient().registerApplicationMaster(request);
    Assert.assertEquals(response,
        this.cluster.createFakeRegisterApplicationMasterResponse());
  }

  @Test(timeout = 15000)
  public void testFinishApplicationMasterOnHA() throws YarnException,
      IOException {
    FinishApplicationMasterRequest request =
        FinishApplicationMasterRequest.newInstance(
            FinalApplicationStatus.SUCCEEDED, "", "");
    FinishApplicationMasterResponse response =
        getAMClient().finishApplicationMaster(request);
    Assert.assertEquals(response,
        this.cluster.createFakeFinishApplicationMasterResponse());
  }

  @Test(timeout = 15000)
  public void testAllocateOnHA() throws YarnException, IOException {
    AllocateRequest request = AllocateRequest.newInstance(0, 50f,
        new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>(),
        ResourceBlacklistRequest.newInstance(new ArrayList<String>(),
            new ArrayList<String>()));
    AllocateResponse response = getAMClient().allocate(request);
    Assert.assertEquals(response, this.cluster.createFakeAllocateResponse());
  }
}
