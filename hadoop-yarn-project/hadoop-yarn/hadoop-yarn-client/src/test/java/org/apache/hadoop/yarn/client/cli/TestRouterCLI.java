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
package org.apache.hadoop.yarn.client.cli;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRouterCLI {

  private ResourceManagerAdministrationProtocol admin;
  private RouterCLI rmAdminCLI;

  @Before
  public void setup() throws Exception {
    admin = mock(ResourceManagerAdministrationProtocol.class);
    when(admin.deregisterSubCluster(any(DeregisterSubClusterRequest.class)))
        .thenAnswer((Answer<DeregisterSubClusterResponse>) invocationOnMock -> {
          // Step1. parse subClusterId.
          Object obj = invocationOnMock.getArgument(0);
          DeregisterSubClusterRequest request = DeregisterSubClusterRequest.class.cast(obj);
          String subClusterId = request.getSubClusterId();

          if (StringUtils.isNotBlank(subClusterId)) {
            return generateSubClusterDataBySCId(subClusterId);
          } else {
            return generateAllSubClusterData();
          }
        });

      rmAdminCLI = new RouterCLI(new Configuration()) {
          @Override
          protected ResourceManagerAdministrationProtocol createAdminProtocol()
              throws IOException {
              return admin;
          }
      };
  }

  private DeregisterSubClusterResponse generateSubClusterDataBySCId(String subClusterId) {
    // Step2. generate return data.
    String lastHeartBeatTime = new Date().toString();
    DeregisterSubClusters deregisterSubClusters =
        DeregisterSubClusters.newInstance(subClusterId, "SUCCESS", lastHeartBeatTime,
        "Heartbeat Time > 30 minutes", "SC_LOST");
    List<DeregisterSubClusters> deregisterSubClusterList = new ArrayList<>();
    deregisterSubClusterList.add(deregisterSubClusters);

    // Step3. return data.
    DeregisterSubClusterResponse response =
        DeregisterSubClusterResponse.newInstance(deregisterSubClusterList);
    return response;
  }

  private DeregisterSubClusterResponse generateAllSubClusterData() {
    List<DeregisterSubClusters> deregisterSubClusterList = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      String subClusterId = "SC-" + i;
      String lastHeartBeatTime = new Date().toString();
      DeregisterSubClusters deregisterSubClusters =
          DeregisterSubClusters.newInstance(subClusterId, "SUCCESS", lastHeartBeatTime,
          "Heartbeat Time > 30 minutes", "SC_LOST");
      deregisterSubClusterList.add(deregisterSubClusters);
    }

    DeregisterSubClusterResponse response =
        DeregisterSubClusterResponse.newInstance(deregisterSubClusterList);
    return response;
  }

  @Test
  public void testHelp() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));

    String[] args = {"-help"};
    assertEquals(0, rmAdminCLI.run(args));
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    oldOutPrintStream.println(dataOut);
    String[] args = {"-deregisterSubCluster", "-c", "SC-1"};
    assertEquals(0, rmAdminCLI.run(args));
  }

  @Test
  public void testDeregisterSubClusters() throws Exception {
    String[] args = {"-deregisterSubCluster", "-c", ""};
    assertEquals(0, rmAdminCLI.run(args));
  }
}
