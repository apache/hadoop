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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;


public class TestClientRMService {

  private static final Log LOG = LogFactory.getLog(TestClientRMService.class);

  @Test
  public void testGetClusterNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager,
          this.rmDTSecretManager);
      };
    };
    rm.start();

    // Add a healthy node
    MockNM node = rm.registerNode("host:1234", 1024);
    node.nodeHeartbeat(true);

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ClientRMProtocol client =
        (ClientRMProtocol) rpc
          .getProxy(ClientRMProtocol.class, rmAddress, conf);

    // Make call
    GetClusterNodesRequest request =
        Records.newRecord(GetClusterNodesRequest.class);
    List<NodeReport> nodeReports =
        client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(1, nodeReports.size());
    Assert.assertTrue("Node is expected to be healthy!", nodeReports.get(0)
      .getNodeHealthStatus().getIsNodeHealthy());

    // Now make the node unhealthy.
    node.nodeHeartbeat(false);

    // Call again
    nodeReports = client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(1, nodeReports.size());
    Assert.assertFalse("Node is expected to be unhealthy!", nodeReports.get(0)
      .getNodeHealthStatus().getIsNodeHealthy());
  }
  
  @Test
  public void testGetApplicationReport() throws YarnRemoteException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(recordFactory
        .newRecordInstance(ApplicationId.class));
    GetApplicationReportResponse applicationReport = rmService
        .getApplicationReport(request);
    Assert.assertNull("It should return null as application report for absent application.",
        applicationReport.getApplicationReport());
  }
}
