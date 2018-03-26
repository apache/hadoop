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

package org.apache.hadoop.yarn.service.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ServiceClient}.
 */
public class TestServiceClient {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testActionUpgrade() throws Exception {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    ServiceClient client = createServiceClient(applicationId);

    Service service = ServiceTestUtils.createExampleApplication();
    service.setVersion("v1");
    client.actionCreate(service);

    //upgrade the service
    service.setVersion("v2");
    client.actionUpgrade(service);

    //wait for service to be in upgrade state
    Service fromFs = ServiceApiUtil.loadServiceUpgrade(rule.getFs(),
        service.getName(), service.getVersion());
    Assert.assertEquals(service.getName(), fromFs.getName());
    Assert.assertEquals(service.getVersion(), fromFs.getVersion());
  }


  private ServiceClient createServiceClient(ApplicationId applicationId)
      throws Exception {
    ClientAMProtocol amProxy = mock(ClientAMProtocol.class);
    YarnClient yarnClient = createMockYarnClient();
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ApplicationAttemptReport attemptReport =
        ApplicationAttemptReport.newInstance(attemptId, "localhost", 0,
            null, null, null,
        YarnApplicationAttemptState.RUNNING, null);

    ApplicationReport appReport = mock(ApplicationReport.class);
    when(appReport.getHost()).thenReturn("localhost");

    when(yarnClient.getApplicationAttemptReport(Matchers.any()))
        .thenReturn(attemptReport);
    when(yarnClient.getApplicationReport(applicationId)).thenReturn(appReport);

    ServiceClient client = new ServiceClient() {
      @Override
      protected void serviceInit(Configuration configuration) throws Exception {
      }

      @Override
      protected ClientAMProtocol createAMProxy(String serviceName,
          ApplicationReport appReport) throws IOException, YarnException {
        return amProxy;
      }

      @Override
      ApplicationId submitApp(Service app) throws IOException, YarnException {
        return applicationId;
      }
    };

    client.setFileSystem(rule.getFs());
    client.setYarnClient(yarnClient);

    client.init(rule.getConf());
    client.start();
    return client;
  }

  private YarnClient createMockYarnClient() throws IOException, YarnException {
    YarnClient yarnClient = mock(YarnClient.class);
    when(yarnClient.getApplications(Matchers.any(GetApplicationsRequest.class)))
        .thenReturn(new ArrayList<>());
    return yarnClient;
  }
}