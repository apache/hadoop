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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ComponentContainers;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.ErrorStrings;
import org.apache.hadoop.yarn.service.utils.FilterUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ServiceClient}.
 */
public class TestServiceClient {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestServiceClient.class);

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testUpgradeDisabledByDefault() throws Exception {
    Service service = createService();
    ServiceClient client = MockServiceClient.create(rule, service, false);

    //upgrade the service
    service.setVersion("v2");
    try {
      client.initiateUpgrade(service);
    } catch (YarnException ex) {
      Assert.assertEquals(ErrorStrings.SERVICE_UPGRADE_DISABLED,
          ex.getMessage());
      return;
    }
    Assert.fail();
  }

  @Test
  public void testActionServiceUpgrade() throws Exception {
    Service service = createService();
    ServiceClient client = MockServiceClient.create(rule, service, true);

    //upgrade the service
    service.setVersion("v2");
    client.initiateUpgrade(service);

    Service fromFs = ServiceApiUtil.loadServiceUpgrade(rule.getFs(),
        service.getName(), service.getVersion());
    Assert.assertEquals(service.getName(), fromFs.getName());
    Assert.assertEquals(service.getVersion(), fromFs.getVersion());
    client.stop();
  }

  @Test
  public void testActionCompInstanceUpgrade() throws Exception {
    Service service = createService();
    MockServiceClient client = MockServiceClient.create(rule, service, true);

    //upgrade the service
    service.setVersion("v2");
    client.initiateUpgrade(service);

    //add containers to the component that needs to be upgraded.
    Component comp = service.getComponents().iterator().next();
    ContainerId containerId = ContainerId.newContainerId(client.attemptId, 1L);
    comp.addContainer(new Container().id(containerId.toString()));

    client.actionUpgrade(service, comp.getContainers());
    CompInstancesUpgradeResponseProto response = client.getLastProxyResponse(
        CompInstancesUpgradeResponseProto.class);
    Assert.assertNotNull("upgrade did not complete", response);
    client.stop();
  }

  @Test
  public void testGetCompInstances() throws Exception {
    Service service = createService();
    MockServiceClient client = MockServiceClient.create(rule, service, true);

    //upgrade the service
    service.setVersion("v2");
    client.initiateUpgrade(service);

    //add containers to the component that needs to be upgraded.
    Component comp = service.getComponents().iterator().next();
    ContainerId containerId = ContainerId.newContainerId(client.attemptId, 1L);
    comp.addContainer(new Container().id(containerId.toString()));

    ComponentContainers[] compContainers = client.getContainers(
        service.getName(), Lists.newArrayList("compa"), "v1", null);
    Assert.assertEquals("num comp", 1, compContainers.length);
    Assert.assertEquals("comp name", "compa",
        compContainers[0].getComponentName());
    Assert.assertEquals("num containers", 2,
        compContainers[0].getContainers().size());
    client.stop();
  }

  @Test
  public void testUpgradeDisabledWhenAllCompsHaveNeverRestartPolicy()
      throws Exception {
    Service service = createService();
    service.getComponents().forEach(comp ->
        comp.setRestartPolicy(Component.RestartPolicyEnum.NEVER));

    ServiceClient client = MockServiceClient.create(rule, service, true);

    //upgrade the service
    service.setVersion("v2");
    try {
      client.initiateUpgrade(service);
    } catch (YarnException ex) {
      Assert.assertEquals("All the components of the service " +
              service.getName() + " have " + Component.RestartPolicyEnum.NEVER
              + " restart policy, so it cannot be upgraded.",
          ex.getMessage());
      return;
    }
    Assert.fail();
  }

  private Service createService() throws IOException,
      YarnException {
    Service service = ServiceTestUtils.createExampleApplication();
    service.setVersion("v1");
    service.setState(ServiceState.UPGRADING);
    return service;
  }

  private static final class MockServiceClient extends ServiceClient {

    private final ApplicationId appId;
    private final ApplicationAttemptId attemptId;
    private final ClientAMProtocol amProxy;
    private Object proxyResponse;
    private Service service;
    private ServiceContext context;

    private MockServiceClient()  {
      amProxy = mock(ClientAMProtocol.class);
      appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
      LOG.debug("mocking service client for {}", appId);
      attemptId = ApplicationAttemptId.newInstance(appId, 1);
    }

    static MockServiceClient create(ServiceTestUtils.ServiceFSWatcher rule,
        Service service, boolean enableUpgrade)
        throws Exception {
      MockServiceClient client = new MockServiceClient();
      ApplicationId applicationId = ApplicationId.newInstance(
          System.currentTimeMillis(), 1);
      service.setId(applicationId.toString());
      client.context = new MockRunningServiceContext(rule, service);

      YarnClient yarnClient = createMockYarnClient();
      ApplicationReport appReport = mock(ApplicationReport.class);
      when(appReport.getHost()).thenReturn("localhost");
      when(appReport.getYarnApplicationState()).thenReturn(
          YarnApplicationState.RUNNING);

      ApplicationAttemptReport attemptReport =
          ApplicationAttemptReport.newInstance(client.attemptId, "localhost", 0,
              null, null, null,
              YarnApplicationAttemptState.RUNNING, null);
      when(yarnClient.getApplicationAttemptReport(any()))
          .thenReturn(attemptReport);
      when(yarnClient.getApplicationReport(client.appId)).thenReturn(appReport);
      when(client.amProxy.upgrade(
          any(UpgradeServiceRequestProto.class))).thenAnswer(
          (Answer<UpgradeServiceResponseProto>) invocation -> {
              UpgradeServiceResponseProto response =
                  UpgradeServiceResponseProto.newBuilder().build();
              client.proxyResponse = response;
              return response;
            });
      when(client.amProxy.upgrade(any(
          CompInstancesUpgradeRequestProto.class))).thenAnswer(
          (Answer<CompInstancesUpgradeResponseProto>) invocation -> {
              CompInstancesUpgradeResponseProto response =
                  CompInstancesUpgradeResponseProto.newBuilder().build();
              client.proxyResponse = response;
              return response;
            });

      when(client.amProxy.getCompInstances(any(
          GetCompInstancesRequestProto.class))).thenAnswer(
          (Answer<GetCompInstancesResponseProto>) invocation -> {

              GetCompInstancesRequestProto req = (GetCompInstancesRequestProto)
                  invocation.getArguments()[0];

              List<ComponentContainers> compContainers =
                  FilterUtils.filterInstances(client.context, req);
              GetCompInstancesResponseProto response =
                  GetCompInstancesResponseProto.newBuilder().setCompInstances(
                      ServiceApiUtil.COMP_CONTAINERS_JSON_SERDE.toJson(
                          compContainers.toArray(
                              new ComponentContainers[compContainers.size()])))
                      .build();

              client.proxyResponse = response;
              return response;
          });

      client.setFileSystem(rule.getFs());
      client.setYarnClient(yarnClient);
      client.service = service;
      rule.getConf().setBoolean(YarnServiceConf.YARN_SERVICE_UPGRADE_ENABLED,
          enableUpgrade);
      client.init(rule.getConf());
      client.start();
      client.actionCreate(service);
      return client;
    }

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
      return appId;
    }

    @Override
    public Service getStatus(String serviceName) throws IOException,
        YarnException {
      service.setState(ServiceState.STABLE);
      return service;
    }

    private <T> T getLastProxyResponse(Class<T> clazz) {
      if (clazz.isInstance(proxyResponse)) {
        return clazz.cast(proxyResponse);
      }
      return null;
    }
  }

  private static YarnClient createMockYarnClient() throws IOException,
      YarnException {
    YarnClient yarnClient = mock(YarnClient.class);
    when(yarnClient.getApplications(any(
        GetApplicationsRequest.class))).thenReturn(new ArrayList<>());
    return yarnClient;
  }
}