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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ServiceManager}.
 */
public class TestServiceManager {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testUpgrade() throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager("testUpgrade");
    upgrade(serviceManager, "v2", false);
    Assert.assertEquals("service not upgraded", ServiceState.UPGRADING,
        serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testRestartNothingToUpgrade()
      throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager("testRestart");
    upgrade(serviceManager, "v2", false);

    //make components stable
    serviceManager.getServiceSpec().getComponents().forEach(comp -> {
      comp.setState(ComponentState.STABLE);
    });
    serviceManager.handle(new ServiceEvent(ServiceEventType.START));
    Assert.assertEquals("service not re-started", ServiceState.STABLE,
        serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testRestartWithPendingUpgrade()
      throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager("testRestart");
    upgrade(serviceManager, "v2", true);
    serviceManager.handle(new ServiceEvent(ServiceEventType.START));
    Assert.assertEquals("service should still be upgrading",
        ServiceState.UPGRADING, serviceManager.getServiceSpec().getState());
  }


  private void upgrade(ServiceManager service, String version,
      boolean upgradeArtifact)
      throws IOException, SliderException {
    Service upgradedDef = ServiceTestUtils.createExampleApplication();
    upgradedDef.setName(service.getName());
    upgradedDef.setVersion(version);
    if (upgradeArtifact) {
      Artifact upgradedArtifact = createTestArtifact("2");
      upgradedDef.getComponents().forEach(component -> {
        component.setArtifact(upgradedArtifact);
      });
    }
    writeUpgradedDef(upgradedDef);
    ServiceEvent upgradeEvent = new ServiceEvent(ServiceEventType.UPGRADE);
    upgradeEvent.setVersion("v2");
    service.handle(upgradeEvent);
  }

  private ServiceManager createTestServiceManager(String name)
      throws IOException {
    ServiceContext context = new ServiceContext();
    context.service = createBaseDef(name);
    context.fs = rule.getFs();

    context.scheduler = new ServiceScheduler(context) {
      @Override
      protected YarnRegistryViewForProviders createYarnRegistryOperations(
          ServiceContext context, RegistryOperations registryClient) {
        return mock(YarnRegistryViewForProviders.class);
      }
    };

    context.scheduler.init(rule.getConf());

    Map<String, org.apache.hadoop.yarn.service.component.Component>
        componentState = context.scheduler.getAllComponents();
    context.service.getComponents().forEach(component -> {
      componentState.put(component.getName(),
          new org.apache.hadoop.yarn.service.component.Component(component,
              1L, context));
    });
    return new ServiceManager(context);
  }

  static Service createBaseDef(String name) {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    Service serviceDef = ServiceTestUtils.createExampleApplication();
    serviceDef.setId(applicationId.toString());
    serviceDef.setName(name);
    serviceDef.setState(ServiceState.STARTED);
    Artifact artifact = createTestArtifact("1");

    serviceDef.getComponents().forEach(component ->
        component.setArtifact(artifact));
    return serviceDef;
  }

  static Artifact createTestArtifact(String artifactId) {
    Artifact artifact = new Artifact();
    artifact.setId(artifactId);
    artifact.setType(Artifact.TypeEnum.TARBALL);
    return artifact;
  }

  private void writeUpgradedDef(Service upgradedDef)
      throws IOException, SliderException {
    Path upgradePath = rule.getFs().buildClusterUpgradeDirPath(
        upgradedDef.getName(), upgradedDef.getVersion());
    ServiceApiUtil.createDirAndPersistApp(rule.getFs(), upgradePath,
        upgradedDef);
  }

}