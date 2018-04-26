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
    upgrade(serviceManager, "v2", false, false);
    Assert.assertEquals("service not upgraded", ServiceState.UPGRADING,
        serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testRestartNothingToUpgrade()
      throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager(
        "testRestartNothingToUpgrade");
    upgrade(serviceManager, "v2", false, false);

    //make components stable
    serviceManager.getServiceSpec().getComponents().forEach(comp -> {
      comp.setState(ComponentState.STABLE);
    });
    serviceManager.handle(new ServiceEvent(ServiceEventType.START));
    Assert.assertEquals("service not re-started", ServiceState.STABLE,
        serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testAutoFinalizeNothingToUpgrade() throws IOException,
      SliderException {
    ServiceManager serviceManager = createTestServiceManager(
        "testAutoFinalizeNothingToUpgrade");
    upgrade(serviceManager, "v2", false, true);

    //make components stable
    serviceManager.getServiceSpec().getComponents().forEach(comp ->
        comp.setState(ComponentState.STABLE));
    serviceManager.handle(new ServiceEvent(ServiceEventType.CHECK_STABLE));
    Assert.assertEquals("service stable", ServiceState.STABLE,
        serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testRestartWithPendingUpgrade()
      throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager("testRestart");
    upgrade(serviceManager, "v2", true, false);
    serviceManager.handle(new ServiceEvent(ServiceEventType.START));
    Assert.assertEquals("service should still be upgrading",
        ServiceState.UPGRADING, serviceManager.getServiceSpec().getState());
  }

  @Test
  public void testCheckState() throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager(
        "testCheckState");
    upgrade(serviceManager, "v2", true, false);
    Assert.assertEquals("service not upgrading", ServiceState.UPGRADING,
        serviceManager.getServiceSpec().getState());

    // make components stable
    serviceManager.getServiceSpec().getComponents().forEach(comp -> {
      comp.setState(ComponentState.STABLE);
    });
    ServiceEvent checkStable = new ServiceEvent(ServiceEventType.CHECK_STABLE);
    serviceManager.handle(checkStable);
    Assert.assertEquals("service should still be upgrading",
        ServiceState.UPGRADING, serviceManager.getServiceSpec().getState());

    // finalize service
    ServiceEvent restart = new ServiceEvent(ServiceEventType.START);
    serviceManager.handle(restart);
    Assert.assertEquals("service not stable",
        ServiceState.STABLE, serviceManager.getServiceSpec().getState());

    validateUpgradeFinalization(serviceManager.getName(), "v2");
  }

  @Test
  public void testCheckStateAutoFinalize() throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager(
        "testCheckState");
    serviceManager.getServiceSpec().setState(
        ServiceState.UPGRADING_AUTO_FINALIZE);
    upgrade(serviceManager, "v2", true, true);
    Assert.assertEquals("service not upgrading",
        ServiceState.UPGRADING_AUTO_FINALIZE,
        serviceManager.getServiceSpec().getState());

    // make components stable
    serviceManager.getServiceSpec().getComponents().forEach(comp ->
        comp.setState(ComponentState.STABLE));
    ServiceEvent checkStable = new ServiceEvent(ServiceEventType.CHECK_STABLE);
    serviceManager.handle(checkStable);
    Assert.assertEquals("service not stable",
        ServiceState.STABLE, serviceManager.getServiceSpec().getState());

    validateUpgradeFinalization(serviceManager.getName(), "v2");
  }

  @Test
  public void testInvalidUpgrade() throws IOException, SliderException {
    ServiceManager serviceManager = createTestServiceManager(
        "testInvalidUpgrade");
    serviceManager.getServiceSpec().setState(
        ServiceState.UPGRADING_AUTO_FINALIZE);
    Service upgradedDef = ServiceTestUtils.createExampleApplication();
    upgradedDef.setName(serviceManager.getName());
    upgradedDef.setVersion("v2");
    upgradedDef.setLifetime(2L);
    writeUpgradedDef(upgradedDef);

    try {
      serviceManager.processUpgradeRequest("v2", true);
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof UnsupportedOperationException);
      return;
    }
    Assert.fail();
  }

  private void validateUpgradeFinalization(String serviceName,
      String expectedVersion) throws IOException {
    Service savedSpec = ServiceApiUtil.loadService(rule.getFs(), serviceName);
    Assert.assertEquals("service def not re-written", expectedVersion,
        savedSpec.getVersion());
    Assert.assertNotNull("app id not present", savedSpec.getId());
    Assert.assertEquals("state not stable", ServiceState.STABLE,
        savedSpec.getState());
    savedSpec.getComponents().forEach(compSpec -> {
      Assert.assertEquals("comp not stable", ComponentState.STABLE,
          compSpec.getState());
    });
  }

  private void upgrade(ServiceManager serviceManager, String version,
      boolean upgradeArtifact, boolean autoFinalize)
      throws IOException, SliderException {
    Service upgradedDef = ServiceTestUtils.createExampleApplication();
    upgradedDef.setName(serviceManager.getName());
    upgradedDef.setVersion(version);
    if (upgradeArtifact) {
      Artifact upgradedArtifact = createTestArtifact("2");
      upgradedDef.getComponents().forEach(component -> {
        component.setArtifact(upgradedArtifact);
      });
    }
    writeUpgradedDef(upgradedDef);
    serviceManager.processUpgradeRequest(version, autoFinalize);
    ServiceEvent upgradeEvent = new ServiceEvent(ServiceEventType.UPGRADE);
    upgradeEvent.setVersion(version);
    if (autoFinalize) {
      upgradeEvent.setAutoFinalize(true);
    }
    serviceManager.handle(upgradeEvent);
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

  public static Service createBaseDef(String name) {
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