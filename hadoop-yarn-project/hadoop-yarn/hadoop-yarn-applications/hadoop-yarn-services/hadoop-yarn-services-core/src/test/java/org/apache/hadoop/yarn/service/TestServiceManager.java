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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.ComponentState;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link ServiceManager}.
 */
public class TestServiceManager {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test (timeout = TIMEOUT)
  public void testUpgrade() throws Exception {
    ServiceContext context = createServiceContext("testUpgrade");
    initUpgrade(context, "v2", false, false, false);
    Assert.assertEquals("service not upgraded", ServiceState.UPGRADING,
        context.getServiceManager().getServiceSpec().getState());
  }

  @Test (timeout = TIMEOUT)
  public void testRestartNothingToUpgrade()
      throws Exception {
    ServiceContext context = createServiceContext(
        "testRestartNothingToUpgrade");
    initUpgrade(context, "v2", false, false, false);
    ServiceManager manager = context.getServiceManager();
    //make components stable by upgrading all instances
    upgradeAndReadyAllInstances(context);

    context.scheduler.getDispatcher().getEventHandler().handle(
        new ServiceEvent(ServiceEventType.START));
    GenericTestUtils.waitFor(()->
        context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service not re-started", ServiceState.STABLE,
        manager.getServiceSpec().getState());
  }

  @Test(timeout = TIMEOUT)
  public void testAutoFinalizeNothingToUpgrade() throws Exception {
    ServiceContext context = createServiceContext(
        "testAutoFinalizeNothingToUpgrade");
    initUpgrade(context, "v2", false, true, false);
    ServiceManager manager = context.getServiceManager();
    //make components stable by upgrading all instances
    upgradeAndReadyAllInstances(context);

    GenericTestUtils.waitFor(()->
        context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service stable", ServiceState.STABLE,
        manager.getServiceSpec().getState());
  }

  @Test(timeout = TIMEOUT)
  public void testRestartWithPendingUpgrade()
      throws Exception {
    ServiceContext context = createServiceContext("testRestart");
    initUpgrade(context, "v2", true, false, false);
    ServiceManager manager = context.getServiceManager();

    context.scheduler.getDispatcher().getEventHandler().handle(
        new ServiceEvent(ServiceEventType.START));
    context.scheduler.getDispatcher().stop();
    Assert.assertEquals("service should still be upgrading",
        ServiceState.UPGRADING, manager.getServiceSpec().getState());
  }

  @Test(timeout = TIMEOUT)
  public void testFinalize() throws Exception {
    ServiceContext context = createServiceContext("testCheckState");
    initUpgrade(context, "v2", true, false, false);
    ServiceManager manager = context.getServiceManager();
    Assert.assertEquals("service not upgrading", ServiceState.UPGRADING,
        manager.getServiceSpec().getState());

    //make components stable by upgrading all instances
    upgradeAndReadyAllInstances(context);

    // finalize service
    context.scheduler.getDispatcher().getEventHandler().handle(
        new ServiceEvent(ServiceEventType.START));
    GenericTestUtils.waitFor(()->
        context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service not re-started", ServiceState.STABLE,
        manager.getServiceSpec().getState());

    validateUpgradeFinalization(manager.getName(), "v2");
  }

  @Test(timeout = TIMEOUT)
  public void testAutoFinalize() throws Exception {
    ServiceContext context = createServiceContext("testCheckStateAutoFinalize");
    ServiceManager manager = context.getServiceManager();
    manager.getServiceSpec().setState(
        ServiceState.UPGRADING_AUTO_FINALIZE);
    initUpgrade(context, "v2", true, true, false);

    // make components stable
    upgradeAndReadyAllInstances(context);

    GenericTestUtils.waitFor(() ->
        context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service not stable",
        ServiceState.STABLE, manager.getServiceSpec().getState());

    validateUpgradeFinalization(manager.getName(), "v2");
  }

  @Test
  public void testInvalidUpgrade() throws Exception {
    ServiceContext serviceContext = createServiceContext("testInvalidUpgrade");
    ServiceManager manager = serviceContext.getServiceManager();
    manager.getServiceSpec().setState(
        ServiceState.UPGRADING_AUTO_FINALIZE);
    Service upgradedDef = ServiceTestUtils.createExampleApplication();
    upgradedDef.setName(manager.getName());
    upgradedDef.setVersion("v2");
    upgradedDef.setLifetime(2L);
    writeUpgradedDef(upgradedDef);

    try {
      manager.processUpgradeRequest("v2", true, false);
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof UnsupportedOperationException);
      return;
    }
    Assert.fail();
  }

  @Test(timeout = TIMEOUT)
  public void testExpressUpgrade() throws Exception {
    ServiceContext context = createServiceContext("testExpressUpgrade");
    ServiceManager manager = context.getServiceManager();
    manager.getServiceSpec().setState(ServiceState.EXPRESS_UPGRADING);
    initUpgrade(context, "v2", true, true, true);

    List<String> comps = ServiceApiUtil.resolveCompsDependency(context.service);
    // wait till instances of first component are upgraded and ready
    String compA = comps.get(0);
    makeInstancesReadyAfterUpgrade(context, compA);

    // wait till instances of second component are upgraded and ready
    String compB = comps.get(1);
    makeInstancesReadyAfterUpgrade(context, compB);

    GenericTestUtils.waitFor(() ->
            context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);

    Assert.assertEquals("service not stable",
        ServiceState.STABLE, manager.getServiceSpec().getState());
    validateUpgradeFinalization(manager.getName(), "v2");
  }

  @Test(timeout = TIMEOUT)
  public void testCancelUpgrade() throws Exception {
    ServiceContext context = createServiceContext("testCancelUpgrade");
    writeInitialDef(context.service);
    initUpgrade(context, "v2", true, false, false);
    ServiceManager manager = context.getServiceManager();
    Assert.assertEquals("service not upgrading", ServiceState.UPGRADING,
        manager.getServiceSpec().getState());

    List<String> comps = ServiceApiUtil.resolveCompsDependency(context.service);
    // wait till instances of first component are upgraded and ready
    String compA = comps.get(0);
    // upgrade the instances
    upgradeInstances(context, compA);
    makeInstancesReadyAfterUpgrade(context, compA);

    // cancel upgrade
    context.scheduler.getDispatcher().getEventHandler().handle(
        new ServiceEvent(ServiceEventType.CANCEL_UPGRADE));
    makeInstancesReadyAfterUpgrade(context, compA);

    GenericTestUtils.waitFor(()->
            context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service upgrade not cancelled", ServiceState.STABLE,
        manager.getServiceSpec().getState());

    validateUpgradeFinalization(manager.getName(), "v1");
  }

  @Test(timeout = TIMEOUT)
  public void testCancelUpgradeAfterInitiate() throws Exception {
    ServiceContext context = createServiceContext("testCancelUpgrade");
    writeInitialDef(context.service);
    initUpgrade(context, "v2", true, false, false);
    ServiceManager manager = context.getServiceManager();
    Assert.assertEquals("service not upgrading", ServiceState.UPGRADING,
        manager.getServiceSpec().getState());

    // cancel upgrade
    context.scheduler.getDispatcher().getEventHandler().handle(
        new ServiceEvent(ServiceEventType.CANCEL_UPGRADE));
    GenericTestUtils.waitFor(()->
            context.service.getState().equals(ServiceState.STABLE),
        CHECK_EVERY_MILLIS, TIMEOUT);
    Assert.assertEquals("service upgrade not cancelled", ServiceState.STABLE,
        manager.getServiceSpec().getState());

    validateUpgradeFinalization(manager.getName(), "v1");
  }

  private void validateUpgradeFinalization(String serviceName,
      String expectedVersion) throws IOException {
    Service savedSpec = ServiceApiUtil.loadService(rule.getFs(), serviceName);
    Assert.assertEquals("service def not re-written", expectedVersion,
        savedSpec.getVersion());
    Assert.assertNotNull("app id not present", savedSpec.getId());
    Assert.assertEquals("state not stable", ServiceState.STABLE,
        savedSpec.getState());
    savedSpec.getComponents().forEach(compSpec ->
        Assert.assertEquals("comp not stable", ComponentState.STABLE,
        compSpec.getState()));
  }

  private void initUpgrade(ServiceContext context, String version,
      boolean upgradeArtifact, boolean autoFinalize, boolean expressUpgrade)
      throws IOException, SliderException, TimeoutException,
      InterruptedException {
    ServiceManager serviceManager = context.getServiceManager();
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
    serviceManager.processUpgradeRequest(version, autoFinalize, expressUpgrade);
    GenericTestUtils.waitFor(() -> {
      for (Component comp : context.scheduler.getAllComponents().values()) {
        if (!comp.getComponentSpec().getState().equals(
            ComponentState.NEEDS_UPGRADE)) {
          return false;
        }
      }
      return true;
    }, CHECK_EVERY_MILLIS, TIMEOUT);
  }

  private void upgradeAndReadyAllInstances(ServiceContext context) throws
      TimeoutException, InterruptedException {
    upgradeAllInstances(context);
    makeAllInstancesReady(context);
  }

  private void upgradeAllInstances(ServiceContext context) throws
      TimeoutException, InterruptedException {
    // upgrade the instances
    context.scheduler.getLiveInstances().forEach(((containerId, instance) -> {
      ComponentInstanceEvent event = new ComponentInstanceEvent(containerId,
          ComponentInstanceEventType.UPGRADE);
      context.scheduler.getDispatcher().getEventHandler().handle(event);
    }));
  }

  private void makeAllInstancesReady(ServiceContext context)
      throws TimeoutException, InterruptedException {
    context.scheduler.getLiveInstances().forEach(((containerId, instance) -> {
      ComponentInstanceEvent startEvent = new ComponentInstanceEvent(
          containerId, ComponentInstanceEventType.START);
      context.scheduler.getDispatcher().getEventHandler().handle(startEvent);

      ComponentInstanceEvent becomeReadyEvent = new ComponentInstanceEvent(
          containerId, ComponentInstanceEventType.BECOME_READY);
      context.scheduler.getDispatcher().getEventHandler().handle(
          becomeReadyEvent);
    }));
    GenericTestUtils.waitFor(()-> {
      for (ComponentInstance instance:
          context.scheduler.getLiveInstances().values()) {
        if (!instance.getContainerState().equals(ContainerState.READY)) {
          return false;
        }
      }
      return true;
    }, CHECK_EVERY_MILLIS, TIMEOUT);
  }

  private void upgradeInstances(ServiceContext context, String compName) {
    Collection<ComponentInstance> compInstances = context.scheduler
        .getAllComponents().get(compName).getAllComponentInstances();
    compInstances.forEach(instance -> {
      ComponentInstanceEvent event = new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.UPGRADE);
      context.scheduler.getDispatcher().getEventHandler().handle(event);
    });
  }

  private void makeInstancesReadyAfterUpgrade(ServiceContext context,
      String compName)
      throws TimeoutException, InterruptedException {
    Collection<ComponentInstance> compInstances = context.scheduler
        .getAllComponents().get(compName).getAllComponentInstances();
    GenericTestUtils.waitFor(() -> {
      for (ComponentInstance instance : compInstances) {
        if (!instance.getContainerState().equals(ContainerState.UPGRADING)) {
          return false;
        }
      }
      return true;
    }, CHECK_EVERY_MILLIS, TIMEOUT);

    // instances of comp1 get upgraded and become ready event is triggered
    // become ready
    compInstances.forEach(instance -> {
      ComponentInstanceEvent startEvent = new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.START);
      context.scheduler.getDispatcher().getEventHandler().handle(startEvent);

      ComponentInstanceEvent becomeReadyEvent = new ComponentInstanceEvent(
          instance.getContainer().getId(),
          ComponentInstanceEventType.BECOME_READY);

      context.scheduler.getDispatcher().getEventHandler().handle(
          becomeReadyEvent);
    });

    GenericTestUtils.waitFor(() -> {
      for (ComponentInstance instance : compInstances) {
        if (!instance.getContainerState().equals(ContainerState.READY)) {
          return false;
        }
      }
      return true;
    }, CHECK_EVERY_MILLIS, TIMEOUT);
  }

  private ServiceContext createServiceContext(String name)
      throws Exception {
    Service service  = createBaseDef(name);
    ServiceContext context = new MockRunningServiceContext(rule,
        service);
    context.scheduler.getDispatcher().setDrainEventsOnStop();
    context.scheduler.getDispatcher().start();
    return context;
  }

  public static Service createBaseDef(String name) {
    return createDef(name, ServiceTestUtils.createExampleApplication());
  }

  public static Service createDef(String name, Service serviceDef) {
    ApplicationId applicationId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
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

  private void writeInitialDef(Service service)
      throws IOException, SliderException {
    Path servicePath = rule.getFs().buildClusterDirPath(
        service.getName());
    ServiceApiUtil.createDirAndPersistApp(rule.getFs(), servicePath,
        service);
  }

  private void writeUpgradedDef(Service upgradedDef)
      throws IOException, SliderException {
    Path upgradePath = rule.getFs().buildClusterUpgradeDirPath(
        upgradedDef.getName(), upgradedDef.getVersion());
    ServiceApiUtil.createDirAndPersistApp(rule.getFs(), upgradePath,
        upgradedDef);
  }

  private static final int TIMEOUT = 10000;
  private static final int CHECK_EVERY_MILLIS = 100;
}