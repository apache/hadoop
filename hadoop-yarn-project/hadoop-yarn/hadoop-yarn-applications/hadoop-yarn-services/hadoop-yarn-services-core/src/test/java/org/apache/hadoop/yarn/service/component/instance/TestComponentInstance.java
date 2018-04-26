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

package org.apache.hadoop.yarn.service.component.instance;

import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.component.TestComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link ComponentInstance}.
 */
public class TestComponentInstance {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testContainerUpgrade() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerUpgrade");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances()
        .iterator().next();
    ComponentInstanceEvent instanceEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(instanceEvent);
    Container containerSpec = component.getComponentSpec().getContainer(
        instance.getContainer().getId().toString());
    Assert.assertEquals("instance not upgrading",
        ContainerState.UPGRADING, containerSpec.getState());
  }

  @Test
  public void testContainerReadyAfterUpgrade() throws Exception {
    ServiceContext context = TestComponent.createTestContext(rule,
        "testContainerStarted");
    Component component = context.scheduler.getAllComponents().entrySet()
        .iterator().next().getValue();
    upgradeComponent(component);

    ComponentInstance instance = component.getAllComponentInstances()
        .iterator().next();

    ComponentInstanceEvent instanceEvent = new ComponentInstanceEvent(
        instance.getContainer().getId(), ComponentInstanceEventType.UPGRADE);
    instance.handle(instanceEvent);

    instance.handle(new ComponentInstanceEvent(instance.getContainer().getId(),
        ComponentInstanceEventType.BECOME_READY));
    Assert.assertEquals("instance not ready",
        ContainerState.READY, instance.getCompSpec().getContainer(
            instance.getContainer().getId().toString()).getState());
  }

  private void upgradeComponent(Component component) {
    component.handle(new ComponentEvent(component.getName(),
        ComponentEventType.UPGRADE)
        .setTargetSpec(component.getComponentSpec()).setUpgradeVersion("v2"));
  }
}
