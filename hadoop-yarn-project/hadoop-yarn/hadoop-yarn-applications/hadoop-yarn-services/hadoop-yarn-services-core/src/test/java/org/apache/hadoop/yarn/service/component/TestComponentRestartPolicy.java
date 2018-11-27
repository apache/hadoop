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
package org.apache.hadoop.yarn.service.component;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ComponentRestartPolicy implementations.
 */
public class TestComponentRestartPolicy {

  @Test
  public void testAlwaysRestartPolicy() throws Exception {

    AlwaysRestartPolicy alwaysRestartPolicy = AlwaysRestartPolicy.getInstance();

    Component component = mock(Component.class);
    when(component.getNumReadyInstances()).thenReturn(1);
    when(component.getNumDesiredInstances()).thenReturn(2);

    ComponentInstance instance = mock(ComponentInstance.class);
    when(instance.getComponent()).thenReturn(component);

    ContainerStatus containerStatus = mock(ContainerStatus.class);

    assertEquals(true, alwaysRestartPolicy.isLongLived());
    assertEquals(true, alwaysRestartPolicy.allowUpgrades());
    assertEquals(false, alwaysRestartPolicy.hasCompleted(component));
    assertEquals(false,
        alwaysRestartPolicy.hasCompletedSuccessfully(component));

    assertEquals(true,
        alwaysRestartPolicy.shouldRelaunchInstance(instance, containerStatus));

    assertEquals(false, alwaysRestartPolicy.isReadyForDownStream(component));
  }

  @Test
  public void testNeverRestartPolicy() throws Exception {

    NeverRestartPolicy restartPolicy = NeverRestartPolicy.getInstance();

    Component component = mock(Component.class);
    when(component.getNumSucceededInstances()).thenReturn(new Long(1));
    when(component.getNumFailedInstances()).thenReturn(new Long(2));
    when(component.getNumDesiredInstances()).thenReturn(3);

    ComponentInstance instance = mock(ComponentInstance.class);
    when(instance.getComponent()).thenReturn(component);

    ContainerStatus containerStatus = mock(ContainerStatus.class);

    assertEquals(false, restartPolicy.isLongLived());
    assertEquals(false, restartPolicy.allowUpgrades());
    assertEquals(true, restartPolicy.hasCompleted(component));
    assertEquals(false,
        restartPolicy.hasCompletedSuccessfully(component));

    assertEquals(false,
        restartPolicy.shouldRelaunchInstance(instance, containerStatus));

    assertEquals(true, restartPolicy.isReadyForDownStream(component));
  }

  @Test
  public void testOnFailureRestartPolicy() throws Exception {

    OnFailureRestartPolicy restartPolicy = OnFailureRestartPolicy.getInstance();

    Component component = mock(Component.class);
    when(component.getNumSucceededInstances()).thenReturn(new Long(3));
    when(component.getNumFailedInstances()).thenReturn(new Long(0));
    when(component.getNumDesiredInstances()).thenReturn(3);

    ComponentInstance instance = mock(ComponentInstance.class);
    when(instance.getComponent()).thenReturn(component);

    ContainerStatus containerStatus = mock(ContainerStatus.class);
    when(containerStatus.getExitStatus()).thenReturn(0);

    assertEquals(false, restartPolicy.isLongLived());
    assertEquals(false, restartPolicy.allowUpgrades());
    assertEquals(true, restartPolicy.hasCompleted(component));
    assertEquals(true,
        restartPolicy.hasCompletedSuccessfully(component));

    assertEquals(false,
        restartPolicy.shouldRelaunchInstance(instance, containerStatus));

    assertEquals(true, restartPolicy.isReadyForDownStream(component));

    when(component.getNumSucceededInstances()).thenReturn(new Long(2));
    when(component.getNumFailedInstances()).thenReturn(new Long(1));
    when(component.getNumDesiredInstances()).thenReturn(3);

    assertEquals(false, restartPolicy.hasCompleted(component));
    assertEquals(false,
        restartPolicy.hasCompletedSuccessfully(component));

    when(containerStatus.getExitStatus()).thenReturn(-1000);

    assertEquals(true,
        restartPolicy.shouldRelaunchInstance(instance, containerStatus));

    assertEquals(false, restartPolicy.isReadyForDownStream(component));

  }
}
