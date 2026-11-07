/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests equality of {@link ContainerSignalContext}.
 */
public class TestContainerSignalContext {
  private Container container;
  private ContainerSignalContext context;

  @BeforeEach
  public void setUp() {
    container = mock(Container.class);
    context = createContext(container, "user", "1234", Signal.KILL);
  }

  @Test
  public void testEqualsAndHashCode() {
    ContainerSignalContext other =
        createContext(container, "user", "1234", Signal.KILL);
    assertEquals(context, context);
    assertEquals(context, other);
    assertEquals(other, context);
    assertEquals(context.hashCode(), other.hashCode());
  }

  @Test
  public void testDifferentPid() {
    for (String pid : new String[] {"1235", null}) {
      ContainerSignalContext other =
          createContext(container, "user", pid, Signal.KILL);
      assertNotEquals(context, other);
      assertNotEquals(other, context);
    }
  }

  @Test
  public void testDifferentSignal() {
    for (Signal signal : new Signal[] {Signal.TERM, null}) {
      ContainerSignalContext other =
          createContext(container, "user", "1234", signal);
      assertNotEquals(context, other);
      assertNotEquals(other, context);
    }
  }

  @Test
  public void testDifferentContainer() {
    for (Container otherContainer :
        new Container[] {mock(Container.class), null}) {
      ContainerSignalContext other =
          createContext(otherContainer, "user", "1234", Signal.KILL);
      assertNotEquals(context, other);
      assertNotEquals(other, context);
    }
  }

  @Test
  public void testDifferentUser() {
    for (String user : new String[] {"other-user", null}) {
      ContainerSignalContext other =
          createContext(container, user, "1234", Signal.KILL);
      assertNotEquals(context, other);
      assertNotEquals(other, context);
    }
  }

  @Test
  public void testEqualsAndHashCodeWithNullFields() {
    ContainerSignalContext empty = new ContainerSignalContext.Builder().build();
    ContainerSignalContext other = new ContainerSignalContext.Builder().build();
    assertEquals(empty, other);
    assertEquals(other, empty);
    assertEquals(empty.hashCode(), other.hashCode());
    assertNotEquals(context, empty);
    assertNotEquals(empty, context);
  }

  @Test
  public void testEqualsWithOtherTypes() {
    assertFalse(context.equals(null));
    assertFalse(context.equals(new Object()));
  }

  private ContainerSignalContext createContext(Container targetContainer,
      String user, String pid, Signal signal) {
    return new ContainerSignalContext.Builder()
        .setContainer(targetContainer)
        .setUser(user)
        .setPid(pid)
        .setSignal(signal)
        .build();
  }
}
