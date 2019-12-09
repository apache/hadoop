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

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Test the attributes of the {@link ContainerReapContext}.
 */
public class TestContainerReapContext {
  private final static String USER = "user";

  private Container container;
  private ContainerReapContext context;

  @Before
  public void setUp() {
    container = mock(Container.class);
    context = new ContainerReapContext.Builder()
        .setUser(USER)
        .setContainer(container)
        .build();
  }

  @Test
  public void getContainer() {
    assertEquals(container, context.getContainer());
  }

  @Test
  public void getUser() {
    assertEquals(USER, context.getUser());
  }
}