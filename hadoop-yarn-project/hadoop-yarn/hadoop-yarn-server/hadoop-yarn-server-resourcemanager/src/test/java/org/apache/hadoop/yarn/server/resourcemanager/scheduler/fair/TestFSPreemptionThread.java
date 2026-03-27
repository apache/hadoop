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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestFSPreemptionThread {

  @Test
  public void testPreemptableContainersConcurrentAccess() throws Exception {
    Class<?> preemptableContainersClass = Class.forName(
        FSPreemptionThread.class.getName() + "$PreemptableContainers");
    Constructor<?> constructor =
        preemptableContainersClass.getDeclaredConstructor(int.class);
    constructor.setAccessible(true);
    Object preemptableContainers = constructor.newInstance(Integer.MAX_VALUE);

    Method addContainer = preemptableContainersClass.getDeclaredMethod(
        "addContainer", RMContainer.class, ApplicationId.class);
    Method getAllContainers = preemptableContainersClass.getDeclaredMethod(
        "getAllContainers");
    addContainer.setAccessible(true);
    getAllContainers.setAccessible(true);

    int iterations = 500;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> writer = executor.submit(() -> {
        await(start);
        for (int i = 0; i < iterations; i++) {
          invokeAddContainer(addContainer, preemptableContainers, i);
        }
      });
      Future<?> reader = executor.submit(() -> {
        await(start);
        for (int i = 0; i < iterations; i++) {
          invokeGetAllContainers(getAllContainers, preemptableContainers);
        }
      });

      start.countDown();
      writer.get();
      reader.get();
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    List<RMContainer> allContainers =
        (List<RMContainer>) getAllContainers.invoke(preemptableContainers);
    assertEquals(iterations, allContainers.size());
  }

  private static void invokeAddContainer(Method addContainer,
      Object preemptableContainers, int index) {
    try {
      addContainer.invoke(preemptableContainers, createContainer(index),
          ApplicationId.newInstance(1L, index));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void invokeGetAllContainers(Method getAllContainers,
      Object preemptableContainers) {
    try {
      getAllContainers.invoke(preemptableContainers);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static RMContainer createContainer(int index) {
    RMContainer container = Mockito.mock(RMContainer.class);
    when(container.isAMContainer()).thenReturn(false);
    Resource resource = Resources.createResource(1);
    when(container.getAllocatedResource()).thenReturn(resource);
    when(container.getContainerId()).thenReturn(null);
    when(container.getNodeId()).thenReturn(null);
    return container;
  }

  private static void await(CountDownLatch start) {
    try {
      start.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
