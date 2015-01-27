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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestAppLevelServiceManager {

  @Test(timeout=60000)
  public void testMultithreadedAdd() throws Exception {
    final AppLevelServiceManager serviceManager =
        spy(new AppLevelServiceManager());
    doReturn(new Configuration()).when(serviceManager).getConfig();

    final int NUM_APPS = 5;
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < NUM_APPS; i++) {
      final String appId = String.valueOf(i);
      Callable<Boolean> task = new Callable<Boolean>() {
        public Boolean call() {
          return serviceManager.addService(appId);
        }
      };
      tasks.add(task);
    }
    ExecutorService executor = Executors.newFixedThreadPool(NUM_APPS);
    try {
      List<Future<Boolean>> futures = executor.invokeAll(tasks);
      for (Future<Boolean> future: futures) {
        assertTrue(future.get());
      }
    } finally {
      executor.shutdownNow();
    }
    // check the keys
    for (int i = 0; i < NUM_APPS; i++) {
      assertTrue(serviceManager.hasService(String.valueOf(i)));
    }
  }

  @Test
  public void testMultithreadedAddAndRemove() throws Exception {
    final AppLevelServiceManager serviceManager =
        spy(new AppLevelServiceManager());
    doReturn(new Configuration()).when(serviceManager).getConfig();

    final int NUM_APPS = 5;
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < NUM_APPS; i++) {
      final String appId = String.valueOf(i);
      Callable<Boolean> task = new Callable<Boolean>() {
        public Boolean call() {
          return serviceManager.addService(appId) &&
              serviceManager.removeService(appId);
        }
      };
      tasks.add(task);
    }
    ExecutorService executor = Executors.newFixedThreadPool(NUM_APPS);
    try {
      List<Future<Boolean>> futures = executor.invokeAll(tasks);
      for (Future<Boolean> future: futures) {
        assertTrue(future.get());
      }
    } finally {
      executor.shutdownNow();
    }
    // check the keys
    for (int i = 0; i < NUM_APPS; i++) {
      assertFalse(serviceManager.hasService(String.valueOf(i)));
    }
  }
}
