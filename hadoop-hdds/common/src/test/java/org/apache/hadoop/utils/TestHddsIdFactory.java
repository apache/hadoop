/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.junit.After;
import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the JMX interface for the rocksdb metastore implementation.
 */
public class TestHddsIdFactory {

  private static final Set<Long> ID_SET = ConcurrentHashMap.newKeySet();
  private static final int IDS_PER_THREAD = 10000;
  private static final int NUM_OF_THREADS = 5;

  @After
  public void cleanup() {
    ID_SET.clear();
  }

  @Test
  public void testGetLongId() throws Exception {

    ExecutorService executor = Executors.newFixedThreadPool(5);
    List<Callable<Integer>> tasks = new ArrayList<>(5);
    addTasks(tasks);
    List<Future<Integer>> result = executor.invokeAll(tasks);
    assertEquals(IDS_PER_THREAD * NUM_OF_THREADS, ID_SET.size());
    for (Future<Integer> r : result) {
      assertEquals(IDS_PER_THREAD, r.get().intValue());
    }
  }

  private void addTasks(List<Callable<Integer>> tasks) {
    for (int i = 0; i < NUM_OF_THREADS; i++) {
      Callable<Integer> task = () -> {
        for (int idNum = 0; idNum < IDS_PER_THREAD; idNum++) {
          long var = HddsIdFactory.getLongId();
          if (ID_SET.contains(var)) {
            Assert.fail("Duplicate id found");
          }
          ID_SET.add(var);
        }
        return IDS_PER_THREAD;
      };
      tasks.add(task);
    }
  }
}