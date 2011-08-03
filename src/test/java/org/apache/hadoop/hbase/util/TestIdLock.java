/*
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestIdLock {

  private static final Log LOG = LogFactory.getLog(TestIdLock.class);

  private static final int NUM_IDS = 16;
  private static final int NUM_THREADS = 128;
  private static final int NUM_SECONDS = 20;

  private IdLock idLock = new IdLock();

  private Map<Long, String> idOwner = new ConcurrentHashMap<Long, String>();

  private class IdLockTestThread implements Callable<Boolean> {

    private String clientId;

    public IdLockTestThread(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public Boolean call() throws Exception {
      Thread.currentThread().setName(clientId);
      Random rand = new Random();
      long endTime = System.currentTimeMillis() + NUM_SECONDS * 1000;
      while (System.currentTimeMillis() < endTime) {
        long id = rand.nextInt(NUM_IDS);

        LOG.info(clientId + " is waiting for id " + id);
        IdLock.Entry lockEntry = idLock.getLockEntry(id);
        try {
          int sleepMs = 1 + rand.nextInt(4);
          String owner = idOwner.get(id);
          if (owner != null) {
            LOG.error("Id " + id + " already taken by " + owner + ", "
                + clientId + " failed");
            return false;
          }

          idOwner.put(id, clientId);
          LOG.info(clientId + " took id " + id + ", sleeping for " +
              sleepMs + "ms");
          Thread.sleep(sleepMs);
          LOG.info(clientId + " is releasing id " + id);
          idOwner.remove(id);

        } finally {
          idLock.releaseLockEntry(lockEntry);
        }
      }
      return true;
    }

  }

  @Test
  public void testMultipleClients() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      ExecutorCompletionService<Boolean> ecs =
          new ExecutorCompletionService<Boolean>(exec);
      for (int i = 0; i < NUM_THREADS; ++i)
        ecs.submit(new IdLockTestThread("client_" + i));
      for (int i = 0; i < NUM_THREADS; ++i) {
        Future<Boolean> result = ecs.take();
        assertTrue(result.get());
      }
      idLock.assertMapEmpty();
    } finally {
      exec.shutdown();
    }
  }

}
