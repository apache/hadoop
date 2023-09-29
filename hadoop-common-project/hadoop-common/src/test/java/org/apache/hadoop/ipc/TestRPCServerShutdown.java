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

package org.apache.hadoop.ipc;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Split from TestRPC. */
@SuppressWarnings("deprecation")
public class TestRPCServerShutdown extends TestRpcBase {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestRPCServerShutdown.class);

  @Before
  public void setup() {
    setupConf();
  }

  /**
   *  Verify the RPC server can shutdown properly when callQueue is full.
   */
  @Test (timeout=30000)
  public void testRPCServerShutdown() throws Exception {
    final int numClients = 3;
    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    RPC.Builder builder = newServerBuilder(conf)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true);
    final Server server = setupTestServer(builder);

    final TestRpcService proxy = getClient(addr, conf);
    try {
      // start a sleep RPC call to consume the only handler thread.
      // Start another sleep RPC call to make callQueue full.
      // Start another sleep RPC call to make reader thread block on CallQueue.
      for (int i = 0; i < numClients; i++) {
        res.add(executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws ServiceException, InterruptedException {
                proxy.sleep(null, newSleepRequest(100000));
                return null;
              }
            }));
      }
      while (server.getCallQueueLen() != 1
          || countThreads(CallQueueManager.class.getName()) != 1
          || countThreads(PBServerImpl.class.getName()) != 1) {
        Thread.sleep(100);
      }
    } finally {
      try {
        stop(server, proxy);
        assertEquals("Not enough clients", numClients, res.size());
        for (Future<Void> f : res) {
          try {
            f.get();
            fail("Future get should not return");
          } catch (ExecutionException e) {
            ServiceException se = (ServiceException) e.getCause();
            assertTrue("Unexpected exception: " + se,
                se.getCause() instanceof IOException);
            LOG.info("Expected exception", e.getCause());
          }
        }
      } finally {
        executorService.shutdown();
      }
    }
  }
}
