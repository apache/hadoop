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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY;

/**
 * tests that the proxy can be interrupted
 */
public class TestRPCWaitForProxy extends TestRpcBase {
  private static final Logger
      LOG = LoggerFactory.getLogger(TestRPCWaitForProxy.class);

  private static final Configuration conf = new Configuration();

  @Before
  public void setupProtocolEngine() {
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);
  }

  /**
   * This tests that the time-bounded wait for a proxy operation works, and
   * times out.
   *
   * @throws Throwable any exception other than that which was expected
   */
  @Test(timeout = 50000)
  public void testWaitForProxy() throws Throwable {
    RpcThread worker = new RpcThread(0);
    worker.start();
    worker.join();
    Throwable caught = worker.getCaught();
    Throwable cause = caught.getCause();
    Assert.assertNotNull("No exception was raised", cause);
    if (!(cause instanceof ConnectException)) {
      throw caught;
    }
  }

  /**
   * This test sets off a blocking thread and then interrupts it, before
   * checking that the thread was interrupted
   *
   * @throws Throwable any exception other than that which was expected
   */
  @Test(timeout = 10000)
  public void testInterruptedWaitForProxy() throws Throwable {
    RpcThread worker = new RpcThread(100);
    worker.start();
    Thread.sleep(1000);
    Assert.assertTrue("worker hasn't started", worker.waitStarted);
    worker.interrupt();
    worker.join();
    Throwable caught = worker.getCaught();
    Assert.assertNotNull("No exception was raised", caught);
    // looking for the root cause here, which can be wrapped
    // as part of the NetUtils work. Having this test look
    // a the type of exception there would be brittle to improvements
    // in exception diagnostics.
    Throwable cause = caught.getCause();
    if (cause == null) {
      // no inner cause, use outer exception as root cause.
      cause = caught;
    } else if (cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (!(cause instanceof InterruptedIOException)
        && !(cause instanceof ClosedByInterruptException)) {
      throw caught;
    }
  }

  /**
   * This thread waits for a proxy for the specified timeout, and retains any
   * throwable that was raised in the process
   */

  private class RpcThread extends Thread {
    private Throwable caught;
    private int connectRetries;
    private volatile boolean waitStarted = false;

    private RpcThread(int connectRetries) {
      this.connectRetries = connectRetries;
    }
    @Override
    public void run() {
      try {
        Configuration config = new Configuration(conf);
        config.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            connectRetries);
        config.setInt(
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            connectRetries);
        waitStarted = true;

        short invalidPort = 20;
        InetSocketAddress invalidAddress = new InetSocketAddress(ADDRESS,
            invalidPort);
        TestRpcBase.TestRpcService proxy = RPC.getProxy(
            TestRpcBase.TestRpcService.class,
            1L, invalidAddress, conf);
        // Test echo method
        proxy.echo(null, newEchoRequest("hello"));

      } catch (Throwable throwable) {
        caught = throwable;
      }
    }

    public Throwable getCaught() {
      return caught;
    }
  }
}
