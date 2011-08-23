/**
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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

/**
 * Test that delayed RPCs work. Fire up three calls, the first of which should
 * be delayed. Check that the last two, which are undelayed, return before the
 * first one.
 */
public class TestDelayedRpc {
  public static RpcServer rpcServer;

  public static final int UNDELAYED = 0;
  public static final int DELAYED = 1;

  @Test
  public void testDelayedRpcImmediateReturnValue() throws Exception {
    testDelayedRpc(false);
  }

  @Test
  public void testDelayedRpcDelayedReturnValue() throws Exception {
    testDelayedRpc(true);
  }

  private void testDelayedRpc(boolean delayReturnValue) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);

    rpcServer = HBaseRPC.getServer(new TestRpcImpl(delayReturnValue),
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();

    TestRpc client = (TestRpc) HBaseRPC.getProxy(TestRpc.class, 0,
        rpcServer.getListenerAddress(), conf, 1000);

    List<Integer> results = new ArrayList<Integer>();

    TestThread th1 = new TestThread(client, true, results);
    TestThread th2 = new TestThread(client, false, results);
    TestThread th3 = new TestThread(client, false, results);
    th1.start();
    Thread.sleep(100);
    th2.start();
    Thread.sleep(200);
    th3.start();

    th1.join();
    th2.join();
    th3.join();

    assertEquals(results.get(0).intValue(), UNDELAYED);
    assertEquals(results.get(1).intValue(), UNDELAYED);
    assertEquals(results.get(2).intValue(), delayReturnValue ? DELAYED :
        0xDEADBEEF);
  }

  private static class ListAppender extends AppenderSkeleton {
    private List<String> messages = new ArrayList<String>();

    @Override
    protected void append(LoggingEvent event) {
      messages.add(event.getMessage().toString());
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    public List<String> getMessages() {
      return messages;
    }
  }

  @Test
  public void testTooManyDelayedRpcs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final int MAX_DELAYED_RPC = 10;
    conf.setInt("hbase.ipc.warn.delayedrpc.number", MAX_DELAYED_RPC);

    ListAppender listAppender = new ListAppender();
    Logger log = Logger.getLogger("org.apache.hadoop.ipc.HBaseServer");
    log.addAppender(listAppender);

    InetSocketAddress isa = new InetSocketAddress("localhost", 0);
    rpcServer = HBaseRPC.getServer(new TestRpcImpl(true),
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();
    TestRpc client = (TestRpc) HBaseRPC.getProxy(TestRpc.class, 0,
        rpcServer.getListenerAddress(), conf, 1000);

    Thread threads[] = new Thread[MAX_DELAYED_RPC + 1];

    for (int i = 0; i < MAX_DELAYED_RPC; i++) {
      threads[i] = new TestThread(client, true, null);
      threads[i].start();
    }

    /* No warnings till here. */
    assertTrue(listAppender.getMessages().isEmpty());

    /* This should give a warning. */
    threads[MAX_DELAYED_RPC] = new TestThread(client, true, null);
    threads[MAX_DELAYED_RPC].start();

    for (int i = 0; i < MAX_DELAYED_RPC; i++) {
      threads[i].join();
    }

    assertFalse(listAppender.getMessages().isEmpty());
    assertTrue(listAppender.getMessages().get(0).startsWith(
        "Too many delayed calls"));

    log.removeAppender(listAppender);
  }

  public interface TestRpc extends VersionedProtocol {
    int test(boolean delay);
  }

  private static class TestRpcImpl implements TestRpc {
    /**
     * Should the return value of delayed call be set at the end of the delay
     * or at call return.
     */
    private boolean delayReturnValue;

    /**
     * @param delayReturnValue Should the response to the delayed call be set
     * at the start or the end of the delay.
     * @param delay Amount of milliseconds to delay the call by
     */
    public TestRpcImpl(boolean delayReturnValue) {
      this.delayReturnValue = delayReturnValue;
    }

    @Override
    public int test(final boolean delay) {
      if (!delay) {
        return UNDELAYED;
      }
      final Delayable call = rpcServer.getCurrentCall();
      call.startDelay(delayReturnValue);
      new Thread() {
        public void run() {
          try {
            Thread.sleep(500);
            call.endDelay(delayReturnValue ? DELAYED : null);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }.start();
      // This value should go back to client only if the response is set
      // immediately at delay time.
      return 0xDEADBEEF;
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 0;
    }
  }

  private static class TestThread extends Thread {
    private TestRpc server;
    private boolean delay;
    private List<Integer> results;

    public TestThread(TestRpc server, boolean delay, List<Integer> results) {
      this.server = server;
      this.delay = delay;
      this.results = results;
    }

    @Override
    public void run() {
      Integer result = new Integer(server.test(delay));
      if (results != null) {
        synchronized (results) {
          results.add(result);
        }
      }
    }
  }

  @Test
  public void testEndDelayThrowing() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    InetSocketAddress isa = new InetSocketAddress("localhost", 0);

    rpcServer = HBaseRPC.getServer(new FaultyTestRpc(),
        new Class<?>[]{ TestRpcImpl.class },
        isa.getHostName(), isa.getPort(), 1, 0, true, conf, 0);
    rpcServer.start();

    TestRpc client = (TestRpc) HBaseRPC.getProxy(TestRpc.class, 0,
        rpcServer.getListenerAddress(), conf, 1000);

    int result = 0xDEADBEEF;

    try {
      result = client.test(false);
    } catch (Exception e) {
      fail("No exception should have been thrown.");
    }
    assertEquals(result, UNDELAYED);

    boolean caughtException = false;
    try {
      result = client.test(true);
    } catch(Exception e) {
      // Exception thrown by server is enclosed in a RemoteException.
      if (e.getCause().getMessage().startsWith(
          "java.lang.Exception: Something went wrong"))
        caughtException = true;
    }
    assertTrue(caughtException);
  }

  /**
   * Delayed calls to this class throw an exception.
   */
  private static class FaultyTestRpc implements TestRpc {
    @Override
    public int test(boolean delay) {
      if (!delay)
        return UNDELAYED;
      Delayable call = rpcServer.getCurrentCall();
      call.startDelay(true);
      try {
        call.endDelayThrowing(new Exception("Something went wrong"));
      } catch (IOException e) {
        e.printStackTrace();
      }
      // Client will receive the Exception, not this value.
      return DELAYED;
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 0;
    }
  }
}
