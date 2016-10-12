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

import com.google.common.base.Supplier;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.test.MockitoUtil;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** Unit tests for RPC. */
@SuppressWarnings("deprecation")
public class TestRPC extends TestRpcBase {

  public static final Log LOG = LogFactory.getLog(TestRPC.class);

  @Before
  public void setup() {
    setupConf();
  }

  int datasize = 1024*100;
  int numThreads = 50;

  public interface TestProtocol extends VersionedProtocol {
    long versionID = 1L;

    void ping() throws IOException;
    void sleep(long delay) throws IOException, InterruptedException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
  }

  public static class TestImpl implements TestProtocol {
    int fastPingCounter = 0;
    
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int hashcode) {
      return new ProtocolSignature(TestProtocol.versionID, null);
    }

    @Override
    public void ping() {}
    
    @Override
    public void sleep(long delay) throws InterruptedException {
      Thread.sleep(delay);
    }

    @Override
    public String echo(String value) throws IOException { return value; }

    @Override
    public String[] echo(String[] values) throws IOException { return values; }

    @Override
    public Writable echo(Writable writable) {
      return writable;
    }
    @Override
    public int add(int v1, int v2) {
      return v1 + v2;
    }

    @Override
    public int add(int[] values) {
      int sum = 0;
      for (int i = 0; i < values.length; i++) {
        sum += values[i];
      }
      return sum;
    }

    @Override
    public int error() throws IOException {
      throw new IOException("bobo");
    }
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    int datasize;
    TestRpcService proxy;

    Transactions(TestRpcService proxy, int datasize) {
      this.proxy = proxy;
      this.datasize = datasize;
    }

    // do two RPC that transfers data.
    @Override
    public void run() {
      Integer[] indata = new Integer[datasize];
      Arrays.fill(indata, 123);
      TestProtos.ExchangeRequestProto exchangeRequest =
          TestProtos.ExchangeRequestProto.newBuilder().addAllValues(
              Arrays.asList(indata)).build();
      Integer[] outdata = null;
      TestProtos.ExchangeResponseProto exchangeResponse;

      TestProtos.AddRequestProto addRequest =
          TestProtos.AddRequestProto.newBuilder().setParam1(1)
              .setParam2(2).build();
      TestProtos.AddResponseProto addResponse;

      int val = 0;
      try {
        exchangeResponse = proxy.exchange(null, exchangeRequest);
        outdata = new Integer[exchangeResponse.getValuesCount()];
        outdata = exchangeResponse.getValuesList().toArray(outdata);
        addResponse = proxy.add(null, addRequest);
        val = addResponse.getResult();
      } catch (ServiceException e) {
        assertTrue("Exception from RPC exchange() "  + e, false);
      }
      assertEquals(indata.length, outdata.length);
      assertEquals(3, val);
      for (int i = 0; i < outdata.length; i++) {
        assertEquals(outdata[i].intValue(), i);
      }
    }
  }

  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestRpcService proxy;
    private volatile boolean done;

    SlowRPC(TestRpcService proxy) {
      this.proxy = proxy;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    @Override
    public void run() {
      try {
        // this would hang until two fast pings happened
        ping(true);
        done = true;
      } catch (ServiceException e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }

    void ping(boolean shouldSlow) throws ServiceException {
      // this would hang until two fast pings happened
      proxy.slowPing(null, newSlowPingRequest(shouldSlow));
    }
  }

  /**
   * A basic interface for testing client-side RPC resource cleanup.
   */
  private interface StoppedProtocol {
    long versionID = 0;

    void stop();
  }

  /**
   * A class used for testing cleanup of client side RPC resources.
   */
  private static class StoppedRpcEngine implements RpcEngine {

    @Override
    public <T> ProtocolProxy<T> getProxy(
        Class<T> protocol, long clientVersion, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout,
        RetryPolicy connectionRetryPolicy) throws IOException {
      return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
          rpcTimeout, connectionRetryPolicy, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ProtocolProxy<T> getProxy(
        Class<T> protocol, long clientVersion, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth) throws IOException {
      T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
          new Class[] { protocol }, new StoppedInvocationHandler());
      return new ProtocolProxy<T>(protocol, proxy, false);
    }

    @Override
    public org.apache.hadoop.ipc.RPC.Server getServer(
        Class<?> protocol, Object instance, String bindAddress, int port,
        int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig) throws IOException {
      return null;
    }

    @Override
    public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
        ConnectionId connId, Configuration conf, SocketFactory factory)
        throws IOException {
      throw new UnsupportedOperationException("This proxy is not supported");
    }
  }

  /**
   * An invocation handler which does nothing when invoking methods, and just
   * counts the number of times close() is called.
   */
  private static class StoppedInvocationHandler
      implements InvocationHandler, Closeable {

    private int closeCalled = 0;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      return null;
    }

    @Override
    public void close() throws IOException {
      closeCalled++;
    }

    public int getCloseCalled() {
      return closeCalled;
    }

  }

  @Test
  public void testConfRpc() throws IOException {
    Server server = newServerBuilder(conf)
        .setNumHandlers(1).setVerbose(false).build();

    // Just one handler
    int confQ = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    assertEquals(confQ, server.getMaxQueueSize());

    int confReaders = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    assertEquals(confReaders, server.getNumReaders());

    server = newServerBuilder(conf)
        .setNumHandlers(1).setnumReaders(3).setQueueSizePerHandler(200)
        .setVerbose(false).build();

    assertEquals(3, server.getNumReaders());
    assertEquals(200, server.getMaxQueueSize());

    server = newServerBuilder(conf).setQueueSizePerHandler(10)
        .setNumHandlers(2).setVerbose(false).build();
    assertEquals(2 * 10, server.getMaxQueueSize());
  }

  @Test
  public void testProxyAddress() throws Exception {
    Server server = null;
    TestRpcService proxy = null;

    try {
      server = setupTestServer(conf, -1);
      // create a client
      proxy = getClient(addr, conf);
      assertEquals(addr, RPC.getServerAddress(proxy));
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testSlowRpc() throws IOException, ServiceException {
    Server server;
    TestRpcService proxy = null;

    System.out.println("Testing Slow RPC");
    // create a server with two handlers
    server = setupTestServer(conf, 2);

    try {
      // create a client
      proxy = getClient(addr, conf);

      SlowRPC slowrpc = new SlowRPC(proxy);
      Thread thread = new Thread(slowrpc, "SlowRPC");
      thread.start(); // send a slow RPC, which won't return until two fast pings
      assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

      slowrpc.ping(false); // first fast ping

      // verify that the first RPC is still stuck
      assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());

      slowrpc.ping(false); // second fast ping

      // Now the slow ping should be able to be executed
      while (!slowrpc.isDone()) {
        System.out.println("Waiting for slow RPC to get done.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
      }
    } finally {
      System.out.println("Down slow rpc testing");
      stop(server, proxy);
    }
  }

  @Test
  public void testCalls() throws Exception {
    testCallsInternal(conf);
  }

  private void testCallsInternal(Configuration myConf) throws Exception {
    Server server;
    TestRpcService proxy = null;

    server = setupTestServer(myConf, -1);
    try {
      proxy = getClient(addr, myConf);

      proxy.ping(null, newEmptyRequest());

      TestProtos.EchoResponseProto echoResp = proxy.echo(null,
          newEchoRequest("foo"));
      assertEquals(echoResp.getMessage(), "foo");

      echoResp = proxy.echo(null, newEchoRequest(""));
      assertEquals(echoResp.getMessage(), "");

      // Check rpcMetrics
      MetricsRecordBuilder rb = getMetrics(server.rpcMetrics.name());
      assertCounter("RpcProcessingTimeNumOps", 3L, rb);
      assertCounterGt("SentBytes", 0L, rb);
      assertCounterGt("ReceivedBytes", 0L, rb);

      // Number of calls to echo method should be 2
      rb = getMetrics(server.rpcDetailedMetrics.name());
      assertCounter("EchoNumOps", 2L, rb);

      // Number of calls to ping method should be 1
      assertCounter("PingNumOps", 1L, rb);

      String[] strings = new String[] {"foo","bar"};
      TestProtos.EchoRequestProto2 echoRequest2 =
          TestProtos.EchoRequestProto2.newBuilder().addAllMessage(
              Arrays.asList(strings)).build();
      TestProtos.EchoResponseProto2 echoResponse2 =
          proxy.echo2(null, echoRequest2);
      assertTrue(Arrays.equals(echoResponse2.getMessageList().toArray(),
          strings));

      echoRequest2 = TestProtos.EchoRequestProto2.newBuilder()
          .addAllMessage(Collections.<String>emptyList()).build();
      echoResponse2 = proxy.echo2(null, echoRequest2);
      assertTrue(Arrays.equals(echoResponse2.getMessageList().toArray(),
          new String[]{}));

      TestProtos.AddRequestProto addRequest =
          TestProtos.AddRequestProto.newBuilder().setParam1(1)
              .setParam2(2).build();
      TestProtos.AddResponseProto addResponse =
          proxy.add(null, addRequest);
      assertEquals(addResponse.getResult(), 3);

      Integer[] integers = new Integer[] {1, 2};
      TestProtos.AddRequestProto2 addRequest2 =
          TestProtos.AddRequestProto2.newBuilder().addAllParams(
              Arrays.asList(integers)).build();
      addResponse = proxy.add2(null, addRequest2);
      assertEquals(addResponse.getResult(), 3);

      boolean caught = false;
      try {
        proxy.error(null, newEmptyRequest());
      } catch (ServiceException e) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Caught " + e);
        }
        caught = true;
      }
      assertTrue(caught);
      rb = getMetrics(server.rpcDetailedMetrics.name());
      assertCounter("RpcServerExceptionNumOps", 1L, rb);

      //proxy.testServerGet();

      // create multiple threads and make them do large data transfers
      System.out.println("Starting multi-threaded RPC test...");
      server.setSocketSendBufSize(1024);
      Thread threadId[] = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        Transactions trans = new Transactions(proxy, datasize);
        threadId[i] = new Thread(trans, "TransactionThread-" + i);
        threadId[i].start();
      }

      // wait for all transactions to get over
      System.out.println("Waiting for all threads to finish RPCs...");
      for (int i = 0; i < numThreads; i++) {
        try {
          threadId[i].join();
        } catch (InterruptedException e) {
          i--;      // retry
        }
      }
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testClientWithoutServer() throws Exception {
    TestRpcService proxy;

    short invalidPort = 20;
    InetSocketAddress invalidAddress = new InetSocketAddress(ADDRESS,
        invalidPort);
    long invalidClientVersion = 1L;
    try {
      proxy = RPC.getProxy(TestRpcService.class,
          invalidClientVersion, invalidAddress, conf);
      // Test echo method
      proxy.echo(null, newEchoRequest("hello"));
      fail("We should not have reached here");
    } catch (ServiceException ioe) {
      //this is what we expected
      if (!(ioe.getCause() instanceof ConnectException)) {
        fail("We should not have reached here");
      }
    }
  }

  private static final String ACL_CONFIG = "test.protocol.acl";

  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestRpcService.class) };
    }
  }

  private void doRPCs(Configuration myConf, boolean expectFailure) throws Exception {
    Server server;
    TestRpcService proxy = null;

    server = setupTestServer(myConf, 5);

    server.refreshServiceAcl(myConf, new TestPolicyProvider());

    TestProtos.EmptyRequestProto emptyRequestProto =
        TestProtos.EmptyRequestProto.newBuilder().build();

    try {
      proxy = getClient(addr, conf);
      proxy.ping(null, emptyRequestProto);
      if (expectFailure) {
        fail("Expect RPC.getProxy to fail with AuthorizationException!");
      }
    } catch (ServiceException e) {
      if (expectFailure) {
        RemoteException re = (RemoteException) e.getCause();
        assertTrue(re.unwrapRemoteException() instanceof AuthorizationException);
        assertEquals("RPC error code should be UNAUTHORIZED",
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, re.getErrorCode());
      } else {
        throw e;
      }
    } finally {
      MetricsRecordBuilder rb = getMetrics(server.rpcMetrics.name());
      if (expectFailure) {
        assertCounter("RpcAuthorizationFailures", 1L, rb);
      } else {
        assertCounter("RpcAuthorizationSuccesses", 1L, rb);
      }
      //since we don't have authentication turned ON, we should see 
      // 0 for the authentication successes and 0 for failure
      assertCounter("RpcAuthenticationFailures", 0L, rb);
      assertCounter("RpcAuthenticationSuccesses", 0L, rb);

      stop(server, proxy);
    }
  }

  @Test
  public void testServerAddress() throws IOException {
    Server server;

    server = setupTestServer(conf, 5);
    try {
      InetSocketAddress bindAddr = NetUtils.getConnectAddress(server);
      assertEquals(InetAddress.getLocalHost(), bindAddr.getAddress());
    } finally {
      stop(server, null);
    }
  }

  @Test
  public void testAuthorization() throws Exception {
    Configuration myConf = new Configuration();
    myConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);

    // Expect to succeed
    myConf.set(ACL_CONFIG, "*");
    doRPCs(myConf, false);

    // Reset authorization to expect failure
    myConf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(myConf, true);

    myConf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    // Expect to succeed
    myConf.set(ACL_CONFIG, "*");
    doRPCs(myConf, false);

    // Reset authorization to expect failure
    myConf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(myConf, true);
  }

  /**
   * Switch off setting socketTimeout values on RPC sockets.
   * Verify that RPC calls still work ok.
   */
  public void testNoPings() throws Exception {
    Configuration conf = new Configuration();

    conf.setBoolean("ipc.client.ping", false);
    new TestRPC().testCallsInternal(conf);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    new TestRPC().testCallsInternal(conf);
  }

  /**
   * Test stopping a non-registered proxy
   * @throws IOException
   */
  @Test(expected=HadoopIllegalArgumentException.class)
  public void testStopNonRegisteredProxy() throws IOException {
    RPC.stopProxy(null);
  }

  /**
   * Test that the mockProtocol helper returns mock proxies that can
   * be stopped without error.
   */
  @Test
  public void testStopMockObject() throws IOException {
    RPC.stopProxy(MockitoUtil.mockProtocol(TestProtocol.class));
  }

  @Test
  public void testStopProxy() throws IOException {
    RPC.setProtocolEngine(conf,
        StoppedProtocol.class, StoppedRpcEngine.class);

    StoppedProtocol proxy = RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(proxy);
    assertEquals(0, invocationHandler.getCloseCalled());
    RPC.stopProxy(proxy);
    assertEquals(1, invocationHandler.getCloseCalled());
  }

  @Test
  public void testWrappedStopProxy() throws IOException {
    StoppedProtocol wrappedProxy = RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(wrappedProxy);

    StoppedProtocol proxy = (StoppedProtocol) RetryProxy.create(
        StoppedProtocol.class, wrappedProxy, RetryPolicies.RETRY_FOREVER);

    assertEquals(0, invocationHandler.getCloseCalled());
    RPC.stopProxy(proxy);
    assertEquals(1, invocationHandler.getCloseCalled());
  }

  @Test
  public void testErrorMsgForInsecureClient() throws IOException {
    Server server;
    TestRpcService proxy = null;

    Configuration serverConf = new Configuration(conf);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS,
        serverConf);
    UserGroupInformation.setConfiguration(serverConf);

    server = setupTestServer(serverConf, 5);

    boolean succeeded = false;

    try {
      UserGroupInformation.setConfiguration(conf);
      proxy = getClient(addr, conf);
      proxy.echo(null, newEchoRequest(""));
    } catch (ServiceException e) {
      assertTrue(e.getCause() instanceof RemoteException);
      RemoteException re = (RemoteException) e.getCause();
      LOG.info("LOGGING MESSAGE: " + re.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED",
          RpcErrorCodeProto.FATAL_UNAUTHORIZED, re.getErrorCode());
      assertTrue(re.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      stop(server, proxy);
    }
    assertTrue(succeeded);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);

    UserGroupInformation.setConfiguration(serverConf);
    server = setupTestServer(serverConf, 5);
    succeeded = false;
    proxy = null;
    try {
      UserGroupInformation.setConfiguration(conf);
      proxy = getClient(addr, conf);
      proxy.echo(null, newEchoRequest(""));
    } catch (ServiceException e) {
      RemoteException re = (RemoteException) e.getCause();
      LOG.info("LOGGING MESSAGE: " + re.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED",
          RpcErrorCodeProto.FATAL_UNAUTHORIZED, re.getErrorCode());
      assertTrue(re.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      stop(server, proxy);
    }
    assertTrue(succeeded);
  }

  /**
   * Test that server.stop() properly stops all threads
   */
  @Test
  public void testStopsAllThreads() throws IOException, InterruptedException {
    Server server;

    int threadsBefore = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads running before test",
        0, threadsBefore);

    server = setupTestServer(conf, 5);

    try {
      // Wait for at least one reader thread to start
      int threadsRunning = 0;
      long totalSleepTime = 0;
      do {
        totalSleepTime += 10;
        Thread.sleep(10);
        threadsRunning = countThreads("Server$Listener$Reader");
      } while (threadsRunning == 0 && totalSleepTime < 5000);

      // Validate that at least one thread started (we didn't timeout)
      threadsRunning = countThreads("Server$Listener$Reader");
      assertTrue(threadsRunning > 0);
    } finally {
      server.stop();
    }

    int threadsAfter = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads left running after test",
        0, threadsAfter);
  }

  @Test
  public void testRPCBuilder() throws IOException {
    // Test mandatory field conf
    try {
      new RPC.Builder(null).setProtocol(TestProtocol.class)
          .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
          .setNumHandlers(5).setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
    // Test mandatory field protocol
    try {
      new RPC.Builder(conf).setInstance(new TestImpl()).setBindAddress(ADDRESS)
          .setPort(0).setNumHandlers(5).setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
    // Test mandatory field instance
    try {
      new RPC.Builder(conf).setProtocol(TestProtocol.class)
          .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5)
          .setVerbose(true).build();
      fail("Didn't throw HadoopIllegalArgumentException");
    } catch (Exception e) {
      if (!(e instanceof HadoopIllegalArgumentException)) {
        fail("Expecting HadoopIllegalArgumentException but caught " + e);
      }
    }
  }

  @Test(timeout=90000)
  public void testRPCInterruptedSimple() throws Exception {
    Server server;
    TestRpcService proxy = null;

    RPC.Builder builder = newServerBuilder(conf)
        .setNumHandlers(5).setVerbose(true)
        .setSecretManager(null);

    server = setupTestServer(builder);

    try {
      proxy = getClient(addr, conf);
      // Connect to the server

      proxy.ping(null, newEmptyRequest());
      // Interrupt self, try another call
      Thread.currentThread().interrupt();
      try {
        proxy.ping(null, newEmptyRequest());
        fail("Interruption did not cause IPC to fail");
      } catch (ServiceException se) {
        if (se.toString().contains("InterruptedException") ||
            se.getCause() instanceof InterruptedIOException) {
          // clear interrupt status for future tests
          Thread.interrupted();
          return;
        }
        throw se;
      }
    } finally {
      stop(server, proxy);
    }
  }

  @Test(timeout=30000)
  public void testRPCInterrupted() throws Exception {
    Server server;

    RPC.Builder builder = newServerBuilder(conf)
        .setNumHandlers(5).setVerbose(true)
        .setSecretManager(null);
    server = setupTestServer(builder);

    int numConcurrentRPC = 200;
    final CyclicBarrier barrier = new CyclicBarrier(numConcurrentRPC);
    final CountDownLatch latch = new CountDownLatch(numConcurrentRPC);
    final AtomicBoolean leaderRunning = new AtomicBoolean(true);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    Thread leaderThread = null;

    try {
      for (int i = 0; i < numConcurrentRPC; i++) {
        final int num = i;
        final TestRpcService proxy = getClient(addr, conf);
        Thread rpcThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
              while (num == 0 || leaderRunning.get()) {
                proxy.slowPing(null, newSlowPingRequest(false));
              }

              proxy.slowPing(null, newSlowPingRequest(false));
            } catch (Exception e) {
              if (num == 0) {
                leaderRunning.set(false);
              } else {
                error.set(e);
              }

              LOG.error("thread " + num, e);
            } finally {
              latch.countDown();
            }
          }
        });
        rpcThread.start();

        if (leaderThread == null) {
          leaderThread = rpcThread;
        }
      }
      // let threads get past the barrier
      Thread.sleep(1000);
      // stop a single thread
      while (leaderRunning.get()) {
        leaderThread.interrupt();
      }

      latch.await();

      // should not cause any other thread to get an error
      assertTrue("rpc got exception " + error.get(), error.get() == null);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testConnectionPing() throws Exception {
    Server server;
    TestRpcService proxy = null;

    int pingInterval = 50;
    conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
    server = setupTestServer(conf, 5);

    try {
      proxy = getClient(addr, conf);

      proxy.sleep(null, newSleepRequest(pingInterval * 4));
    } finally {
      stop(server, proxy);
    }
  }

  @Test(timeout=30000)
  public void testExternalCall() throws Exception {
    final UserGroupInformation ugi = UserGroupInformation
        .createUserForTesting("user123", new String[0]);
    final IOException expectedIOE = new IOException("boom");

    // use 1 handler so the callq can be plugged
    final Server server = setupTestServer(conf, 1);
    try {
      final AtomicBoolean result = new AtomicBoolean();

      ExternalCall<String> remoteUserCall = newExtCall(ugi,
          new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
              return UserGroupInformation.getCurrentUser().getUserName();
            }
          });

      ExternalCall<String> exceptionCall = newExtCall(ugi,
          new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
              throw expectedIOE;
            }
          });

      final CountDownLatch latch = new CountDownLatch(1);
      final CyclicBarrier barrier = new CyclicBarrier(2);

      ExternalCall<Void> barrierCall = newExtCall(ugi,
          new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              // notify we are in a handler and then wait to keep the callq
              // plugged up
              latch.countDown();
              barrier.await();
              return null;
            }
          });

      server.queueCall(barrierCall);
      server.queueCall(exceptionCall);
      server.queueCall(remoteUserCall);

      // wait for barrier call to enter the handler, check that the other 2
      // calls are actually queued
      latch.await();
      assertEquals(2, server.getCallQueueLen());

      // unplug the callq
      barrier.await();
      barrierCall.get();

      // verify correct ugi is used
      String answer = remoteUserCall.get();
      assertEquals(ugi.getUserName(), answer);

      try {
        exceptionCall.get();
        fail("didn't throw");
      } catch (ExecutionException ee) {
        assertTrue((ee.getCause()) instanceof IOException);
        assertEquals(expectedIOE.getMessage(), ee.getCause().getMessage());
      }
    } finally {
      server.stop();
    }
  }

  private <T> ExternalCall<T> newExtCall(UserGroupInformation ugi,
      PrivilegedExceptionAction<T> callable) {
    return new ExternalCall<T>(callable) {
      @Override
      public String getProtocol() {
        return "test";
      }
      @Override
      public UserGroupInformation getRemoteUser() {
        return ugi;
      }
    };
  }

  @Test
  public void testRpcMetrics() throws Exception {
    Server server;
    TestRpcService proxy = null;

    final int interval = 1;
    conf.setBoolean(CommonConfigurationKeys.
        RPC_METRICS_QUANTILE_ENABLE, true);
    conf.set(CommonConfigurationKeys.
        RPC_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);

    server = setupTestServer(conf, 5);

    try {
      proxy = getClient(addr, conf);

      for (int i = 0; i < 1000; i++) {
        proxy.ping(null, newEmptyRequest());

        proxy.echo(null, newEchoRequest("" + i));
      }
      MetricsRecordBuilder rpcMetrics =
          getMetrics(server.getRpcMetrics().name());
      assertTrue("Expected non-zero rpc queue time",
          getLongCounter("RpcQueueTimeNumOps", rpcMetrics) > 0);
      assertTrue("Expected non-zero rpc processing time",
          getLongCounter("RpcProcessingTimeNumOps", rpcMetrics) > 0);
      MetricsAsserts.assertQuantileGauges("RpcQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcProcessingTime" + interval + "s",
          rpcMetrics);
    } finally {
      stop(server, proxy);
    }
  }

  /**
   *  Test RPC backoff by queue full.
   */
  @Test (timeout=30000)
  public void testClientBackOff() throws Exception {
    Server server;
    final TestRpcService proxy;

    boolean succeeded = false;
    final int numClients = 2;
    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    conf.setBoolean(CommonConfigurationKeys.IPC_NAMESPACE +
        ".0." + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);
    RPC.Builder builder = newServerBuilder(conf)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true);
    server = setupTestServer(builder);

    @SuppressWarnings("unchecked")
    CallQueueManager<Call> spy = spy((CallQueueManager<Call>) Whitebox
        .getInternalState(server, "callQueue"));
    Whitebox.setInternalState(server, "callQueue", spy);

    Exception lastException = null;
    proxy = getClient(addr, conf);
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
        verify(spy, timeout(500).times(i + 1)).offer(Mockito.<Call>anyObject());
      }
      try {
        proxy.sleep(null, newSleepRequest(100));
      } catch (ServiceException e) {
        RemoteException re = (RemoteException) e.getCause();
        IOException unwrapExeption = re.unwrapRemoteException();
        if (unwrapExeption instanceof RetriableException) {
          succeeded = true;
        } else {
          lastException = unwrapExeption;
        }
      }
    } finally {
      executorService.shutdown();
      stop(server, proxy);
    }
    if (lastException != null) {
      LOG.error("Last received non-RetriableException:", lastException);
    }
    assertTrue("RetriableException not received", succeeded);
  }

  /**
   *  Test RPC backoff by response time of each priority level.
   */
  @Test (timeout=30000)
  public void testClientBackOffByResponseTime() throws Exception {
    final TestRpcService proxy;
    boolean succeeded = false;
    final int numClients = 1;

    GenericTestUtils.setLogLevel(DecayRpcScheduler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RPC.LOG, Level.DEBUG);

    final List<Future<Void>> res = new ArrayList<Future<Void>>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);

    final String ns = CommonConfigurationKeys.IPC_NAMESPACE + ".0";
    Server server = setupDecayRpcSchedulerandTestServer(ns + ".");

    @SuppressWarnings("unchecked")
    CallQueueManager<Call> spy = spy((CallQueueManager<Call>) Whitebox
        .getInternalState(server, "callQueue"));
    Whitebox.setInternalState(server, "callQueue", spy);

    Exception lastException = null;
    proxy = getClient(addr, conf);

    MetricsRecordBuilder rb1 =
        getMetrics("DecayRpcSchedulerMetrics2." + ns);
    final long beginDecayedCallVolume = MetricsAsserts.getLongCounter(
        "DecayedCallVolume", rb1);
    final long beginRawCallVolume = MetricsAsserts.getLongCounter(
        "CallVolume", rb1);
    final int beginUniqueCaller = MetricsAsserts.getIntCounter("UniqueCallers",
        rb1);

    try {
      // start a sleep RPC call that sleeps 3s.
      for (int i = 0; i < numClients; i++) {
        res.add(executorService.submit(
            new Callable<Void>() {
              @Override
              public Void call() throws ServiceException, InterruptedException {
                proxy.sleep(null, newSleepRequest(3000));
                return null;
              }
            }));
        verify(spy, timeout(500).times(i + 1)).offer(Mockito.<Call>anyObject());
      }
      // Start another sleep RPC call and verify the call is backed off due to
      // avg response time(3s) exceeds threshold (2s).
      try {
        // wait for the 1st response time update
        Thread.sleep(5500);
        proxy.sleep(null, newSleepRequest(100));
      } catch (ServiceException e) {
        RemoteException re = (RemoteException) e.getCause();
        IOException unwrapExeption = re.unwrapRemoteException();
        if (unwrapExeption instanceof RetriableException) {
          succeeded = true;
        } else {
          lastException = unwrapExeption;
        }

        // Lets Metric system update latest metrics
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            MetricsRecordBuilder rb2 =
              getMetrics("DecayRpcSchedulerMetrics2." + ns);
            long decayedCallVolume1 = MetricsAsserts.getLongCounter(
                "DecayedCallVolume", rb2);
            long rawCallVolume1 = MetricsAsserts.getLongCounter(
                "CallVolume", rb2);
            int uniqueCaller1 = MetricsAsserts.getIntCounter(
                "UniqueCallers", rb2);
            long callVolumePriority0 = MetricsAsserts.getLongGauge(
                "Priority.0.CompletedCallVolume", rb2);
            long callVolumePriority1 = MetricsAsserts.getLongGauge(
                "Priority.1.CompletedCallVolume", rb2);
            double avgRespTimePriority0 = MetricsAsserts.getDoubleGauge(
                "Priority.0.AvgResponseTime", rb2);
            double avgRespTimePriority1 = MetricsAsserts.getDoubleGauge(
                "Priority.1.AvgResponseTime", rb2);

            LOG.info("DecayedCallVolume: " + decayedCallVolume1);
            LOG.info("CallVolume: " + rawCallVolume1);
            LOG.info("UniqueCaller: " + uniqueCaller1);
            LOG.info("Priority.0.CompletedCallVolume: " + callVolumePriority0);
            LOG.info("Priority.1.CompletedCallVolume: " + callVolumePriority1);
            LOG.info("Priority.0.AvgResponseTime: " + avgRespTimePriority0);
            LOG.info("Priority.1.AvgResponseTime: " + avgRespTimePriority1);

            return decayedCallVolume1 > beginDecayedCallVolume &&
                rawCallVolume1 > beginRawCallVolume &&
                uniqueCaller1 > beginUniqueCaller;
          }
        }, 30, 60000);
      }
    } finally {
      executorService.shutdown();
      stop(server, proxy);
    }
    if (lastException != null) {
      LOG.error("Last received non-RetriableException:", lastException);
    }
    assertTrue("RetriableException not received", succeeded);
  }

  private Server setupDecayRpcSchedulerandTestServer(String ns)
      throws Exception {
    final int queueSizePerHandler = 3;

    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    conf.setBoolean(ns + CommonConfigurationKeys.IPC_BACKOFF_ENABLE, true);
    conf.setStrings(ns + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY,
        "org.apache.hadoop.ipc.FairCallQueue");
    conf.setStrings(ns + CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY,
        "org.apache.hadoop.ipc.DecayRpcScheduler");
    conf.setInt(ns + CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY,
        2);
    conf.setBoolean(ns +
            DecayRpcScheduler.IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_KEY,
        true);
    // set a small thresholds 2s and 4s for level 0 and level 1 for testing
    conf.set(ns +
            DecayRpcScheduler.IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_THRESHOLDS_KEY
        , "2s, 4s");

    // Set max queue size to 3 so that 2 calls from the test won't trigger
    // back off because the queue is full.
    RPC.Builder builder = newServerBuilder(conf)
        .setQueueSizePerHandler(queueSizePerHandler).setNumHandlers(1)
        .setVerbose(true);
    return setupTestServer(builder);
  }

  /**
   *  Test RPC timeout.
   */
  @Test(timeout=30000)
  public void testClientRpcTimeout() throws Exception {
    Server server;
    TestRpcService proxy = null;

    RPC.Builder builder = newServerBuilder(conf)
        .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true);
    server = setupTestServer(builder);

    try {
      // Test RPC timeout with default ipc.client.ping.
      try {
        Configuration c = new Configuration(conf);
        c.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1000);
        proxy = getClient(addr, c);
        proxy.sleep(null, newSleepRequest(3000));
        fail("RPC should time out.");
      } catch (ServiceException e) {
        assertTrue(e.getCause() instanceof SocketTimeoutException);
        LOG.info("got expected timeout.", e);
      }

      // Test RPC timeout when ipc.client.ping is false.
      try {
        Configuration c = new Configuration(conf);
        c.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        c.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1000);
        proxy = getClient(addr, c);
        proxy.sleep(null, newSleepRequest(3000));
        fail("RPC should time out.");
      } catch (ServiceException e) {
        assertTrue(e.getCause() instanceof SocketTimeoutException);
        LOG.info("got expected timeout.", e);
      }

      // Test negative timeout value.
      try {
        Configuration c = new Configuration(conf);
        c.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1);
        proxy = getClient(addr, c);
        proxy.sleep(null, newSleepRequest(2000));
      } catch (ServiceException e) {
        LOG.info("got unexpected exception.", e);
        fail("RPC should not time out.");
      }

      // Test RPC timeout greater than ipc.ping.interval.
      try {
        Configuration c = new Configuration(conf);
        c.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        c.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, 800);
        c.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 1000);
        proxy = getClient(addr, c);

        try {
          // should not time out because effective rpc-timeout is
          // multiple of ping interval: 1600 (= 800 * (1000 / 800 + 1))
          proxy.sleep(null, newSleepRequest(1300));
        } catch (ServiceException e) {
          LOG.info("got unexpected exception.", e);
          fail("RPC should not time out.");
        }

        proxy.sleep(null, newSleepRequest(2000));
        fail("RPC should time out.");
      } catch (ServiceException e) {
        assertTrue(e.getCause() instanceof SocketTimeoutException);
        LOG.info("got expected timeout.", e);
      }

    } finally {
      stop(server, proxy);
    }
  }

  public static void main(String[] args) throws Exception {
    new TestRPC().testCallsInternal(conf);
  }
}
