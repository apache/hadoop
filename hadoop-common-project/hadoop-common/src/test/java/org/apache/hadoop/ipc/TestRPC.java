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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.metrics.RpcMetrics;

import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.test.MetricsAsserts.assertGaugeGt;
import static org.apache.hadoop.test.MetricsAsserts.assertGaugeGte;
import static org.apache.hadoop.test.MetricsAsserts.mockMetricsRecordBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getDoubleGauge;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** Unit tests for RPC. */
@SuppressWarnings("deprecation")
public class TestRPC extends TestRpcBase {

  public static final Logger LOG = LoggerFactory.getLogger(TestRPC.class);

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
          rpcTimeout, connectionRetryPolicy, null, null);
    }

    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        ConnectionId connId, Configuration conf, SocketFactory factory,
        AlignmentContext alignmentContext)
        throws IOException {
      throw new UnsupportedOperationException("This proxy is not supported");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ProtocolProxy<T> getProxy(
        Class<T> protocol, long clientVersion, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
        throws IOException {
      T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
          new Class[] { protocol }, new StoppedInvocationHandler());
      return new ProtocolProxy<>(protocol, proxy, false);
    }

    @Override
    public org.apache.hadoop.ipc.RPC.Server getServer(
        Class<?> protocol, Object instance, String bindAddress, int port,
        int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
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
        .setNumHandlers(1).setNumReaders(3).setQueueSizePerHandler(200)
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
  public void testConnectionWithSocketFactory() throws IOException, ServiceException {
    TestRpcService firstProxy = null;
    TestRpcService secondProxy = null;

    Configuration newConf = new Configuration(conf);
    newConf.set(CommonConfigurationKeysPublic.
        HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "");

    RetryPolicy retryPolicy = RetryUtils.getDefaultRetryPolicy(
        newConf, "Test.No.Such.Key",
        true,
        "Test.No.Such.Key", "10000,6",
        null);

    // create a server with two handlers
    Server server = setupTestServer(newConf, 2);
    try {
      // create the first client
      firstProxy = getClient(addr, newConf);
      // create the second client
      secondProxy = getClient(addr, newConf);

      firstProxy.ping(null, newEmptyRequest());
      secondProxy.ping(null, newEmptyRequest());

      Client client = ProtobufRpcEngine2.getClient(newConf);
      assertEquals(1, client.getConnectionIds().size());

      stop(null, firstProxy, secondProxy);
      ProtobufRpcEngine2.clearClientCache();

      // create the first client with index 1
      firstProxy = getMultipleClientWithIndex(addr, newConf, retryPolicy, 1);
      // create the second client with index 2
      secondProxy = getMultipleClientWithIndex(addr, newConf, retryPolicy, 2);
      firstProxy.ping(null, newEmptyRequest());
      secondProxy.ping(null, newEmptyRequest());

      Client client2 = ProtobufRpcEngine2.getClient(newConf);
      assertEquals(2, client2.getConnectionIds().size());
    } finally {
      System.out.println("Down slow rpc testing");
      stop(server, firstProxy, secondProxy);
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

      // Check tags of the metrics
      assertEquals("" + server.getPort(),
          server.getRpcMetrics().getTag("port").value());

      assertEquals("TestProtobufRpcProto",
          server.getRpcMetrics().getTag("serverName").value());



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
      assertThat(addResponse.getResult()).isEqualTo(3);

      Integer[] integers = new Integer[] {1, 2};
      TestProtos.AddRequestProto2 addRequest2 =
          TestProtos.AddRequestProto2.newBuilder().addAllParams(
              Arrays.asList(integers)).build();
      addResponse = proxy.add2(null, addRequest2);
      assertThat(addResponse.getResult()).isEqualTo(3);

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

  /**
   * This tests the case where the server isn't receiving new data and
   * multiple threads queue up to send rpc requests. Only one of the requests
   * should be written and all of the calling threads should be interrupted.
   *
   * We use a mock SocketFactory so that we can control when the input and
   * output streams are frozen.
   */
  @Test(timeout=30000)
  public void testSlowConnection() throws Exception {
    SocketFactory mockFactory = Mockito.mock(SocketFactory.class);
    Socket mockSocket = Mockito.mock(Socket.class);
    Mockito.when(mockFactory.createSocket()).thenReturn(mockSocket);
    Mockito.when(mockSocket.getPort()).thenReturn(1234);
    Mockito.when(mockSocket.getLocalPort()).thenReturn(2345);
    MockOutputStream mockOutputStream = new MockOutputStream();
    Mockito.when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);
    // Use an input stream that always blocks
    Mockito.when(mockSocket.getInputStream()).thenReturn(new InputStream() {
      @Override
      public int read() throws IOException {
        // wait forever
        while (true) {
          try {
            Thread.sleep(TimeUnit.DAYS.toMillis(1));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("test");
          }
        }
      }
    });
    Configuration clientConf = new Configuration();
    // disable ping & timeout to minimize traffic
    clientConf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
    clientConf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, 0);
    RPC.setProtocolEngine(clientConf, TestRpcService.class, ProtobufRpcEngine2.class);
    // set async mode so that we don't need to implement the input stream
    final boolean wasAsync = Client.isAsynchronousMode();
    TestRpcService client = null;
    try {
      Client.setAsynchronousMode(true);
      client = RPC.getProtocolProxy(
          TestRpcService.class,
          0,
          new InetSocketAddress("localhost", 1234),
          UserGroupInformation.getCurrentUser(),
          clientConf,
          mockFactory).getProxy();
      // The connection isn't actually made until the first call.
      client.ping(null, newEmptyRequest());
      mockOutputStream.waitForFlush(1);
      final long headerAndFirst = mockOutputStream.getBytesWritten();
      client.ping(null, newEmptyRequest());
      mockOutputStream.waitForFlush(2);
      final long second = mockOutputStream.getBytesWritten() - headerAndFirst;
      // pause the writer thread
      mockOutputStream.pause();
      // create a set of threads to create calls that will back up
      ExecutorService pool = Executors.newCachedThreadPool();
      Future[] futures = new Future[numThreads];
      final AtomicInteger doneThreads = new AtomicInteger(0);
      for(int thread = 0; thread < numThreads; ++thread) {
        final TestRpcService finalClient = client;
        futures[thread] = pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            finalClient.ping(null, newEmptyRequest());
            doneThreads.incrementAndGet();
            return null;
          }
        });
      }
      // wait until the threads have started writing
      mockOutputStream.waitForWriters();
      // interrupt all the threads
      for(int thread=0; thread < numThreads; ++thread) {
        assertTrue("cancel thread " + thread,
            futures[thread].cancel(true));
      }
      // wait until all the writers are cancelled
      pool.shutdown();
      pool.awaitTermination(10, TimeUnit.SECONDS);
      mockOutputStream.resume();
      // wait for the in flight rpc request to be flushed
      mockOutputStream.waitForFlush(3);
      // All the threads should have been interrupted
      assertEquals(0, doneThreads.get());
      // make sure that only one additional rpc request was sent
      assertEquals(headerAndFirst + second * 2,
          mockOutputStream.getBytesWritten());
    } finally {
      Client.setAsynchronousMode(wasAsync);
      if (client != null) {
        RPC.stopProxy(client);
      }
    }
  }

  private static final class MockOutputStream extends OutputStream {
    private long bytesWritten = 0;
    private AtomicInteger flushCount = new AtomicInteger(0);
    private ReentrantLock lock = new ReentrantLock(true);

    @Override
    public synchronized void write(int b) throws IOException {
      lock.lock();
      bytesWritten += 1;
      lock.unlock();
    }

    @Override
    public void flush() {
      flushCount.incrementAndGet();
    }

    public synchronized long getBytesWritten() {
      return bytesWritten;
    }

    public void pause() {
      lock.lock();
    }

    public void resume() {
      lock.unlock();
    }

    private static final int DELAY_MS = 250;

    /**
     * Wait for the Nth flush, which we assume will happen exactly when the
     * Nth RPC request is sent.
     * @param flush the total flush count to wait for
     * @throws InterruptedException
     */
    public void waitForFlush(int flush) throws InterruptedException {
      while (flushCount.get() < flush) {
        Thread.sleep(DELAY_MS);
      }
    }

    public void waitForWriters() throws InterruptedException {
      while (!lock.hasQueuedThreads()) {
        Thread.sleep(DELAY_MS);
      }
    }
  }

  /**
   * This test causes an exception in the RPC connection setup to make
   * sure that threads aren't leaked.
   */
  @Test(timeout=30000)
  public void testBadSetup() throws Exception {
    SocketFactory mockFactory = Mockito.mock(SocketFactory.class);
    Mockito.when(mockFactory.createSocket())
        .thenThrow(new IOException("can't connect"));
    Configuration clientConf = new Configuration();
    // Set an illegal value to cause an exception in the constructor
    clientConf.set(CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
        "xxx");
    RPC.setProtocolEngine(clientConf, TestRpcService.class,
        ProtobufRpcEngine2.class);
    TestRpcService client = null;
    int threadCount = Thread.getAllStackTraces().size();
    try {
      try {
        client = RPC.getProtocolProxy(
            TestRpcService.class,
            0,
            new InetSocketAddress("localhost", 1234),
            UserGroupInformation.getCurrentUser(),
            clientConf,
            mockFactory).getProxy();
        client.ping(null, newEmptyRequest());
        assertTrue("Didn't throw exception!", false);
      } catch (ServiceException nfe) {
        // ensure no extra threads are running.
        assertEquals(threadCount, Thread.getAllStackTraces().size());
      } catch (Throwable t) {
        assertTrue("wrong exception: " + t, false);
      }
    } finally {
      if (client != null) {
        RPC.stopProxy(client);
      }
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
    final Server server;
    TestRpcService proxy = null;

    final int interval = 1;
    conf.setBoolean(CommonConfigurationKeys.
        RPC_METRICS_QUANTILE_ENABLE, true);
    conf.set(CommonConfigurationKeys.
        RPC_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);

    server = setupTestServer(conf, 5);
    String testUser = "testUser";
    UserGroupInformation anotherUser =
        UserGroupInformation.createRemoteUser(testUser);
    TestRpcService proxy2 =
        anotherUser.doAs(new PrivilegedAction<TestRpcService>() {
          public TestRpcService run() {
            try {
              return RPC.getProxy(TestRpcService.class, 0,
                  server.getListenerAddress(), conf);
            } catch (IOException e) {
              e.printStackTrace();
            }
            return null;
          }
        });
    try {
      proxy = getClient(addr, conf);

      for (int i = 0; i < 1000; i++) {
        proxy.ping(null, newEmptyRequest());

        proxy.echo(null, newEchoRequest("" + i));
        proxy2.echo(null, newEchoRequest("" + i));
      }
      MetricsRecordBuilder rpcMetrics =
          getMetrics(server.getRpcMetrics().name());
      assertEquals("Expected correct rpc en queue count",
          3000, getLongCounter("RpcEnQueueTimeNumOps", rpcMetrics));
      assertEquals("Expected correct rpc queue count",
          3000, getLongCounter("RpcQueueTimeNumOps", rpcMetrics));
      assertEquals("Expected correct rpc processing count",
          3000, getLongCounter("RpcProcessingTimeNumOps", rpcMetrics));
      assertEquals("Expected correct rpc lock wait count",
          3000, getLongCounter("RpcLockWaitTimeNumOps", rpcMetrics));
      assertEquals("Expected correct rpc response count",
          3000, getLongCounter("RpcResponseTimeNumOps", rpcMetrics));
      assertEquals("Expected zero rpc lock wait time",
          0, getDoubleGauge("RpcLockWaitTimeAvgTime", rpcMetrics), 0.001);
      MetricsAsserts.assertQuantileGauges("RpcEnQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcProcessingTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcResponseTime" + interval + "s",
          rpcMetrics);
      String actualUserVsCon = MetricsAsserts
          .getStringMetric("NumOpenConnectionsPerUser", rpcMetrics);
      String proxyUser =
          UserGroupInformation.getCurrentUser().getShortUserName();
      assertTrue(actualUserVsCon.contains("\"" + proxyUser + "\":1"));
      assertTrue(actualUserVsCon.contains("\"" + testUser + "\":1"));

      proxy.lockAndSleep(null, newSleepRequest(5));
      rpcMetrics = getMetrics(server.getRpcMetrics().name());
      assertGauge("RpcLockWaitTimeAvgTime",
          (double)(server.getRpcMetrics().getMetricsTimeUnit().convert(10L,
              TimeUnit.SECONDS)), rpcMetrics);
    } finally {
      if (proxy2 != null) {
        RPC.stopProxy(proxy2);
      }
      stop(server, proxy);
    }
  }

  @Test
  public void testNumInProcessHandlerMetrics() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.
        createUserForTesting("user123", new String[0]);
    // use 1 handler so the callq can be plugged
    final Server server = setupTestServer(conf, 1);
    try {
      RpcMetrics rpcMetrics = server.getRpcMetrics();
      assertEquals(0, rpcMetrics.getNumInProcessHandler());

      ExternalCall<String> call1 = newExtCall(ugi, () -> {
        assertEquals(1, rpcMetrics.getNumInProcessHandler());
        return UserGroupInformation.getCurrentUser().getUserName();
      });
      ExternalCall<Void> call2 = newExtCall(ugi, () -> {
        assertEquals(1, rpcMetrics.getNumInProcessHandler());
        return null;
      });

      server.queueCall(call1);
      server.queueCall(call2);

      // Wait for call1 and call2 to enter the handler.
      call1.get();
      call2.get();
      assertEquals(0, rpcMetrics.getNumInProcessHandler());
    } finally {
      server.stop();
    }
  }

  /**
   * Test the rpcCallSucesses metric in RpcMetrics.
   */
  @Test
  public void testRpcCallSuccessesMetric() throws Exception {
    final Server server;
    TestRpcService proxy = null;

    server = setupTestServer(conf, 5);
    try {
      proxy = getClient(addr, conf);

      // 10 successful responses
      for (int i = 0; i < 10; i++) {
        proxy.ping(null, newEmptyRequest());
      }
      MetricsRecordBuilder rpcMetrics =
          getMetrics(server.getRpcMetrics().name());
      assertCounter("RpcCallSuccesses", 10L, rpcMetrics);
      // rpcQueueTimeNumOps equals total number of RPC calls.
      assertCounter("RpcQueueTimeNumOps", 10L, rpcMetrics);

      // 2 failed responses with ERROR status and 1 more successful response.
      for (int i = 0; i < 2; i++) {
        try {
          proxy.error(null, newEmptyRequest());
        } catch (ServiceException ignored) {
        }
      }
      proxy.ping(null, newEmptyRequest());

      rpcMetrics = getMetrics(server.getRpcMetrics().name());
      assertCounter("RpcCallSuccesses", 11L, rpcMetrics);
      assertCounter("RpcQueueTimeNumOps", 13L, rpcMetrics);
    } finally {
      stop(server, proxy);
    }
  }

  /**
   * Test per-type overall RPC processing time metric.
   */
  @Test
  public void testOverallRpcProcessingTimeMetric() throws Exception {
    final Server server;
    TestRpcService proxy = null;

    server = setupTestServer(conf, 5);
    try {
      proxy = getClient(addr, conf);

      // Sent 1 ping request and 2 lockAndSleep requests
      proxy.ping(null, newEmptyRequest());
      proxy.lockAndSleep(null, newSleepRequest(10));
      proxy.lockAndSleep(null, newSleepRequest(12));

      MetricsRecordBuilder rb = mockMetricsRecordBuilder();
      MutableRatesWithAggregation rates =
          server.rpcDetailedMetrics.getOverallRpcProcessingRates();
      rates.snapshot(rb, true);

      // Verify the ping request.
      // Overall processing time for ping is zero when this test is run together with
      // the rest of tests. Thus, we use assertGaugeGte() for OverallPingAvgTime.
      assertCounter("OverallPingNumOps", 1L, rb);
      assertGaugeGte("OverallPingAvgTime", 0.0, rb);

      // Verify lockAndSleep requests. AvgTime should be greater than 10 ms,
      // since we sleep for 10 and 12 ms respectively.
      assertCounter("OverallLockAndSleepNumOps", 2L, rb);
      assertGaugeGt("OverallLockAndSleepAvgTime", 10.0, rb);

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

    CallQueueManager<Call> spy = spy(server.getCallQueue());
    server.setCallQueue(spy);

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
        verify(spy, timeout(500).times(i + 1)).addInternal(any(), eq(false));
      }
      try {
        proxy.sleep(null, newSleepRequest(100));
      } catch (ServiceException e) {
        RemoteException re = (RemoteException) e.getCause();
        IOException unwrapExeption = re.unwrapRemoteException();
        if (unwrapExeption instanceof RetriableException) {
          succeeded = true;
          assertEquals(1L, server.getRpcMetrics().getClientBackoffDisconnected());
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

    final List<Future<Void>> res = new ArrayList<>();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(numClients);
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);

    final String ns = CommonConfigurationKeys.IPC_NAMESPACE + ".0";
    Server server = setupDecayRpcSchedulerandTestServer(ns + ".");

    CallQueueManager<Call> spy = spy(server.getCallQueue());
    server.setCallQueue(spy);

    Exception lastException = null;
    proxy = getClient(addr, conf);

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
        verify(spy, timeout(500).times(i + 1)).addInternal(any(), eq(false));
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

  /** Test that the metrics for DecayRpcScheduler are updated. */
  @Test (timeout=30000)
  public void testDecayRpcSchedulerMetrics() throws Exception {
    final String ns = CommonConfigurationKeys.IPC_NAMESPACE + ".0";
    Server server = setupDecayRpcSchedulerandTestServer(ns + ".");

    MetricsRecordBuilder rb1 =
        getMetrics("DecayRpcSchedulerMetrics2." + ns);
    final long beginDecayedCallVolume = MetricsAsserts.getLongCounter(
        "DecayedCallVolume", rb1);
    final long beginRawCallVolume = MetricsAsserts.getLongCounter(
        "CallVolume", rb1);
    final int beginUniqueCaller = MetricsAsserts.getIntCounter("UniqueCallers",
        rb1);

    TestRpcService proxy = getClient(addr, conf);
    try {
      for (int i = 0; i < 2; i++) {
        proxy.sleep(null, newSleepRequest(100));
      }

      // Lets Metric system update latest metrics
      GenericTestUtils.waitFor(() -> {
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

        LOG.info("DecayedCallVolume: {}", decayedCallVolume1);
        LOG.info("CallVolume: {}", rawCallVolume1);
        LOG.info("UniqueCaller: {}", uniqueCaller1);
        LOG.info("Priority.0.CompletedCallVolume: {}", callVolumePriority0);
        LOG.info("Priority.1.CompletedCallVolume: {}", callVolumePriority1);
        LOG.info("Priority.0.AvgResponseTime: {}", avgRespTimePriority0);
        LOG.info("Priority.1.AvgResponseTime: {}", avgRespTimePriority1);

        return decayedCallVolume1 > beginDecayedCallVolume &&
            rawCallVolume1 > beginRawCallVolume &&
            uniqueCaller1 > beginUniqueCaller;
      }, 30, 60000);
    } finally {
      stop(server, proxy);
    }
  }

  @Test (timeout=30000)
  public void testProtocolUserPriority() throws Exception {
    final String ns = CommonConfigurationKeys.IPC_NAMESPACE + ".0";
    conf.set(CLIENT_PRINCIPAL_KEY, "clientForProtocol");
    Server server = null;
    try {
      server = setupDecayRpcSchedulerandTestServer(ns + ".");

      UserGroupInformation ugi1 = UserGroupInformation.createRemoteUser("user");
      // normal users start with priority 0.
      Assert.assertEquals(0, server.getPriorityLevel(ugi1));
      // calls for a protocol defined client will have priority of 0.
      Assert.assertEquals(0, server.getPriorityLevel(newSchedulable(ugi1)));

      // protocol defined client will have top priority of -1.
      UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("clientForProtocol");
      Assert.assertEquals(-1, server.getPriorityLevel(ugi2));
      // calls for a protocol defined client will have priority of 0.
      Assert.assertEquals(0, server.getPriorityLevel(newSchedulable(ugi2)));

      // user call
      ugi1.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          TestRpcService proxy = getClient(addr, conf);
          for (int i = 0; i < 10; i++) {
            proxy.ping(null, newEmptyRequest());
          }
          return null;
        }
      });
      // clientForProtocol call
      ugi2.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          TestRpcService proxy = getClient(addr, conf);
          for (int i = 0; i < 30; i++) {
            proxy.ping(null, newEmptyRequest());
          }
          return null;
        }
      });

      CallQueueManager callQueueManager =
          (CallQueueManager) Whitebox.getInternalState(server, "callQueue");
      DecayRpcScheduler scheduler =
          (DecayRpcScheduler) Whitebox.getInternalState(callQueueManager, "scheduler");
      Assert.assertNotNull(scheduler);

      // test total costs.
      assertEquals(10, scheduler.getTotalCallVolume());
      assertEquals(10, scheduler.getTotalRawCallVolume());
      assertEquals(30, scheduler.getTotalServiceUserCallVolume());
      assertEquals(30, scheduler.getTotalServiceUserRawCallVolume());
      assertEquals(1, scheduler.getPriorityLevel(newSchedulable(ugi1)));
      assertEquals(0, scheduler.getPriorityLevel(newSchedulable(ugi2)));

      // test total costs after decay.
      scheduler.forceDecay();
      assertEquals(5, scheduler.getTotalCallVolume());
      assertEquals(10, scheduler.getTotalRawCallVolume());
      assertEquals(15, scheduler.getTotalServiceUserCallVolume());
      assertEquals(30, scheduler.getTotalServiceUserRawCallVolume());
      assertEquals(1, scheduler.getPriorityLevel(newSchedulable(ugi1)));
      assertEquals(0, scheduler.getPriorityLevel(newSchedulable(ugi2)));

    } finally {
      stop(server, null);
    }
  }

  private static Schedulable newSchedulable(UserGroupInformation ugi) {
    return new Schedulable(){
      @Override
      public UserGroupInformation getUserGroupInformation() {
        return ugi;
      }
      @Override
      public int getPriorityLevel() {
        return 0; // doesn't matter.
      }
    };
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

  @Test
  public void testServerNameFromClass() {
    Assert.assertEquals("TestRPC",
        RPC.Server.serverNameFromClass(this.getClass()));
    Assert.assertEquals("TestClass",
        RPC.Server.serverNameFromClass(TestRPC.TestClass.class));

    Object testing = new TestClass().classFactory();
    Assert.assertEquals("Embedded",
        RPC.Server.serverNameFromClass(testing.getClass()));

    testing = new TestClass().classFactoryAbstract();
    Assert.assertEquals("TestClass",
        RPC.Server.serverNameFromClass(testing.getClass()));

    testing = new TestClass().classFactoryObject();
    Assert.assertEquals("TestClass",
        RPC.Server.serverNameFromClass(testing.getClass()));

  }

  static class TestClass {
    class Embedded {
    }

    abstract class AbstractEmbedded {

    }

    private Object classFactory() {
      return new Embedded();
    }

    private Object classFactoryAbstract() {
      return new AbstractEmbedded() {
      };
    }

    private Object classFactoryObject() {
      return new Object() {
      };
    }

  }
  public static class FakeRequestClass extends RpcWritable {
    static volatile IOException exception;
    @Override
    void writeTo(ResponseBuffer out) throws IOException {
      throw new UnsupportedOperationException();
    }
    @Override
    <T> T readFrom(ByteBuffer bb) throws IOException {
      throw exception;
    }
  }

  @SuppressWarnings("serial")
  public static class TestReaderException extends IOException {
    public TestReaderException(String msg) {
      super(msg);
    }
    @Override
    public boolean equals(Object t) {
      return (t.getClass() == TestReaderException.class) &&
             getMessage().equals(((TestReaderException)t).getMessage());
    }
  }

  @Test (timeout=30000)
  public void testReaderExceptions() throws Exception {
    Server server = null;
    TestRpcService proxy = null;

    // will attempt to return this exception from a reader with and w/o
    // the connection closing.
    IOException expectedIOE = new TestReaderException("testing123");

    @SuppressWarnings("serial")
    IOException rseError = new RpcServerException("keepalive", expectedIOE){
      @Override
      public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.ERROR;
      }
    };
    @SuppressWarnings("serial")
    IOException rseFatal = new RpcServerException("disconnect", expectedIOE) {
      @Override
      public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.FATAL;
      }
    };

    try {
      RPC.Builder builder = newServerBuilder(conf)
          .setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true);
      server = setupTestServer(builder);
      server.setRpcRequestClass(FakeRequestClass.class);
      MutableCounterLong authMetric = server.getRpcMetrics().getRpcAuthorizationSuccesses();

      proxy = getClient(addr, conf);
      boolean isDisconnected = true;
      Connection lastConn = null;
      long expectedAuths = 0;

      // fuzz the client.
      for (int i=0; i < 128; i++) {
        String reqName = "request[" + i + "]";
        int r = ThreadLocalRandom.current().nextInt();
        final boolean doDisconnect = r % 4 == 0;
        LOG.info("TestDisconnect request[" + i + "] " +
                 " shouldConnect=" + isDisconnected +
                 " willDisconnect=" + doDisconnect);
        if (isDisconnected) {
          expectedAuths++;
        }
        try {
          FakeRequestClass.exception = doDisconnect ? rseFatal : rseError;
          proxy.ping(null, newEmptyRequest());
          fail(reqName + " didn't fail");
        } catch (ServiceException e) {
          RemoteException re = (RemoteException)e.getCause();
          assertEquals(reqName, expectedIOE, re.unwrapRemoteException());
        }
        // check authorizations to ensure new connection when expected,
        // then conclusively determine if connections are disconnected
        // correctly.
        assertEquals(reqName, expectedAuths, authMetric.value());
        if (!doDisconnect) {
          // if it wasn't fatal, verify there's only one open connection.
          Connection[] conns = server.getConnections();
          assertEquals(reqName, 1, conns.length);
          String connectionInfo = conns[0].toString();
          LOG.info("Connection is from: {}", connectionInfo);
          assertEquals(
              "Connection string representation should include only IP address for healthy "
                  + "connection", 1, connectionInfo.split(" / ").length);
          // verify whether the connection should have been reused.
          if (isDisconnected) {
            assertNotSame(reqName, lastConn, conns[0]);
          } else {
            assertSame(reqName, lastConn, conns[0]);
          }
          lastConn = conns[0];
        } else if (lastConn != null) {
          // avoid race condition in server where connection may not be
          // fully removed yet.  just make sure it's marked for being closed.
          // the open connection checks above ensure correct behavior.
          assertTrue(reqName, lastConn.shouldClose());
        }
        isDisconnected = doDisconnect;
      }
    } finally {
      stop(server, proxy);
    }
  }

  @Test
  public void testSetProtocolEngine() {
    Configuration conf = new Configuration();
    RPC.setProtocolEngine(conf, StoppedProtocol.class, StoppedRpcEngine.class);
    RpcEngine rpcEngine = RPC.getProtocolEngine(StoppedProtocol.class, conf);
    assertTrue(rpcEngine instanceof StoppedRpcEngine);

    RPC.setProtocolEngine(conf, StoppedProtocol.class, ProtobufRpcEngine.class);
    rpcEngine = RPC.getProtocolEngine(StoppedProtocol.class, conf);
    assertTrue(rpcEngine instanceof StoppedRpcEngine);
  }

  @Test
  public void testRpcMetricsInNanos() throws Exception {
    final Server server;
    TestRpcService proxy = null;

    final int interval = 1;
    conf.setBoolean(CommonConfigurationKeys.
        RPC_METRICS_QUANTILE_ENABLE, true);
    conf.set(CommonConfigurationKeys.
        RPC_METRICS_PERCENTILES_INTERVALS_KEY, "" + interval);
    conf.set(CommonConfigurationKeys.RPC_METRICS_TIME_UNIT, "NANOSECONDS");

    server = setupTestServer(conf, 5);
    String testUser = "testUserInNanos";
    UserGroupInformation anotherUser =
        UserGroupInformation.createRemoteUser(testUser);
    TestRpcService proxy2 =
        anotherUser.doAs((PrivilegedAction<TestRpcService>) () -> {
          try {
            return RPC.getProxy(TestRpcService.class, 0,
                server.getListenerAddress(), conf);
          } catch (IOException e) {
            LOG.error("Something went wrong.", e);
          }
          return null;
        });
    try {
      proxy = getClient(addr, conf);
      for (int i = 0; i < 100; i++) {
        proxy.ping(null, newEmptyRequest());
        proxy.echo(null, newEchoRequest("" + i));
        proxy2.echo(null, newEchoRequest("" + i));
      }
      MetricsRecordBuilder rpcMetrics =
          getMetrics(server.getRpcMetrics().name());
      assertEquals("Expected zero rpc lock wait time",
          0, getDoubleGauge("RpcLockWaitTimeAvgTime", rpcMetrics), 0.001);
      MetricsAsserts.assertQuantileGauges("RpcEnQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcQueueTime" + interval + "s",
          rpcMetrics);
      MetricsAsserts.assertQuantileGauges("RpcProcessingTime" + interval + "s",
          rpcMetrics);

      proxy.lockAndSleep(null, newSleepRequest(5));
      rpcMetrics = getMetrics(server.getRpcMetrics().name());
      assertGauge("RpcLockWaitTimeAvgTime",
          (double)(server.getRpcMetrics().getMetricsTimeUnit().convert(10L,
              TimeUnit.SECONDS)), rpcMetrics);
      LOG.info("RpcProcessingTimeAvgTime: {} , RpcEnQueueTimeAvgTime: {} , RpcQueueTimeAvgTime: {}",
          getDoubleGauge("RpcProcessingTimeAvgTime", rpcMetrics),
          getDoubleGauge("RpcEnQueueTimeAvgTime", rpcMetrics),
          getDoubleGauge("RpcQueueTimeAvgTime", rpcMetrics));

      assertTrue(getDoubleGauge("RpcProcessingTimeAvgTime", rpcMetrics)
          > 4000000D);
      assertTrue(getDoubleGauge("RpcEnQueueTimeAvgTime", rpcMetrics)
          > 4000D);
      assertTrue(getDoubleGauge("RpcQueueTimeAvgTime", rpcMetrics)
          > 4000D);
    } finally {
      if (proxy2 != null) {
        RPC.stopProxy(proxy2);
      }
      stop(server, proxy);
    }
  }

  @Test
  public void testNumTotalRequestsMetrics() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.
        createUserForTesting("userXyz", new String[0]);

    final Server server = setupTestServer(conf, 1);

    ExecutorService executorService = null;
    try {
      RpcMetrics rpcMetrics = server.getRpcMetrics();
      assertEquals(0, rpcMetrics.getTotalRequests());
      assertEquals(0, rpcMetrics.getTotalRequestsPerSecond());

      List<ExternalCall<Void>> externalCallList = new ArrayList<>();

      executorService = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("testNumTotalRequestsMetrics")
              .build());
      AtomicInteger rps = new AtomicInteger(0);
      CountDownLatch countDownLatch = new CountDownLatch(1);
      executorService.submit(() -> {
        while (true) {
          int numRps = (int) rpcMetrics.getTotalRequestsPerSecond();
          rps.getAndSet(numRps);
          if (rps.get() > 0) {
            countDownLatch.countDown();
            break;
          }
        }
      });

      for (int i = 0; i < 100000; i++) {
        externalCallList.add(newExtCall(ugi, () -> null));
      }
      for (ExternalCall<Void> externalCall : externalCallList) {
        server.queueCall(externalCall);
      }
      for (ExternalCall<Void> externalCall : externalCallList) {
        externalCall.get();
      }

      assertEquals(100000, rpcMetrics.getTotalRequests());
      if (countDownLatch.await(10, TimeUnit.SECONDS)) {
        assertTrue(rps.get() > 10);
      } else {
        throw new AssertionError("total requests per seconds are still 0");
      }
    } finally {
      if (executorService != null) {
        executorService.shutdown();
      }
      server.stop();
    }
  }


  public static void main(String[] args) throws Exception {
    new TestRPC().testCallsInternal(conf);
  }
}
