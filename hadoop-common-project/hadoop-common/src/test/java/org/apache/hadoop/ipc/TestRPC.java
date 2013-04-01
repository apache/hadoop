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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
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
import org.apache.hadoop.test.MockitoUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;

/** Unit tests for RPC. */
@SuppressWarnings("deprecation")
public class TestRPC {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestRPC.class);
  
  private static Configuration conf;
  
  @Before
  public void setupConf() {
    conf = new Configuration();
    conf.setClass("rpc.engine." + StoppedProtocol.class.getName(),
        StoppedRpcEngine.class, RpcEngine.class);
    UserGroupInformation.setConfiguration(conf);
  }

  int datasize = 1024*100;
  int numThreads = 50;
	
  public interface TestProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
    
    void ping() throws IOException;
    void slowPing(boolean shouldSlow) throws IOException;
    String echo(String value) throws IOException;
    String[] echo(String[] value) throws IOException;
    Writable echo(Writable value) throws IOException;
    int add(int v1, int v2) throws IOException;
    int add(int[] values) throws IOException;
    int error() throws IOException;
    void testServerGet() throws IOException;
    int[] exchange(int[] values) throws IOException;
    
    DescriptorProtos.EnumDescriptorProto exchangeProto(
        DescriptorProtos.EnumDescriptorProto arg);
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
    public synchronized void slowPing(boolean shouldSlow) {
      if (shouldSlow) {
        while (fastPingCounter < 2) {
          try {
          wait();  // slow response until two fast pings happened
          } catch (InterruptedException ignored) {}
        }
        fastPingCounter -= 2;
      } else {
        fastPingCounter++;
        notify();
      }
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

    @Override
    public void testServerGet() throws IOException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new IOException("Server.get() failed");
      }
    }

    @Override
    public int[] exchange(int[] values) {
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      return values;
    }

    @Override
    public EnumDescriptorProto exchangeProto(EnumDescriptorProto arg) {
      return arg;
    }
  }

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    int datasize;
    TestProtocol proxy;

    Transactions(TestProtocol proxy, int datasize) {
      this.proxy = proxy;
      this.datasize = datasize;
    }

    // do two RPC that transfers data.
    @Override
    public void run() {
      int[] indata = new int[datasize];
      int[] outdata = null;
      int val = 0;
      try {
        outdata = proxy.exchange(indata);
        val = proxy.add(1,2);
      } catch (IOException e) {
        assertTrue("Exception from RPC exchange() "  + e, false);
      }
      assertEquals(indata.length, outdata.length);
      assertEquals(val, 3);
      for (int i = 0; i < outdata.length; i++) {
        assertEquals(outdata[i], i);
      }
    }
  }

  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestProtocol proxy;
    private volatile boolean done;
   
    SlowRPC(TestProtocol proxy) {
      this.proxy = proxy;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    @Override
    public void run() {
      try {
        proxy.slowPing(true);   // this would hang until two fast pings happened
        done = true;
      } catch (IOException e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }
  }
  
  /**
   * A basic interface for testing client-side RPC resource cleanup.
   */
  private static interface StoppedProtocol {
    long versionID = 0;

    public void stop();
  }
  
  /**
   * A class used for testing cleanup of client side RPC resources.
   */
  private static class StoppedRpcEngine implements RpcEngine {

    @SuppressWarnings("unchecked")
    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
        ) throws IOException {
      T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
              new Class[] { protocol }, new StoppedInvocationHandler());
      return new ProtocolProxy<T>(protocol, proxy, false);
    }

    @Override
    public org.apache.hadoop.ipc.RPC.Server getServer(Class<?> protocol,
        Object instance, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose, Configuration conf,
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
  public void testConfRpc() throws Exception {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
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
    server.stop();
    
    server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(1).setnumReaders(3).setQueueSizePerHandler(200)
        .setVerbose(false).build();        
        
    assertEquals(3, server.getNumReaders());
    assertEquals(200, server.getMaxQueueSize());
    server.stop();    
  }

  @Test
  public void testProxyAddress() throws Exception {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0).build();
    TestProtocol proxy = null;
    
    try {
      server.start();
      InetSocketAddress addr = NetUtils.getConnectAddress(server);

      // create a client
      proxy = (TestProtocol)RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      
      assertEquals(addr, RPC.getServerAddress(proxy));
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  @Test
  public void testSlowRpc() throws Exception {
    System.out.println("Testing Slow RPC");
    // create a server with two handlers
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(2).setVerbose(false).build();
    
    TestProtocol proxy = null;
    
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    // create a client
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);

    SlowRPC slowrpc = new SlowRPC(proxy);
    Thread thread = new Thread(slowrpc, "SlowRPC");
    thread.start(); // send a slow RPC, which won't return until two fast pings
    assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

    proxy.slowPing(false); // first fast ping
    
    // verify that the first RPC is still stuck
    assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());

    proxy.slowPing(false); // second fast ping
    
    // Now the slow ping should be able to be executed
    while (!slowrpc.isDone()) {
      System.out.println("Waiting for slow RPC to get done.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      System.out.println("Down slow rpc testing");
    }
  }
  
  @Test
  public void testCalls() throws Exception {
    testCallsInternal(conf);
  }
  
  private void testCallsInternal(Configuration conf) throws Exception {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0).build();
    TestProtocol proxy = null;
    try {
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    proxy = (TestProtocol)RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);
      
    proxy.ping();

    String stringResult = proxy.echo("foo");
    assertEquals(stringResult, "foo");

    stringResult = proxy.echo((String)null);
    assertEquals(stringResult, null);
    
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
    
    String[] stringResults = proxy.echo(new String[]{"foo","bar"});
    assertTrue(Arrays.equals(stringResults, new String[]{"foo","bar"}));

    stringResults = proxy.echo((String[])null);
    assertTrue(Arrays.equals(stringResults, null));

    UTF8 utf8Result = (UTF8)proxy.echo(new UTF8("hello world"));
    assertEquals(utf8Result, new UTF8("hello world"));

    utf8Result = (UTF8)proxy.echo((UTF8)null);
    assertEquals(utf8Result, null);

    int intResult = proxy.add(1, 2);
    assertEquals(intResult, 3);

    intResult = proxy.add(new int[] {1, 2});
    assertEquals(intResult, 3);
    
    // Test protobufs
    EnumDescriptorProto sendProto =
      EnumDescriptorProto.newBuilder().setName("test").build();
    EnumDescriptorProto retProto = proxy.exchangeProto(sendProto);
    assertEquals(sendProto, retProto);
    assertNotSame(sendProto, retProto);

    boolean caught = false;
    try {
      proxy.error();
    } catch (IOException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Caught " + e);
      }
      caught = true;
    }
    assertTrue(caught);

    proxy.testServerGet();

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
      server.stop();
      if(proxy!=null) RPC.stopProxy(proxy);
    }
  }
  
  @Test
  public void testStandaloneClient() throws IOException {
    try {
      TestProtocol proxy = RPC.waitForProxy(TestProtocol.class,
        TestProtocol.versionID, new InetSocketAddress(ADDRESS, 20), conf, 15000L);
      proxy.echo("");
      fail("We should not have reached here");
    } catch (ConnectException ioe) {
      //this is what we expected
    }
  }
  
  private static final String ACL_CONFIG = "test.protocol.acl";
  
  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestProtocol.class) };
    }
    
  }
  
  private void doRPCs(Configuration conf, boolean expectFailure) throws Exception {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();

    server.refreshServiceAcl(conf, new TestPolicyProvider());

    TestProtocol proxy = null;

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    
    try {
      proxy = (TestProtocol)RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      proxy.ping();

      if (expectFailure) {
        fail("Expect RPC.getProxy to fail with AuthorizationException!");
      }
    } catch (RemoteException e) {
      if (expectFailure) {
        assertTrue(e.unwrapRemoteException() instanceof AuthorizationException);
      } else {
        throw e;
      }
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      MetricsRecordBuilder rb = getMetrics(server.rpcMetrics.name());
      if (expectFailure) {
        assertCounter("RpcAuthorizationFailures", 1, rb);
      } else {
        assertCounter("RpcAuthorizationSuccesses", 1, rb);
      }
      //since we don't have authentication turned ON, we should see 
      // 0 for the authentication successes and 0 for failure
      assertCounter("RpcAuthenticationFailures", 0, rb);
      assertCounter("RpcAuthenticationSuccesses", 0, rb);
    }
  }
  
  @Test
  public void testServerAddress() throws IOException {
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    InetSocketAddress bindAddr = null;
    try {
      bindAddr = NetUtils.getConnectAddress(server);
    } finally {
      server.stop();
    }
    assertEquals(bindAddr.getAddress(), InetAddress.getLocalHost());
  }
  
  @Test
  public void testAuthorization() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    
    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);
    
    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);
    
    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);
    // Expect to succeed
    conf.set(ACL_CONFIG, "*");
    doRPCs(conf, false);
    
    // Reset authorization to expect failure
    conf.set(ACL_CONFIG, "invalid invalid");
    doRPCs(conf, true);
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
   * @throws Exception
   */
  @Test(expected=HadoopIllegalArgumentException.class)
  public void testStopNonRegisteredProxy() throws Exception {
    RPC.stopProxy(null);
  }

  /**
   * Test that the mockProtocol helper returns mock proxies that can
   * be stopped without error.
   */
  @Test
  public void testStopMockObject() throws Exception {
    RPC.stopProxy(MockitoUtil.mockProtocol(TestProtocol.class)); 
  }
  
  @Test
  public void testStopProxy() throws IOException {
    StoppedProtocol proxy = (StoppedProtocol) RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(proxy);
    assertEquals(invocationHandler.getCloseCalled(), 0);
    RPC.stopProxy(proxy);
    assertEquals(invocationHandler.getCloseCalled(), 1);
  }
  
  @Test
  public void testWrappedStopProxy() throws IOException {
    StoppedProtocol wrappedProxy = (StoppedProtocol) RPC.getProxy(StoppedProtocol.class,
        StoppedProtocol.versionID, null, conf);
    StoppedInvocationHandler invocationHandler = (StoppedInvocationHandler)
        Proxy.getInvocationHandler(wrappedProxy);
    
    StoppedProtocol proxy = (StoppedProtocol) RetryProxy.create(StoppedProtocol.class,
        wrappedProxy, RetryPolicies.RETRY_FOREVER);
    
    assertEquals(invocationHandler.getCloseCalled(), 0);
    RPC.stopProxy(proxy);
    assertEquals(invocationHandler.getCloseCalled(), 1);
  }
  
  @Test
  public void testErrorMsgForInsecureClient() throws Exception {
    Configuration serverConf = new Configuration(conf);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS,
                                         serverConf);
    UserGroupInformation.setConfiguration(serverConf);
    
    final Server server = new RPC.Builder(serverConf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    server.start();

    UserGroupInformation.setConfiguration(conf);
    boolean succeeded = false;
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    TestProtocol proxy = null;
    try {
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, addr, conf);
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);

    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY, 2);

    UserGroupInformation.setConfiguration(serverConf);
    final Server multiServer = new RPC.Builder(serverConf)
        .setProtocol(TestProtocol.class).setInstance(new TestImpl())
        .setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true)
        .build();
    multiServer.start();
    succeeded = false;
    final InetSocketAddress mulitServerAddr =
                      NetUtils.getConnectAddress(multiServer);
    proxy = null;
    try {
      UserGroupInformation.setConfiguration(conf);
      proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
          TestProtocol.versionID, mulitServerAddr, conf);
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      multiServer.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    assertTrue(succeeded);
  }

  /**
   * Count the number of threads that have a stack frame containing
   * the given string
   */
  private static int countThreads(String search) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains(search)) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  /**
   * Test that server.stop() properly stops all threads
   */
  @Test
  public void testStopsAllThreads() throws Exception {
    int threadsBefore = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads running before test",
      0, threadsBefore);

    final Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new TestImpl()).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).build();
    server.start();
    try {
      int threadsRunning = countThreads("Server$Listener$Reader");
      assertTrue(threadsRunning > 0);
    } finally {
      server.stop();
    }
    int threadsAfter = countThreads("Server$Listener$Reader");
    assertEquals("Expect no Reader threads left running after test",
      0, threadsAfter);
  }
  
  @Test
  public void testRPCBuilder() throws Exception {
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
    final Configuration conf = new Configuration();
    Server server = RPC.getServer(
      TestProtocol.class, new TestImpl(), ADDRESS, 0, 5, true, conf, null
    );
    server.start();
    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
  
      final TestProtocol proxy = (TestProtocol) RPC.getProxy(
          TestProtocol.class, TestProtocol.versionID, addr, conf);
      // Connect to the server
      proxy.ping();
      // Interrupt self, try another call
      Thread.currentThread().interrupt();
      try {
        proxy.ping();
        fail("Interruption did not cause IPC to fail");
      } catch (IOException ioe) {
        if (!ioe.toString().contains("InterruptedException")) {
          throw ioe;
        }
        // clear interrupt status for future tests
        Thread.interrupted();
      }
    } finally {
      server.stop();
    }
  }
  
  @Test(timeout=30000)
  public void testRPCInterrupted() throws IOException, InterruptedException {
    final Configuration conf = new Configuration();
    Server server = RPC.getServer(
      TestProtocol.class, new TestImpl(), ADDRESS, 0, 5, true, conf, null
    );

    server.start();
    try {
      int numConcurrentRPC = 200;
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      final CyclicBarrier barrier = new CyclicBarrier(numConcurrentRPC);
      final CountDownLatch latch = new CountDownLatch(numConcurrentRPC);
      final AtomicBoolean leaderRunning = new AtomicBoolean(true);
      final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
      Thread leaderThread = null;
      
      for (int i = 0; i < numConcurrentRPC; i++) {
        final int num = i;
        final TestProtocol proxy = (TestProtocol) RPC.getProxy(
        TestProtocol.class, TestProtocol.versionID, addr, conf);
        Thread rpcThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              barrier.await();
              while (num == 0 || leaderRunning.get()) {
                proxy.slowPing(false);
              }
  
              proxy.slowPing(false);
            } catch (Exception e) {
              if (num == 0) {
                leaderRunning.set(false);
              } else {
                error.set(e);
              }
  
              LOG.error(e);
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

  public static void main(String[] args) throws Exception {
    new TestRPC().testCallsInternal(conf);

  }
}
