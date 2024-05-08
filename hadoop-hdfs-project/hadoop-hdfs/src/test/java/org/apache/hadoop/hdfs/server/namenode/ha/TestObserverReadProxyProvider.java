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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.StopWatch;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link ObserverReadProxyProvider} under various configurations of
 * NameNode states. Mainly testing that the proxy provider picks the correct
 * NameNode to communicate with.
 */
public class TestObserverReadProxyProvider {
  private final static long SLOW_RESPONSE_SLEEP_TIME = TimeUnit.SECONDS.toMillis(5); // 5 s
  private final static long NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT = TimeUnit.SECONDS.toMillis(2);
  private final static long NAMENODE_HA_STATE_PROBE_TIMEOUT_LONG = TimeUnit.SECONDS.toMillis(25);
  private final GenericTestUtils.LogCapturer proxyLog =
      GenericTestUtils.LogCapturer.captureLogs(ObserverReadProxyProvider.LOG);

  private static final LocatedBlock[] EMPTY_BLOCKS = new LocatedBlock[0];
  private String ns;
  private URI nnURI;

  private ObserverReadProxyProvider<ClientProtocol> proxyProvider;
  private NameNodeAnswer[] namenodeAnswers;
  private String[] namenodeAddrs;

  @BeforeClass
  public static void setLogLevel() {
    GenericTestUtils.setLogLevel(ObserverReadProxyProvider.LOG, Level.DEBUG);
  }

  @Before
  public void setup() throws Exception {
    ns = "testcluster";
    nnURI = URI.create("hdfs://" + ns);
  }

  private void setupProxyProvider(int namenodeCount) throws Exception {
    setupProxyProvider(namenodeCount, new Configuration());
  }

  private void setupProxyProvider(int namenodeCount, long nnHAStateProbeTimeout) throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(NAMENODE_HA_STATE_PROBE_TIMEOUT, nnHAStateProbeTimeout);
    setupProxyProvider(namenodeCount, conf);
  }

  private void setupProxyProvider(int namenodeCount, Configuration conf) throws Exception {
    String[] namenodeIDs = new String[namenodeCount];
    namenodeAddrs = new String[namenodeCount];
    namenodeAnswers = new NameNodeAnswer[namenodeCount];
    ClientProtocol[] proxies = new ClientProtocol[namenodeCount];
    Map<String, ClientProtocol> proxyMap = new HashMap<>();
    for (int i  = 0; i < namenodeCount; i++) {
      namenodeIDs[i] = "nn" + i;
      namenodeAddrs[i] = "namenode" + i + ".test:8020";
      conf.set(HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns +
          "." + namenodeIDs[i], namenodeAddrs[i]);
      namenodeAnswers[i] = new NameNodeAnswer();
      proxies[i] = mock(ClientProtocol.class);
      doWrite(Mockito.doAnswer(namenodeAnswers[i].clientAnswer)
          .when(proxies[i]));
      doRead(Mockito.doAnswer(namenodeAnswers[i].clientAnswer)
          .when(proxies[i]));
      Mockito.doAnswer(namenodeAnswers[i].clientAnswer)
          .when(proxies[i]).getHAServiceState();
      proxyMap.put(namenodeAddrs[i], proxies[i]);
    }
    conf.set(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns,
        Joiner.on(",").join(namenodeIDs));
    conf.set(HdfsClientConfigKeys.DFS_NAMESERVICES, ns);
    // Set observer probe retry period to 0. Required by the tests that
    // transition observer back and forth
    conf.setTimeDuration(
        OBSERVER_PROBE_RETRY_PERIOD_KEY, 0, TimeUnit.MILLISECONDS);
    conf.setBoolean(HdfsClientConfigKeys.Failover.RANDOM_ORDER, false);
    proxyProvider = new ObserverReadProxyProvider<ClientProtocol>(conf, nnURI,
        ClientProtocol.class,
        new ClientHAProxyFactory<ClientProtocol>() {
          @Override
          public ClientProtocol createProxy(Configuration config,
              InetSocketAddress nnAddr, Class<ClientProtocol> xface,
              UserGroupInformation ugi, boolean withRetries,
              AtomicBoolean fallbackToSimpleAuth) {
            return proxyMap.get(nnAddr.toString());
          }
        })  {
      @Override
      protected List<NNProxyInfo<ClientProtocol>> getProxyAddresses(
          URI uri, String addressKey) {
        List<NNProxyInfo<ClientProtocol>> nnProxies =
            super.getProxyAddresses(uri, addressKey);
        return nnProxies;
      }
    };
    proxyProvider.setObserverReadEnabled(true);
  }

  @Test
  public void testWithNonClientProxy() throws Exception {
    setupProxyProvider(2); // This will initialize all of the instance fields
    final String fakeUser = "fakeUser";
    final String[] fakeGroups = {"fakeGroup"};
    HAProxyFactory<GetUserMappingsProtocol> proxyFactory =
        new NameNodeHAProxyFactory<GetUserMappingsProtocol>() {
          @Override
          public GetUserMappingsProtocol createProxy(Configuration config,
              InetSocketAddress addr, Class<GetUserMappingsProtocol> xface,
              UserGroupInformation ugi, boolean withRetries,
              AtomicBoolean fallbackToSimpleAuth) throws IOException {
            GetUserMappingsProtocol proxy =
                mock(GetUserMappingsProtocol.class);
            when(proxy.getGroupsForUser(fakeUser)).thenReturn(fakeGroups);
            return proxy;
          }
        };
    ObserverReadProxyProvider<GetUserMappingsProtocol> userProxyProvider =
        new ObserverReadProxyProvider<>(proxyProvider.conf, nnURI,
            GetUserMappingsProtocol.class, proxyFactory);
    assertArrayEquals(fakeGroups,
        userProxyProvider.getProxy().proxy.getGroupsForUser(fakeUser));
  }

  @Test
  public void testReadOperationOnObserver() throws Exception {
    setupProxyProvider(3);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[2].setObserverState();

    doRead();
    assertHandledBy(2);
  }

  @Test
  public void testWriteOperationOnActive() throws Exception {
    setupProxyProvider(3);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[2].setObserverState();

    doWrite();
    assertHandledBy(0);
  }

  @Test
  public void testUnreachableObserverWithNoBackup() throws Exception {
    setupProxyProvider(2);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();

    namenodeAnswers[1].setUnreachable(true);
    // Confirm that read still succeeds even though observer is not available
    doRead();
    assertHandledBy(0);
  }

  @Test
  public void testUnreachableObserverWithMultiple() throws Exception {
    setupProxyProvider(4);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[2].setObserverState();
    namenodeAnswers[3].setObserverState();

    doRead();
    assertHandledBy(2);

    namenodeAnswers[2].setUnreachable(true);
    doRead();
    // Fall back to the second observer node
    assertHandledBy(3);

    namenodeAnswers[2].setUnreachable(false);
    doRead();
    // Current index has changed, so although the first observer is back,
    // it should continue requesting from the second observer
    assertHandledBy(3);

    namenodeAnswers[3].setUnreachable(true);
    doRead();
    // Now that second is unavailable, go back to using the first observer
    assertHandledBy(2);

    namenodeAnswers[2].setUnreachable(true);
    doRead();
    // Both observers are now unavailable, so it should fall back to active
    assertHandledBy(0);
  }

  @Test
  public void testObserverToActive() throws Exception {
    setupProxyProvider(3);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();
    namenodeAnswers[2].setObserverState();

    doWrite();
    assertHandledBy(0);

    // Transition an observer to active
    namenodeAnswers[0].setStandbyState();
    namenodeAnswers[1].setActiveState();
    try {
      doWrite();
      fail("Write should fail; failover required");
    } catch (RemoteException re) {
      assertEquals(re.getClassName(),
          StandbyException.class.getCanonicalName());
    }
    proxyProvider.performFailover(proxyProvider.getProxy().proxy);
    doWrite();
    // After failover, previous observer is now active
    assertHandledBy(1);
    doRead();
    assertHandledBy(2);

    // Transition back to original state but second observer not available
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();
    namenodeAnswers[2].setUnreachable(true);
    for (int i = 0; i < 2; i++) {
      try {
        doWrite();
        fail("Should have failed");
      } catch (IOException ioe) {
        proxyProvider.performFailover(proxyProvider.getProxy().proxy);
      }
    }
    doWrite();
    assertHandledBy(0);

    doRead();
    assertHandledBy(1);
  }

  @Test
  public void testObserverToStandby() throws Exception {
    setupProxyProvider(3);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();
    namenodeAnswers[2].setObserverState();

    doRead();
    assertHandledBy(1);

    namenodeAnswers[1].setStandbyState();
    doRead();
    assertHandledBy(2);

    namenodeAnswers[2].setStandbyState();
    doRead();
    assertHandledBy(0);

    namenodeAnswers[1].setObserverState();
    doRead();
    assertHandledBy(1);
  }

  @Test
  public void testSingleObserverToStandby() throws Exception {
    setupProxyProvider(2);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();

    doRead();
    assertHandledBy(1);

    namenodeAnswers[1].setStandbyState();
    doRead();
    assertHandledBy(0);

    namenodeAnswers[1].setObserverState();
    // The proxy provider still thinks the second NN is in observer state,
    // so it will take a second call for it to notice the new observer
    doRead();
    doRead();
    assertHandledBy(1);
  }

  @Test
  public void testObserverRetriableException() throws Exception {
    setupProxyProvider(3);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setObserverState();
    namenodeAnswers[2].setObserverState();

    // Set the first observer to throw "ObserverRetryOnActiveException" so that
    // the request should skip the second observer and be served by the active.
    namenodeAnswers[1].setRetryActive(true);

    doRead();
    assertHandledBy(0);

    namenodeAnswers[1].setRetryActive(false);

    doRead();
    assertHandledBy(1);
  }

  /**
   * Happy case for GetHAServiceStateWithTimeout.
   */
  @Test
  public void testGetHAServiceStateWithTimeout() throws Exception {
    proxyLog.clearOutput();

    setupProxyProvider(1, NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT);
    final HAServiceState state = HAServiceState.STANDBY;
    @SuppressWarnings("unchecked")
    NNProxyInfo<ClientProtocol> dummyNNProxyInfo =
        (NNProxyInfo<ClientProtocol>) mock(NNProxyInfo.class);
    @SuppressWarnings("unchecked")
    Future<HAServiceState> task = mock(Future.class);
    when(task.get(anyLong(), any(TimeUnit.class))).thenReturn(state);

    HAServiceState state2 = proxyProvider.getHAServiceStateWithTimeout(dummyNNProxyInfo, task);
    assertEquals(state, state2);
    verify(task).get(anyLong(), any(TimeUnit.class));
    verifyNoMoreInteractions(task);
    assertEquals(1, StringUtils.countMatches(proxyLog.getOutput(),
        "HA State for " + dummyNNProxyInfo.proxyInfo + " is " + state));
    proxyLog.clearOutput();
  }

  /**
   * Test TimeoutException for GetHAServiceStateWithTimeout.
   */
  @Test
  public void testTimeoutExceptionGetHAServiceStateWithTimeout() throws Exception {
    proxyLog.clearOutput();

    setupProxyProvider(1, NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT);
    @SuppressWarnings("unchecked")
    NNProxyInfo<ClientProtocol> dummyNNProxyInfo =
        (NNProxyInfo<ClientProtocol>) Mockito.mock(NNProxyInfo.class);
    @SuppressWarnings("unchecked")
    Future<HAServiceState> task = mock(Future.class);
    TimeoutException e = new TimeoutException("Timeout");
    when(task.get(anyLong(), any(TimeUnit.class))).thenThrow(e);

    HAServiceState state = proxyProvider.getHAServiceStateWithTimeout(dummyNNProxyInfo, task);
    assertNull(state);
    verify(task).get(anyLong(), any(TimeUnit.class));
    verify(task).cancel(true);
    verifyNoMoreInteractions(task);
    assertEquals(1, StringUtils.countMatches(proxyLog.getOutput(),
        "Cancel NN probe task due to timeout for " + dummyNNProxyInfo.proxyInfo));
    proxyLog.clearOutput();
  }

  /**
   * Test InterruptedException for GetHAServiceStateWithTimeout.
   */
  @Test
  public void testInterruptedExceptionGetHAServiceStateWithTimeout() throws Exception {
    proxyLog.clearOutput();

    setupProxyProvider(1, NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT);
    @SuppressWarnings("unchecked")
    NNProxyInfo<ClientProtocol> dummyNNProxyInfo =
        (NNProxyInfo<ClientProtocol>) Mockito.mock(NNProxyInfo.class);
    @SuppressWarnings("unchecked")
    Future<HAServiceState> task = mock(Future.class);
    InterruptedException e = new InterruptedException("Interrupted");
    when(task.get(anyLong(), any(TimeUnit.class))).thenThrow(e);

    HAServiceState state = proxyProvider.getHAServiceStateWithTimeout(dummyNNProxyInfo, task);
    assertNull(state);
    verify(task).get(anyLong(), any(TimeUnit.class));
    verifyNoMoreInteractions(task);
    assertEquals(1, StringUtils.countMatches(proxyLog.getOutput(),
        "Exception in NN probe task for " + dummyNNProxyInfo.proxyInfo));
    proxyLog.clearOutput();
  }

  /**
   * Test ExecutionException for GetHAServiceStateWithTimeout.
   */
  @Test
  public void testExecutionExceptionGetHAServiceStateWithTimeout() throws Exception {
    proxyLog.clearOutput();

    setupProxyProvider(1, NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT);
    @SuppressWarnings("unchecked")
    NNProxyInfo<ClientProtocol> dummyNNProxyInfo =
        (NNProxyInfo<ClientProtocol>) Mockito.mock(NNProxyInfo.class);
    @SuppressWarnings("unchecked")
    Future<HAServiceState> task = mock(Future.class);
    Exception e = new ExecutionException(new InterruptedException("Interrupted"));
    when(task.get(anyLong(), any(TimeUnit.class))).thenThrow(e);

    HAServiceState state = proxyProvider.getHAServiceStateWithTimeout(dummyNNProxyInfo, task);
    assertNull(state);
    verify(task).get(anyLong(), any(TimeUnit.class));
    verifyNoMoreInteractions(task);
    assertEquals(1, StringUtils.countMatches(proxyLog.getOutput(),
        "Exception in NN probe task for " + dummyNNProxyInfo.proxyInfo));
    proxyLog.clearOutput();
  }

  /**
   * Test GetHAServiceState when timeout is disabled (test the else { task.get() } code path).
   */
  @Test
  public void testGetHAServiceStateWithoutTimeout() throws Exception {
    proxyLog.clearOutput();
    setupProxyProvider(1, 0);

    final HAServiceState state = HAServiceState.STANDBY;
    @SuppressWarnings("unchecked")
    NNProxyInfo<ClientProtocol> dummyNNProxyInfo =
        (NNProxyInfo<ClientProtocol>) mock(NNProxyInfo.class);
    @SuppressWarnings("unchecked")
    Future<HAServiceState> task = mock(Future.class);
    when(task.get()).thenReturn(state);

    HAServiceState state2 = proxyProvider.getHAServiceStateWithTimeout(dummyNNProxyInfo, task);
    assertEquals(state, state2);
    verify(task).get();
    verifyNoMoreInteractions(task);
    assertEquals(1, StringUtils.countMatches(proxyLog.getOutput(),
        "HA State for " + dummyNNProxyInfo.proxyInfo + " is " + state));
    proxyLog.clearOutput();
  }

  /**
   * Test getHAServiceState when we have a slow NN, using a 25s timeout.
   * This is to verify the old behavior without being able to fast-fail (we can also set
   * namenodeHAStateProbeTimeoutMs to 0 or a negative value and the rest of the test can stay
   * the same).
   *
   * 5-second (SLOW_RESPONSE_SLEEP_TIME) latency is introduced and we expect that latency is added
   * to the READ operation.
   */
  @Test
  public void testStandbyGetHAServiceStateLongTimeout() throws Exception {
    setupProxyProvider(4, NAMENODE_HA_STATE_PROBE_TIMEOUT_LONG);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setSlowNode(true);
    namenodeAnswers[3].setObserverState();

    StopWatch watch = new StopWatch();
    watch.start();
    doRead();
    long runtime = watch.now(TimeUnit.MILLISECONDS);
    assertTrue("Read operation finished earlier than we expected",
        runtime > SLOW_RESPONSE_SLEEP_TIME);
  }

  /**
   * Test getHAServiceState using a 2s timeout with a slow standby.
   * Fail the test if we don't complete it in 4s.
   */
  @Test(timeout = 4000)
  public void testStandbyGetHAServiceStateTimeout() throws Exception {
    setupProxyProvider(4, NAMENODE_HA_STATE_PROBE_TIMEOUT_SHORT);
    namenodeAnswers[0].setActiveState();
    namenodeAnswers[1].setSlowNode(true);
    namenodeAnswers[3].setObserverState();

    doRead();
  }

  private void doRead() throws Exception {
    doRead(proxyProvider.getProxy().proxy);
  }

  private void doWrite() throws Exception {
    doWrite(proxyProvider.getProxy().proxy);
  }

  private void assertHandledBy(int namenodeIdx) {
    assertEquals(namenodeAddrs[namenodeIdx],
        proxyProvider.getLastProxy().proxyInfo);
  }

  private static void doWrite(ClientProtocol client) throws Exception {
    client.reportBadBlocks(EMPTY_BLOCKS);
  }

  private static void doRead(ClientProtocol client) throws Exception {
    client.checkAccess("/", FsAction.READ);
  }

  /**
   * An {@link Answer} used for mocking of {@link ClientProtocol}.
   * Setting the state or unreachability of this
   * Answer will make the linked ClientProtocol respond as if it was
   * communicating with a NameNode of the corresponding state. It is in Standby
   * state by default.
   */
  private static class NameNodeAnswer {

    private volatile boolean unreachable = false;
    private volatile boolean retryActive = false;
    private volatile boolean slowNode = false;

    // Standby state by default
    private volatile boolean allowWrites = false;
    private volatile boolean allowReads = false;

    private ClientProtocolAnswer clientAnswer = new ClientProtocolAnswer();

    private class ClientProtocolAnswer implements Answer<Object> {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (unreachable) {
          throw new IOException("Unavailable");
        }

        // sleep to simulate slow rpc responses.
        if (slowNode) {
          Thread.sleep(SLOW_RESPONSE_SLEEP_TIME);
        }

        // retryActive should be checked before getHAServiceState.
        // Check getHAServiceState first here only because in test,
        // it relies read call, which relies on getHAServiceState
        // to have passed already. May revisit future.
        if (invocationOnMock.getMethod()
            .getName().equals("getHAServiceState")) {
          HAServiceState status;
          if (allowReads && allowWrites) {
            status = HAServiceState.ACTIVE;
          } else if (allowReads) {
            status = HAServiceState.OBSERVER;
          } else {
            status = HAServiceState.STANDBY;
          }
          return status;
        }
        if (retryActive) {
          throw new RemoteException(
              ObserverRetryOnActiveException.class.getCanonicalName(),
              "Try active!"
          );
        }
        switch (invocationOnMock.getMethod().getName()) {
        case "reportBadBlocks":
          if (!allowWrites) {
            throw new RemoteException(
                StandbyException.class.getCanonicalName(), "No writes!");
          }
          return null;
        case "checkAccess":
          if (!allowReads) {
            throw new RemoteException(
                StandbyException.class.getCanonicalName(), "No reads!");
          }
          return null;
        default:
          throw new IllegalArgumentException(
              "Only reportBadBlocks and checkAccess supported!");
        }
      }
    }

    void setUnreachable(boolean unreachable) {
      this.unreachable = unreachable;
    }

    // Whether this node should be slow in rpc response.
    void setSlowNode(boolean slowNode) {
      this.slowNode = slowNode;
    }

    void setActiveState() {
      allowReads = true;
      allowWrites = true;
    }

    void setStandbyState() {
      allowReads = false;
      allowWrites = false;
    }

    void setObserverState() {
      allowReads = true;
      allowWrites = false;
    }

    void setRetryActive(boolean shouldRetryActive) {
      retryActive = shouldRetryActive;
    }
  }

}
