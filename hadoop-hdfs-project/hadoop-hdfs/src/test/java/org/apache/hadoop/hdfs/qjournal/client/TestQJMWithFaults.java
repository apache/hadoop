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
package org.apache.hadoop.hdfs.qjournal.client;

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeSegment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.server.JournalFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class TestQJMWithFaults {
  private static final Log LOG = LogFactory.getLog(
      TestQJMWithFaults.class);

  private static final String RAND_SEED_PROPERTY =
      "TestQJMWithFaults.random-seed";

  private static final int NUM_WRITER_ITERS = 500;
  private static final int SEGMENTS_PER_WRITER = 2;

  private static final Configuration conf = new Configuration();


  static {
    // Don't retry connections - it just slows down the tests.
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    
    // Make tests run faster by avoiding fsync()
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  // Set up fault injection mock.
  private static final JournalFaultInjector faultInjector =
      JournalFaultInjector.instance = Mockito.mock(JournalFaultInjector.class); 

  /**
   * Run through the creation of a log without any faults injected,
   * and count how many RPCs are made to each node. This sets the
   * bounds for the other test cases, so they can exhaustively explore
   * the space of potential failures.
   */
  private static long determineMaxIpcNumber() throws Exception {
    Configuration conf = new Configuration();
    MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).build();
    cluster.waitActive();
    QuorumJournalManager qjm = null;
    long ret;
    try {
      qjm = createInjectableQJM(cluster);
      qjm.format(FAKE_NSINFO);
      doWorkload(cluster, qjm);
      
      SortedSet<Integer> ipcCounts = Sets.newTreeSet();
      for (AsyncLogger l : qjm.getLoggerSetForTests().getLoggersForTests()) {
        InvocationCountingChannel ch = (InvocationCountingChannel)l;
        ch.waitForAllPendingCalls();
        ipcCounts.add(ch.getRpcCount());
      }
  
      // All of the loggers should have sent the same number of RPCs, since there
      // were no failures.
      assertEquals(1, ipcCounts.size());
      
      ret = ipcCounts.first();
      LOG.info("Max IPC count = " + ret);
    } finally {
      IOUtils.closeStream(qjm);
      cluster.shutdown();
    }
    return ret;
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Sets up two of the nodes to each drop a single RPC, at all
   * possible combinations of RPCs. This may result in the
   * active writer failing to write. After this point, a new writer
   * should be able to recover and continue writing without
   * data loss.
   */
  @Test
  public void testRecoverAfterDoubleFailures() throws Exception {
    final long MAX_IPC_NUMBER = determineMaxIpcNumber();
    
    for (int failA = 1; failA <= MAX_IPC_NUMBER; failA++) {
      for (int failB = 1; failB <= MAX_IPC_NUMBER; failB++) {
        String injectionStr = "(" + failA + ", " + failB + ")";
        
        LOG.info("\n\n-------------------------------------------\n" +
            "Beginning test, failing at " + injectionStr + "\n" +
            "-------------------------------------------\n\n");
        
        MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf)
          .build();
        cluster.waitActive();
        QuorumJournalManager qjm = null;
        try {
          qjm = createInjectableQJM(cluster);
          qjm.format(FAKE_NSINFO);
          List<AsyncLogger> loggers = qjm.getLoggerSetForTests().getLoggersForTests();
          failIpcNumber(loggers.get(0), failA);
          failIpcNumber(loggers.get(1), failB);
          int lastAckedTxn = doWorkload(cluster, qjm);

          if (lastAckedTxn < 6) {
            LOG.info("Failed after injecting failures at " + injectionStr + 
                ". This is expected since we injected a failure in the " +
                "majority.");
          }
          qjm.close();
          qjm = null;

          // Now should be able to recover
          qjm = createInjectableQJM(cluster);
          long lastRecoveredTxn = QJMTestUtil.recoverAndReturnLastTxn(qjm);
          assertTrue(lastRecoveredTxn >= lastAckedTxn);
          
          writeSegment(cluster, qjm, lastRecoveredTxn + 1, 3, true);
        } catch (Throwable t) {
          // Test failure! Rethrow with the test setup info so it can be
          // easily triaged.
          throw new RuntimeException("Test failed with injection: " + injectionStr,
                t); 
        } finally {
          cluster.shutdown();
          cluster = null;
          IOUtils.closeStream(qjm);
          qjm = null;
        }
      }
    }
  }
  
  /**
   * Expect {@link UnknownHostException} if a hostname can't be resolved.
   */
  @Test
  public void testUnresolvableHostName() throws Exception {
    expectedException.expect(UnknownHostException.class);
    new QuorumJournalManager(conf,
        new URI("qjournal://" + "bogus:12345" + "/" + JID), FAKE_NSINFO);
  }

  /**
   * Test case in which three JournalNodes randomly flip flop between
   * up and down states every time they get an RPC.
   * 
   * The writer keeps track of the latest ACKed edit, and on every
   * recovery operation, ensures that it recovers at least to that
   * point or higher. Since at any given point, a majority of JNs
   * may be injecting faults, any writer operation is allowed to fail,
   * so long as the exception message indicates it failed due to injected
   * faults.
   * 
   * Given a random seed, the test should be entirely deterministic.
   */
  @Test
  public void testRandomized() throws Exception {
    long seed;
    Long userSpecifiedSeed = Long.getLong(RAND_SEED_PROPERTY);
    if (userSpecifiedSeed != null) {
      LOG.info("Using seed specified in system property");
      seed = userSpecifiedSeed;
      
      // If the user specifies a seed, then we should gather all the
      // IPC trace information so that debugging is easier. This makes
      // the test run about 25% slower otherwise.
      GenericTestUtils.setLogLevel(ProtobufRpcEngine.LOG, Level.ALL);
    } else {
      seed = new Random().nextLong();
    }
    LOG.info("Random seed: " + seed);
    
    Random r = new Random(seed);
    
    MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf)
      .build();
    cluster.waitActive();
    
    // Format the cluster using a non-faulty QJM.
    QuorumJournalManager qjmForInitialFormat =
        createInjectableQJM(cluster);
    qjmForInitialFormat.format(FAKE_NSINFO);
    qjmForInitialFormat.close();
    
    try {
      long txid = 0;
      long lastAcked = 0;
      
      for (int i = 0; i < NUM_WRITER_ITERS; i++) {
        LOG.info("Starting writer " + i + "\n-------------------");
        
        QuorumJournalManager qjm = createRandomFaultyQJM(cluster, r);
        try {
          long recovered;
          try {
            recovered = QJMTestUtil.recoverAndReturnLastTxn(qjm);
          } catch (Throwable t) {
            LOG.info("Failed recovery", t);
            checkException(t);
            continue;
          }
          assertTrue("Recovered only up to txnid " + recovered +
              " but had gotten an ack for " + lastAcked,
              recovered >= lastAcked);
          
          txid = recovered + 1;
          
          // Periodically purge old data on disk so it's easier to look
          // at failure cases.
          if (txid > 100 && i % 10 == 1) {
            qjm.purgeLogsOlderThan(txid - 100);
          }

          Holder<Throwable> thrown = new Holder<Throwable>(null);
          for (int j = 0; j < SEGMENTS_PER_WRITER; j++) {
            lastAcked = writeSegmentUntilCrash(cluster, qjm, txid, 4, thrown);
            if (thrown.held != null) {
              LOG.info("Failed write", thrown.held);
              checkException(thrown.held);
              break;
            }
            txid += 4;
          }
        } finally {
          qjm.close();
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  private void checkException(Throwable t) {
    GenericTestUtils.assertExceptionContains("Injected", t);
    if (t.toString().contains("AssertionError")) {
      throw new RuntimeException("Should never see AssertionError in fault test!",
          t);
    }
  }

  private long writeSegmentUntilCrash(MiniJournalCluster cluster,
      QuorumJournalManager qjm, long txid, int numTxns, Holder<Throwable> thrown) {
    
    long firstTxId = txid;
    long lastAcked = txid - 1;
    try {
      EditLogOutputStream stm = qjm.startLogSegment(txid,
          NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      
      for (int i = 0; i < numTxns; i++) {
        QJMTestUtil.writeTxns(stm, txid++, 1);
        lastAcked++;
      }
      
      stm.close();
      qjm.finalizeLogSegment(firstTxId, lastAcked);
    } catch (Throwable t) {
      thrown.held = t;
    }
    return lastAcked;
  }

  /**
   * Run a simple workload of becoming the active writer and writing
   * two log segments: 1-3 and 4-6.
   */
  private static int doWorkload(MiniJournalCluster cluster,
      QuorumJournalManager qjm) throws IOException {
    int lastAcked = 0;
    try {
      qjm.recoverUnfinalizedSegments();
      writeSegment(cluster, qjm, 1, 3, true);
      lastAcked = 3;
      writeSegment(cluster, qjm, 4, 3, true);
      lastAcked = 6;
    } catch (QuorumException qe) {
      LOG.info("Failed to write at txid " + lastAcked,
          qe);
    }
    return lastAcked;
  }

  /**
   * Inject a failure at the given IPC number, such that the JN never
   * receives the RPC. The client side sees an IOException. Future
   * IPCs after this number will be received as usual.
   */
  private void failIpcNumber(AsyncLogger logger, int idx) {
    ((InvocationCountingChannel)logger).failIpcNumber(idx);
  }
  
  private static class RandomFaultyChannel extends IPCLoggerChannel {
    private final Random random;
    private final float injectionProbability = 0.1f;
    private boolean isUp = true;
    
    public RandomFaultyChannel(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr, long seed) {
      super(conf, nsInfo, journalId, addr);
      this.random = new Random(seed);
    }

    @Override
    protected QJournalProtocol createProxy() throws IOException {
      QJournalProtocol realProxy = super.createProxy();
      return mockProxy(
          new WrapEveryCall<Object>(realProxy) {
            @Override
            void beforeCall(InvocationOnMock invocation) throws Exception {
              if (random.nextFloat() < injectionProbability) {
                isUp = !isUp;
                LOG.info("transitioned " + addr + " to " +
                    (isUp ? "up" : "down"));
              }
    
              if (!isUp) {
                throw new IOException("Injected - faking being down");
              }
              
              if (invocation.getMethod().getName().equals("acceptRecovery")) {
                if (random.nextFloat() < injectionProbability) {
                  Mockito.doThrow(new IOException(
                      "Injected - faking fault before persisting paxos data"))
                      .when(faultInjector).beforePersistPaxosData();
                } else if (random.nextFloat() < injectionProbability) {
                  Mockito.doThrow(new IOException(
                      "Injected - faking fault after persisting paxos data"))
                      .when(faultInjector).afterPersistPaxosData();
                }
              }
            }
            
            @Override
            public void afterCall(InvocationOnMock invocation, boolean succeeded) {
              Mockito.reset(faultInjector);
            }
          });
    }

    @Override
    protected ExecutorService createSingleThreadExecutor() {
      return new DirectExecutorService();
    }
  }

  private static class InvocationCountingChannel extends IPCLoggerChannel {
    private int rpcCount = 0;
    private final Map<Integer, Callable<Void>> injections = Maps.newHashMap();
    
    public InvocationCountingChannel(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr) {
      super(conf, nsInfo, journalId, addr);
    }
    
    int getRpcCount() {
      return rpcCount;
    }
    
    void failIpcNumber(final int idx) {
      Preconditions.checkArgument(idx > 0,
          "id must be positive");
      inject(idx, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          throw new IOException("injected failed IPC at " + idx);
        }
      });
    }
    
    private void inject(int beforeRpcNumber, Callable<Void> injectedCode) {
      injections.put(beforeRpcNumber, injectedCode);
    }

    @Override
    protected QJournalProtocol createProxy() throws IOException {
      final QJournalProtocol realProxy = super.createProxy();
      QJournalProtocol mock = mockProxy(
          new WrapEveryCall<Object>(realProxy) {
            void beforeCall(InvocationOnMock invocation) throws Exception {
              rpcCount++;

              String param="";
              for (Object val : invocation.getArguments()) {
                param += val +",";
              }
              String callStr = "[" + addr + "] " + 
                  invocation.getMethod().getName() + "(" +
                  param + ")";
 
              Callable<Void> inject = injections.get(rpcCount);
              if (inject != null) {
                LOG.info("Injecting code before IPC #" + rpcCount + ": " +
                    callStr);
                inject.call();
              } else {
                LOG.info("IPC call #" + rpcCount + ": " + callStr);
              }
            }
          });
      return mock;
    }
  }


  private static QJournalProtocol mockProxy(WrapEveryCall<Object> wrapper)
      throws IOException {
    QJournalProtocol mock = Mockito.mock(QJournalProtocol.class,
        Mockito.withSettings()
          .defaultAnswer(wrapper)
          .extraInterfaces(Closeable.class));
    return mock;
  }

  private static abstract class WrapEveryCall<T> implements Answer<T> {
    private final Object realObj;
    WrapEveryCall(Object realObj) {
      this.realObj = realObj;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T answer(InvocationOnMock invocation) throws Throwable {
      // Don't want to inject an error on close() since that isn't
      // actually an IPC call!
      if (!Closeable.class.equals(
            invocation.getMethod().getDeclaringClass())) {
        beforeCall(invocation);
      }
      boolean success = false;
      try {
        T ret = (T) invocation.getMethod().invoke(realObj,
          invocation.getArguments());
        success = true;
        return ret;
      } catch (InvocationTargetException ite) {
        throw ite.getCause();
      } finally {
        afterCall(invocation, success);
      }
    }

    abstract void beforeCall(InvocationOnMock invocation) throws Exception;
    void afterCall(InvocationOnMock invocation, boolean succeeded) {}
  }
  
  private static QuorumJournalManager createInjectableQJM(MiniJournalCluster cluster)
      throws IOException, URISyntaxException {
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, String nameserviceId, InetSocketAddress addr) {
        return new InvocationCountingChannel(conf, nsInfo, journalId, addr);
      }
    };
    return new QuorumJournalManager(conf, cluster.getQuorumJournalURI(JID),
        FAKE_NSINFO, spyFactory);
  }
  
  private static QuorumJournalManager createRandomFaultyQJM(
      MiniJournalCluster cluster, final Random seedGenerator)
          throws IOException, URISyntaxException {
    
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, String nameServiceId, InetSocketAddress addr) {
        return new RandomFaultyChannel(conf, nsInfo, journalId, addr,
            seedGenerator.nextLong());
      }
    };
    return new QuorumJournalManager(conf, cluster.getQuorumJournalURI(JID),
        FAKE_NSINFO, spyFactory);
  }

}
