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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.QuorumException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeSegment;


public class TestQJMWithFaults {
  private static final Log LOG = LogFactory.getLog(
      TestQJMWithFaults.class);

  private static Configuration conf = new Configuration();
  static {
    // Don't retry connections - it just slows down the tests.
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);    
  }
  private static long MAX_IPC_NUMBER;


  /**
   * Run through the creation of a log without any faults injected,
   * and count how many RPCs are made to each node. This sets the
   * bounds for the other test cases, so they can exhaustively explore
   * the space of potential failures.
   */
  @BeforeClass
  public static void determineMaxIpcNumber() throws Exception {
    Configuration conf = new Configuration();
    MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).build();
    try {
      QuorumJournalManager qjm = createInjectableQJM(cluster);
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
      
      MAX_IPC_NUMBER = ipcCounts.first();
      LOG.info("Max IPC count = " + MAX_IPC_NUMBER);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Sets up two of the nodes to each drop a single RPC, at all
   * possible combinations of RPCs. This may result in the
   * active writer failing to write. After this point, a new writer
   * should be able to recover and continue writing without
   * data loss.
   */
  @Test
  public void testRecoverAfterDoubleFailures() throws Exception {
    for (int failA = 1; failA <= MAX_IPC_NUMBER; failA++) {
      for (int failB = 1; failB <= MAX_IPC_NUMBER; failB++) {
        String injectionStr = "(" + failA + ", " + failB + ")";
        
        LOG.info("\n\n-------------------------------------------\n" +
            "Beginning test, failing at " + injectionStr + "\n" +
            "-------------------------------------------\n\n");
        
        MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf)
          .build();
        try {
          QuorumJournalManager qjm;
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

          // Now should be able to recover
          try {
            qjm = createInjectableQJM(cluster);
            qjm.recoverUnfinalizedSegments();
            writeSegment(cluster, qjm, lastAckedTxn + 1, 3, true);
            // TODO: verify log segments
          } catch (Throwable t) {
            // Test failure! Rethrow with the test setup info so it can be
            // easily triaged.
            throw new RuntimeException("Test failed with injection: " + injectionStr,
                t);
          }
        } finally {
          cluster.shutdown();
          cluster = null;
        }
      }
    }
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

  private static class InvocationCountingChannel extends IPCLoggerChannel {
    private int rpcCount = 0;
    private Map<Integer, Callable<Void>> injections = Maps.newHashMap();
    
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
      QJournalProtocol mock = Mockito.mock(QJournalProtocol.class,
          new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
              rpcCount++;
              String callStr = "[" + addr + "] " + 
                  invocation.getMethod().getName() + "(" +
                  Joiner.on(", ").join(invocation.getArguments()) + ")";
 
              Callable<Void> inject = injections.get(rpcCount);
              if (inject != null) {
                LOG.info("Injecting code before IPC #" + rpcCount + ": " +
                    callStr);
                inject.call();
              } else {
                LOG.info("IPC call #" + rpcCount + ": " + callStr);
              }

              return invocation.getMethod().invoke(realProxy,
                  invocation.getArguments());
            }
          });
      return mock;
    }
  }
  
  private static QuorumJournalManager createInjectableQJM(MiniJournalCluster cluster)
      throws IOException, URISyntaxException {
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, InetSocketAddress addr) {
        return new InvocationCountingChannel(conf, nsInfo, journalId, addr);
      }
    };
    return new QuorumJournalManager(conf, cluster.getQuorumJournalURI(JID),
        FAKE_NSINFO, spyFactory);
  }
}
