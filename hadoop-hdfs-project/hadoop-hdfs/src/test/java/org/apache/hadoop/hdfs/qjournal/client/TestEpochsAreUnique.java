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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


public class TestEpochsAreUnique {
  private static final Log LOG = LogFactory.getLog(TestEpochsAreUnique.class);
  private static final String JID = "testEpochsAreUnique-jid";
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);
  private final Random r = new Random();
  
  @Test
  public void testSingleThreaded() throws IOException {
    Configuration conf = new Configuration();
    MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).build();
    URI uri = cluster.getQuorumJournalURI(JID);
    QuorumJournalManager qjm = new QuorumJournalManager(
        conf, uri, FAKE_NSINFO);
    try {
      qjm.format(FAKE_NSINFO);
    } finally {
      qjm.close();
    }
    
    try {
      // With no failures or contention, epochs should increase one-by-one
      for (int i = 0; i < 5; i++) {
        qjm = new QuorumJournalManager(
            conf, uri, FAKE_NSINFO);
        try {
          qjm.createNewUniqueEpoch();
          assertEquals(i + 1, qjm.getLoggerSetForTests().getEpoch());
        } finally {
          qjm.close();
        }
      }
      
      long prevEpoch = 5;
      // With some failures injected, it should still always increase, perhaps
      // skipping some
      for (int i = 0; i < 20; i++) {
        long newEpoch = -1;
        while (true) {
          qjm = new QuorumJournalManager(
              conf, uri, FAKE_NSINFO, new FaultyLoggerFactory());
          try {
            qjm.createNewUniqueEpoch();
            newEpoch = qjm.getLoggerSetForTests().getEpoch();
            break;
          } catch (IOException ioe) {
            // It's OK to fail to create an epoch, since we randomly inject
            // faults. It's possible we'll inject faults in too many of the
            // underlying nodes, and a failure is expected in that case
          } finally {
            qjm.close();
          }
        }
        LOG.info("Created epoch " + newEpoch);
        assertTrue("New epoch " + newEpoch + " should be greater than previous " +
            prevEpoch, newEpoch > prevEpoch);
        prevEpoch = newEpoch;
      }
    } finally {
      cluster.shutdown();
    }
  }

  private class FaultyLoggerFactory implements AsyncLogger.Factory {
    @Override
    public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr) {
      AsyncLogger ch = IPCLoggerChannel.FACTORY.createLogger(
          conf, nsInfo, journalId, addr);
      AsyncLogger spy = Mockito.spy(ch);
      Mockito.doAnswer(new SometimesFaulty<Long>(0.10f))
          .when(spy).getJournalState();
      Mockito.doAnswer(new SometimesFaulty<Void>(0.40f))
          .when(spy).newEpoch(Mockito.anyLong());

      return spy;
    }
    
  }

  private class SometimesFaulty<T> implements Answer<ListenableFuture<T>> {
    private final float faultProbability;

    public SometimesFaulty(float faultProbability) {
      this.faultProbability = faultProbability;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<T> answer(InvocationOnMock invocation)
        throws Throwable {
      if (r.nextFloat() < faultProbability) {
        return Futures.immediateFailedFuture(
            new IOException("Injected fault"));
      }
      return (ListenableFuture<T>)invocation.callRealMethod();
    }
  }



}
