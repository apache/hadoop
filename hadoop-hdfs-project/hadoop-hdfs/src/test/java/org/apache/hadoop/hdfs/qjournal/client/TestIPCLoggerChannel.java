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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.LoggerTooFarBehindException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;

public class TestIPCLoggerChannel {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestIPCLoggerChannel.class);
  
  private final Configuration conf = new Configuration();
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);
  private static final String JID = "test-journalid";
  private static final InetSocketAddress FAKE_ADDR =
      new InetSocketAddress(0);
  private static final byte[] FAKE_DATA = new byte[4096];
  
  private final QJournalProtocol mockProxy = Mockito.mock(QJournalProtocol.class);
  private IPCLoggerChannel ch;
  
  private static final int LIMIT_QUEUE_SIZE_MB = 1;
  private static final int LIMIT_QUEUE_SIZE_BYTES =
      LIMIT_QUEUE_SIZE_MB * 1024 * 1024;
  
  @Before
  public void setupMock() {
    conf.setInt(DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        LIMIT_QUEUE_SIZE_MB);

    // Channel to the mock object instead of a real IPC proxy.
    ch = new IPCLoggerChannel(conf, FAKE_NSINFO, JID, FAKE_ADDR) {
      @Override
      protected QJournalProtocol getProxy() throws IOException {
        return mockProxy;
      }
    };
    
    ch.setEpoch(1);
  }
  
  @Test
  public void testSimpleCall() throws Exception {
    ch.sendEdits(1, 1, 3, FAKE_DATA).get();
    Mockito.verify(mockProxy).journal(Mockito.<RequestInfo>any(),
        Mockito.eq(1L), Mockito.eq(1L),
        Mockito.eq(3), Mockito.same(FAKE_DATA));
  }

  
  /**
   * Test that, once the queue eclipses the configure size limit,
   * calls to journal more data are rejected.
   */
  @Test
  public void testQueueLimiting() throws Exception {
    // Block the underlying fake proxy from actually completing any calls.
    DelayAnswer delayer = new DelayAnswer(LOG);
    Mockito.doAnswer(delayer).when(mockProxy).journal(
        Mockito.<RequestInfo>any(),
        Mockito.eq(1L), Mockito.eq(1L),
        Mockito.eq(1), Mockito.same(FAKE_DATA));
    
    // Queue up the maximum number of calls.
    int numToQueue = LIMIT_QUEUE_SIZE_BYTES / FAKE_DATA.length;
    for (int i = 1; i <= numToQueue; i++) {
      ch.sendEdits(1L, (long)i, 1, FAKE_DATA);
    }
    
    // The accounting should show the correct total number queued.
    assertEquals(LIMIT_QUEUE_SIZE_BYTES, ch.getQueuedEditsSize());
    
    // Trying to queue any more should fail.
    try {
      ch.sendEdits(1L, numToQueue + 1, 1, FAKE_DATA).get(1, TimeUnit.SECONDS);
      fail("Did not fail to queue more calls after queue was full");
    } catch (ExecutionException ee) {
      if (!(ee.getCause() instanceof LoggerTooFarBehindException)) {
        throw ee;
      }
    }
    
    delayer.proceed();

    // After we allow it to proceeed, it should chug through the original queue
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return ch.getQueuedEditsSize() == 0;
      }
    }, 10, 1000);
  }
  
  /**
   * Test that, if the remote node gets unsynchronized (eg some edits were
   * missed or the node rebooted), the client stops sending edits until
   * the next roll. Test for HDFS-3726.
   */
  @Test
  public void testStopSendingEditsWhenOutOfSync() throws Exception {
    Mockito.doThrow(new IOException("injected error"))
      .when(mockProxy).journal(
        Mockito.<RequestInfo>any(),
        Mockito.eq(1L), Mockito.eq(1L),
        Mockito.eq(1), Mockito.same(FAKE_DATA));

    try {
      ch.sendEdits(1L, 1L, 1, FAKE_DATA).get();
      fail("Injected JOOSE did not cause sendEdits() to throw");
    } catch (ExecutionException ee) {
      GenericTestUtils.assertExceptionContains("injected", ee);
    }
    Mockito.verify(mockProxy).journal(
        Mockito.<RequestInfo>any(),
        Mockito.eq(1L), Mockito.eq(1L),
        Mockito.eq(1), Mockito.same(FAKE_DATA));

    assertTrue(ch.isOutOfSync());
    
    try {
      ch.sendEdits(1L, 2L, 1, FAKE_DATA).get();
      fail("sendEdits() should throw until next roll");
    } catch (ExecutionException ee) {
      GenericTestUtils.assertExceptionContains("disabled until next roll",
          ee.getCause());
    }
    
    // It should have failed without even sending the edits, since it was not sync.
    Mockito.verify(mockProxy, Mockito.never()).journal(
        Mockito.<RequestInfo>any(),
        Mockito.eq(1L), Mockito.eq(2L),
        Mockito.eq(1), Mockito.same(FAKE_DATA));
    // It should have sent a heartbeat instead.
    Mockito.verify(mockProxy).heartbeat(
        Mockito.<RequestInfo>any());
    
    // After a roll, sending new edits should not fail.
    ch.startLogSegment(3L, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION).get();
    assertFalse(ch.isOutOfSync());

    ch.sendEdits(3L, 3L, 1, FAKE_DATA).get();
  }
}
