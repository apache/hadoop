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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeOp;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.createTxnData;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.verifyEdits;

/**
 * True unit tests for QuorumJournalManager
 */
public class TestQuorumJournalManagerUnit {
  static {
    GenericTestUtils.setLogLevel(QuorumJournalManager.LOG, Level.TRACE);
  }
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L);

  private final Configuration conf = new Configuration();
  private List<AsyncLogger> spyLoggers;
  private QuorumJournalManager qjm;
  
  @Before
  public void setup() throws Exception {
    spyLoggers = ImmutableList.of(
        mockLogger(),
        mockLogger(),
        mockLogger());

    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    qjm = new QuorumJournalManager(conf, new URI("qjournal://host/jid"), FAKE_NSINFO) {
      @Override
      protected List<AsyncLogger> createLoggers(AsyncLogger.Factory factory) {
        return spyLoggers;
      }
    };

    for (AsyncLogger logger : spyLoggers) {
      futureReturns(GetJournalStateResponseProto.newBuilder()
          .setLastPromisedEpoch(0)
          .setHttpPort(-1)
          .build())
        .when(logger).getJournalState();
      
      futureReturns(
          NewEpochResponseProto.newBuilder().build()
          ).when(logger).newEpoch(Mockito.anyLong());
      
      futureReturns(null).when(logger).format(Mockito.<NamespaceInfo>any(),
          anyBoolean());
    }
    
    qjm.recoverUnfinalizedSegments();
  }
  
  private AsyncLogger mockLogger() {
    return Mockito.mock(AsyncLogger.class);
  }
  
  static <V> Stubber futureReturns(V value) {
    ListenableFuture<V> ret = Futures.immediateFuture(value);
    return Mockito.doReturn(ret);
  }
  
  static Stubber futureThrows(Throwable t) {
    ListenableFuture<?> ret = Futures.immediateFailedFuture(t);
    return Mockito.doReturn(ret);
  }


  @Test
  public void testAllLoggersStartOk() throws Exception {
    futureReturns(null).when(spyLoggers.get(0)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(1)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(2)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    qjm.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  @Test
  public void testQuorumOfLoggersStartOk() throws Exception {
    futureReturns(null).when(spyLoggers.get(0)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(1)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureThrows(new IOException("logger failed"))
      .when(spyLoggers.get(2)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    qjm.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  @Test
  public void testQuorumOfLoggersFail() throws Exception {
    futureReturns(null).when(spyLoggers.get(0)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureThrows(new IOException("logger failed"))
    .when(spyLoggers.get(1)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureThrows(new IOException("logger failed"))
      .when(spyLoggers.get(2)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    try {
      qjm.startLogSegment(1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      fail("Did not throw when quorum failed");
    } catch (QuorumException qe) {
      GenericTestUtils.assertExceptionContains("logger failed", qe);
    }
  }
  
  @Test
  public void testQuorumOutputStreamReport() throws Exception {
    futureReturns(null).when(spyLoggers.get(0)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(1)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(2)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    QuorumOutputStream os = (QuorumOutputStream) qjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    String report = os.generateReport();
    Assert.assertFalse("Report should be plain text", report.contains("<"));
  }

  @Test
  public void testWriteEdits() throws Exception {
    EditLogOutputStream stm = createLogSegment();
    writeOp(stm, 1);
    writeOp(stm, 2);
    
    stm.setReadyToFlush();
    writeOp(stm, 3);
    
    // The flush should log txn 1-2
    futureReturns(null).when(spyLoggers.get(0)).sendEdits(
        anyLong(), eq(1L), eq(2), Mockito.<byte[]>any());
    futureReturns(null).when(spyLoggers.get(1)).sendEdits(
        anyLong(), eq(1L), eq(2), Mockito.<byte[]>any());
    futureReturns(null).when(spyLoggers.get(2)).sendEdits(
        anyLong(), eq(1L), eq(2), Mockito.<byte[]>any());
    stm.flush();

    // Another flush should now log txn #3
    stm.setReadyToFlush();
    futureReturns(null).when(spyLoggers.get(0)).sendEdits(
        anyLong(), eq(3L), eq(1), Mockito.<byte[]>any());
    futureReturns(null).when(spyLoggers.get(1)).sendEdits(
        anyLong(), eq(3L), eq(1), Mockito.<byte[]>any());
    futureReturns(null).when(spyLoggers.get(2)).sendEdits(
        anyLong(), eq(3L), eq(1), Mockito.<byte[]>any());
    stm.flush();
  }
  
  @Test
  public void testWriteEditsOneSlow() throws Exception {
    EditLogOutputStream stm = createLogSegment();
    writeOp(stm, 1);
    stm.setReadyToFlush();
    
    // Make the first two logs respond immediately
    futureReturns(null).when(spyLoggers.get(0)).sendEdits(
        anyLong(), eq(1L), eq(1), Mockito.<byte[]>any());
    futureReturns(null).when(spyLoggers.get(1)).sendEdits(
        anyLong(), eq(1L), eq(1), Mockito.<byte[]>any());
    
    // And the third log not respond
    SettableFuture<Void> slowLog = SettableFuture.create();
    Mockito.doReturn(slowLog).when(spyLoggers.get(2)).sendEdits(
        anyLong(), eq(1L), eq(1), Mockito.<byte[]>any());
    stm.flush();
    
    Mockito.verify(spyLoggers.get(0)).setCommittedTxId(1L);
  }

  @Test
  public void testReadRpcInputStreams() throws Exception {
    for (int jn = 0; jn < 3; jn++) {
      futureReturns(getJournaledEditsReponse(1, 3))
          .when(spyLoggers.get(jn)).getJournaledEdits(1,
          QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
    }

    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 1, true, true);
    assertEquals(1, streams.size());
    verifyEdits(streams, 1, 3);
  }

  @Test
  public void testReadRpcMismatchedInputStreams() throws Exception {
    for (int jn = 0; jn < 3; jn++) {
      futureReturns(getJournaledEditsReponse(1, jn + 1))
          .when(spyLoggers.get(jn)).getJournaledEdits(1,
          QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
    }

    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 1, true, true);
    assertEquals(1, streams.size());
    verifyEdits(streams, 1, 2);
  }

  @Test
  public void testReadRpcInputStreamsOneSlow() throws Exception {
    for (int jn = 0; jn < 2; jn++) {
      futureReturns(getJournaledEditsReponse(1, jn + 1))
          .when(spyLoggers.get(jn)).getJournaledEdits(1,
          QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
    }
    Mockito.doReturn(SettableFuture.create())
        .when(spyLoggers.get(2)).getJournaledEdits(1,
        QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);

    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 1, true, true);
    assertEquals(1, streams.size());
    verifyEdits(streams, 1, 1);
  }

  @Test
  public void testReadRpcInputStreamsOneException() throws Exception {
    for (int jn = 0; jn < 2; jn++) {
      futureReturns(getJournaledEditsReponse(1, jn + 1))
          .when(spyLoggers.get(jn)).getJournaledEdits(1,
          QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
    }
    futureThrows(new IOException()).when(spyLoggers.get(2))
        .getJournaledEdits(1, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);

    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 1, true, true);
    assertEquals(1, streams.size());
    verifyEdits(streams, 1, 1);
  }

  @Test
  public void testReadRpcInputStreamsNoNewEdits() throws Exception {
    for (int jn = 0; jn < 3; jn++) {
      futureReturns(GetJournaledEditsResponseProto.newBuilder()
          .setTxnCount(0).setEditLog(ByteString.EMPTY).build())
          .when(spyLoggers.get(jn))
          .getJournaledEdits(1, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
    }

    List<EditLogInputStream> streams = Lists.newArrayList();
    qjm.selectInputStreams(streams, 1, true, true);
    assertEquals(0, streams.size());
  }

  private GetJournaledEditsResponseProto getJournaledEditsReponse(
      int startTxn, int numTxns) throws Exception {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    EditLogFileOutputStream.writeHeader(
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
        new DataOutputStream(byteStream));
    byteStream.write(createTxnData(startTxn, numTxns));
    return GetJournaledEditsResponseProto.newBuilder()
        .setTxnCount(numTxns)
        .setEditLog(ByteString.copyFrom(byteStream.toByteArray()))
        .build();
  }

  private EditLogOutputStream createLogSegment() throws IOException {
    futureReturns(null).when(spyLoggers.get(0)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(1)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    futureReturns(null).when(spyLoggers.get(2)).startLogSegment(Mockito.anyLong(),
        Mockito.eq(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION));
    EditLogOutputStream stm = qjm.startLogSegment(1,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    return stm;
  }
}
