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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;

/**
 * One Util class to mock QJuournals for some UTs not in this package.
 */
public final class SpyQJournalUtil {

  private SpyQJournalUtil() {
  }

  /**
   * Mock a QuorumJournalManager with input uri, nsInfo and namServiceId.
   * @param conf input configuration.
   * @param uri input uri.
   * @param nsInfo input nameservice info.
   * @param nameServiceId input nameservice Id.
   * @return one mocked QuorumJournalManager.
   * @throws IOException throw IOException.
   */
  public static QuorumJournalManager createSpyingQJM(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId) throws IOException {
    AsyncLogger.Factory spyFactory = (conf1, nsInfo1, journalId1, nameServiceId1, addr1) -> {
      AsyncLogger logger = new IPCLoggerChannel(conf1, nsInfo1, journalId1, nameServiceId1, addr1);
      return Mockito.spy(logger);
    };
    return new QuorumJournalManager(conf, uri, nsInfo, nameServiceId, spyFactory);
  }

  /**
   * Try to mock one abnormal JournalNode with one empty response
   * for getJournaledEdits rpc with startTxid.
   * @param manager QuorumJournalmanager.
   * @param startTxid input StartTxid.
   */
  public static void mockOneJNReturnEmptyResponse(
      QuorumJournalManager manager, long startTxid, int journalIndex) {
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    // Mock JN0 return an empty response.
    GetJournaledEditsResponseProto responseProto = GetJournaledEditsResponseProto
        .newBuilder().setTxnCount(journalIndex).build();
    ListenableFuture<GetJournaledEditsResponseProto> ret = Futures.immediateFuture(responseProto);
    Mockito.doReturn(ret).when(spies.get(journalIndex))
        .getJournaledEdits(eq(startTxid), eq(QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT));
  }

  /**
   * Try to mock one abnormal JournalNode with slow response for
   * getJournaledEdits rpc with startTxid.
   * @param manager input QuormJournalManager.
   * @param startTxid input start txid.
   * @param sleepTime sleep time.
   * @param journalIndex the journal index need to be mocked.
   */
  public static void mockOneJNWithSlowResponse(
      QuorumJournalManager manager, long startTxid, int sleepTime, int journalIndex) {
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();

    ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor());
    Mockito.doAnswer(invocation -> service.submit(() -> {
      Thread.sleep(sleepTime);
      ListenableFuture<?> future = null;
      try {
        future = (ListenableFuture<?>) invocation.callRealMethod();
      } catch (Throwable e) {
        fail("getJournaledEdits failed " + e.getMessage());
      }
      return future.get();
    })).when(spies.get(journalIndex))
        .getJournaledEdits(startTxid, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
  }
}
