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
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * One Util class to mock QJM for some UTs not in this package.
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
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, String nameServiceId, InetSocketAddress addr) {
        AsyncLogger logger = new IPCLoggerChannel(conf, nsInfo, journalId,
            nameServiceId, addr) {
          protected ExecutorService createSingleThreadExecutor() {
            // Don't parallelize calls to the quorum in the tests.
            // This makes the tests more deterministic.
            return new DirectExecutorService();
          }
        };
        return Mockito.spy(logger);
      }
    };
    return new QuorumJournalManager(conf, uri, nsInfo, nameServiceId, spyFactory);
  }

  /**
   * Mock Journals with different response for getJournaledEdits rpc with the input startTxid.
   * 1. First journal with one empty response.
   * 2. Second journal with one normal response.
   * 3. Third journal with one slow response.
   * @param manager input QuorumJournalManager.
   * @param startTxid input start txid.
   */
  public static void mockJNWithEmptyOrSlowResponse(QuorumJournalManager manager, long startTxid) {
    List<AsyncLogger> spies = manager.getLoggerSetForTests().getLoggersForTests();
    Semaphore semaphore = new Semaphore(0);

    // Mock JN0 return an empty response.
    Mockito.doAnswer(invocation -> {
      semaphore.release();
      return GetJournaledEditsResponseProto.newBuilder().setTxnCount(0).build();
    }).when(spies.get(0))
        .getJournaledEdits(startTxid, QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);

    // Mock JN1 return a normal response.
    spyGetJournaledEdits(spies, 1, startTxid, () -> semaphore.release(1));

    // Mock JN2 return a slow response
    spyGetJournaledEdits(spies, 2, startTxid, () -> semaphore.acquireUninterruptibly(2));
  }

  public static void spyGetJournaledEdits(List<AsyncLogger> spies,
      int jnSpyIdx, long fromTxId, Runnable preHook) {
    Mockito.doAnswer((Answer<ListenableFuture<GetJournaledEditsResponseProto>>) invocation -> {
      preHook.run();
      @SuppressWarnings("unchecked")
      ListenableFuture<GetJournaledEditsResponseProto> result =
          (ListenableFuture<GetJournaledEditsResponseProto>) invocation.callRealMethod();
      return result;
    }).when(spies.get(jnSpyIdx)).getJournaledEdits(fromTxId,
        QuorumJournalManager.QJM_RPC_MAX_TXNS_DEFAULT);
  }
}
