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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
@InterfaceAudience.Private
public class IPCLoggerChannel implements AsyncLogger {

  private final Configuration conf;
  protected final InetSocketAddress addr;
  private QJournalProtocol proxy;

  private final ListeningExecutorService executor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private final String journalId;
  private final NamespaceInfo nsInfo;
  private int httpPort = -1;
  
  /**
   * The number of bytes of edits data still in the queue.
   */
  private int queuedEditsSizeBytes = 0;

  /**
   * The maximum number of bytes that can be pending in the queue.
   * This keeps the writer from hitting OOME if one of the loggers
   * starts responding really slowly. Eventually, the queue
   * overflows and it starts to treat the logger as having errored.
   */
  private final int queueSizeLimitBytes;

  
  static final Factory FACTORY = new AsyncLogger.Factory() {
    @Override
    public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
        String journalId, InetSocketAddress addr) {
      return new IPCLoggerChannel(conf, nsInfo, journalId, addr);
    }
  };

  public IPCLoggerChannel(Configuration conf,
      NamespaceInfo nsInfo,
      String journalId,
      InetSocketAddress addr) {
    this.conf = conf;
    this.nsInfo = nsInfo;
    this.journalId = journalId;
    this.addr = addr;
    
    this.queueSizeLimitBytes = 1024 * 1024 * conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT);
    
    executor = MoreExecutors.listeningDecorator(
        createExecutor());
  }
  @Override
  public synchronized void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  @Override
  public void close() {
    // No more tasks may be submitted after this point.
    executor.shutdown();
    if (proxy != null) {
      // TODO: this can hang for quite some time if the client
      // is currently in the middle of a call to a downed JN.
      // We should instead do this asynchronously, and just stop
      // making any more calls after this point (eg clear the queue)
      RPC.stopProxy(proxy);
    }
  }
  
  protected QJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;
    proxy = createProxy();
    return proxy;
  }
  
  protected QJournalProtocol createProxy() throws IOException {
    RPC.setProtocolEngine(conf,
        QJournalProtocolPB.class, ProtobufRpcEngine.class);
    QJournalProtocolPB pbproxy = RPC.getProxy(
        QJournalProtocolPB.class,
        RPC.getProtocolVersion(QJournalProtocolPB.class),
        addr, conf);
    return new QJournalProtocolTranslatorPB(pbproxy);
  }
  
  
  /**
   * Separated out for easy overriding in tests.
   */
  @VisibleForTesting
  protected ExecutorService createExecutor() {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Logger channel to " + addr)
          .setUncaughtExceptionHandler(
              UncaughtExceptionHandlers.systemExit())
          .build());
  }
  
  @Override
  public URL buildURLToFetchLogs(long segmentTxId) {
    Preconditions.checkArgument(segmentTxId > 0,
        "Invalid segment: %s", segmentTxId);
    Preconditions.checkState(httpPort != -1,
        "HTTP port not set yet");
        
    try {
      String path = GetJournalEditServlet.buildPath(
          journalId, segmentTxId, nsInfo);
      return new URL("http", addr.getHostName(), httpPort, path.toString());
    } catch (MalformedURLException e) {
      // should never get here.
      throw new RuntimeException(e);
    }
  }

  private synchronized RequestInfo createReqInfo() {
    Preconditions.checkState(epoch > 0, "bad epoch: " + epoch);
    return new RequestInfo(journalId, epoch, ipcSerial++);
  }

  @VisibleForTesting
  synchronized long getNextIpcSerial() {
    return ipcSerial;
  }

  public synchronized int getQueuedEditsSize() {
    return queuedEditsSizeBytes;
  }
  
  @VisibleForTesting
  void waitForAllPendingCalls() throws InterruptedException {
    try {
      executor.submit(new Runnable() {
        @Override
        public void run() {
        }
      }).get();
    } catch (ExecutionException e) {
      // This can't happen!
      throw new AssertionError(e);
    }
  }

  @Override
  public ListenableFuture<GetJournalStateResponseProto> getJournalState() {
    return executor.submit(new Callable<GetJournalStateResponseProto>() {
      @Override
      public GetJournalStateResponseProto call() throws IOException {
        GetJournalStateResponseProto ret =
            getProxy().getJournalState(journalId);
        httpPort = ret.getHttpPort();
        return ret;
      }
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final long epoch) {
    return executor.submit(new Callable<NewEpochResponseProto>() {
      @Override
      public NewEpochResponseProto call() throws IOException {
        return getProxy().newEpoch(journalId, nsInfo, epoch);
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data) {
    try {
      reserveQueueSpace(data.length);
    } catch (LoggerTooFarBehindException e) {
      return Futures.immediateFailedFuture(e);
    }
    ListenableFuture<Void> ret = null;
    try {
      ret = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          getProxy().journal(createReqInfo(),
              segmentTxId, firstTxnId, numTxns, data);
          return null;
        }
      });
    } finally {
      if (ret == null) {
        // it didn't successfully get submitted,
        // so adjust the queue size back down.
        unreserveQueueSpace(data.length);
      } else {
        // It was submitted to the queue, so adjust the length
        // once the call completes, regardless of whether it
        // succeeds or fails.
        Futures.addCallback(ret, new FutureCallback<Void>() {
          @Override
          public void onFailure(Throwable t) {
            unreserveQueueSpace(data.length);
          }

          @Override
          public void onSuccess(Void t) {
            unreserveQueueSpace(data.length);
          }
        });
      }
    }
    return ret;
  }

  private synchronized void reserveQueueSpace(int size)
      throws LoggerTooFarBehindException {
    Preconditions.checkArgument(size >= 0);
    if (queuedEditsSizeBytes + size > queueSizeLimitBytes &&
        queuedEditsSizeBytes > 0) {
      throw new LoggerTooFarBehindException();
    }
    queuedEditsSizeBytes += size;
  }
  
  private synchronized void unreserveQueueSpace(int size) {
    Preconditions.checkArgument(size >= 0);
    queuedEditsSizeBytes -= size;
  }

  @Override
  public ListenableFuture<Void> format(final NamespaceInfo nsInfo) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().format(journalId, nsInfo);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> startLogSegment(final long txid) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().startLogSegment(createReqInfo(), txid);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> finalizeLogSegment(
      final long startTxId, final long endTxId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().finalizeLogSegment(createReqInfo(), startTxId, endTxId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> purgeLogsOlderThan(final long minTxIdToKeep) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        getProxy().purgeLogsOlderThan(createReqInfo(), minTxIdToKeep);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<RemoteEditLogManifest> getEditLogManifest(
      final long fromTxnId) {
    return executor.submit(new Callable<RemoteEditLogManifest>() {
      @Override
      public RemoteEditLogManifest call() throws IOException {
        GetEditLogManifestResponseProto ret = getProxy().getEditLogManifest(
            journalId, fromTxnId);
        // Update the http port, since we need this to build URLs to any of the
        // returned logs.
        httpPort = ret.getHttpPort();
        return PBHelper.convert(ret.getManifest());
      }
    });
  }

  @Override
  public ListenableFuture<PrepareRecoveryResponseProto> prepareRecovery(
      final long segmentTxId) {
    return executor.submit(new Callable<PrepareRecoveryResponseProto>() {
      @Override
      public PrepareRecoveryResponseProto call() throws IOException {
        if (httpPort < 0) {
          // If the HTTP port hasn't been set yet, force an RPC call so we know
          // what the HTTP port should be.
          httpPort = getProxy().getJournalState(journalId).getHttpPort();
        }
        return getProxy().prepareRecovery(createReqInfo(), segmentTxId);
      }
    });
  }

  @Override
  public ListenableFuture<Void> acceptRecovery(
      final SegmentStateProto log, final URL url) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().acceptRecovery(createReqInfo(), log, url);
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return "Channel to journal node " + addr; 
  }
}
