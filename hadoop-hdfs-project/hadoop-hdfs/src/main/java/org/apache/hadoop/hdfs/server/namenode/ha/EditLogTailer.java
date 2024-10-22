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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;


/**
 * EditLogTailer represents a thread which periodically reads from edits
 * journals and applies the transactions contained within to a given
 * FSNamesystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Logger LOG = LoggerFactory.getLogger(EditLogTailer.class);

  /**
   * StandbyNode will hold namesystem lock to apply at most this many journal
   * transactions.
   * It will then release the lock and re-acquire it to load more transactions.
   * By default the write lock is held for the entire journal segment.
   * Fine-grained locking allows read requests to get through.
   */
  public static final String  DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY =
      "dfs.ha.tail-edits.max-txns-per-lock";
  public static final long DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT =
      Long.MAX_VALUE;

  private final EditLogTailerThread tailerThread;
  
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private final Iterator<RemoteNameNodeInfo> nnLookup;
  private FSEditLog editLog;

  private RemoteNameNodeInfo currentNN;

  /**
   * The last transaction ID at which an edit log roll was initiated.
   */
  private long lastRollTriggerTxId = HdfsServerConstants.INVALID_TXID;
  
  /**
   * The highest transaction ID loaded by the Standby.
   */
  private long lastLoadedTxnId = HdfsServerConstants.INVALID_TXID;

  /**
   * The last time we successfully loaded a non-zero number of edits from the
   * shared directory.
   */
  private long lastLoadTimeMs;

  /**
   * The last time we triggered a edit log roll on active namenode.
   */
  private long lastRollTimeMs;

  /**
   * How often the Standby should roll edit logs. Since the Standby only reads
   * from finalized log segments, the Standby will only be as up-to-date as how
   * often the logs are rolled.
   */
  private final long logRollPeriodMs;

  /**
   * The timeout in milliseconds of calling rollEdits RPC to Active NN.
   * See HDFS-4176.
   */
  private final long rollEditsTimeoutMs;

  /**
   * The executor to run roll edit RPC call in a daemon thread.
   */
  private final ExecutorService rollEditsRpcExecutor;

  /**
   * How often the tailer should check if there are new edit log entries
   * ready to be consumed. This is the initial delay before any backoff.
   */
  private final long sleepTimeMs;
  /**
   * The maximum time the tailer should wait between checking for new edit log
   * entries. Exponential backoff will be applied when an edit log tail is
   * performed but no edits are available to be read. If this is less than or
   * equal to 0, backoff is disabled.
   */
  private final long maxSleepTimeMs;

  private final int nnCount;
  private NamenodeProtocol cachedActiveProxy = null;
  // count of the number of NNs we have attempted in the current lookup loop
  private int nnLoopCount = 0;

  /**
   * Maximum number of retries we should give each of the remote namenodes
   * before giving up.
   */
  private int maxRetries;

  /**
   * Whether the tailer should tail the in-progress edit log segments. If true,
   * this will also attempt to optimize for latency when tailing the edit logs
   * (if using the
   * {@link org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager}, this
   * implies using the RPC-based mechanism to tail edits).
   */
  private final boolean inProgressOk;

  /**
   * Release the namesystem lock after loading this many transactions.
   * Then re-acquire the lock to load more edits.
   */
  private final long maxTxnsPerLock;

  /**
   * Timer instance to be set only using constructor.
   * Only tests can reassign this by using setTimerForTests().
   * For source code, this timer instance should be treated as final.
   */
  private Timer timer;

  public EditLogTailer(FSNamesystem namesystem, Configuration conf) {
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;
    this.namesystem = namesystem;
    this.timer = new Timer();
    this.editLog = namesystem.getEditLog();
    this.lastLoadTimeMs = timer.monotonicNow();
    this.lastRollTimeMs = timer.monotonicNow();

    logRollPeriodMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    List<RemoteNameNodeInfo> nns = Collections.emptyList();
    if (logRollPeriodMs >= 0) {
      try {
        nns = RemoteNameNodeInfo.getRemoteNameNodes(conf);
      } catch (IOException e) {
        throw new IllegalArgumentException("Remote NameNodes not correctly configured!", e);
      }

      for (RemoteNameNodeInfo info : nns) {
        // overwrite the socket address, if we need to
        InetSocketAddress ipc = NameNode.getServiceAddress(info.getConfiguration(), true);
        // sanity check the ipc address
        Preconditions.checkArgument(ipc.getPort() > 0,
            "Active NameNode must have an IPC port configured. " + "Got address '%s'", ipc);
        info.setIpcAddress(ipc);
      }

      LOG.info("Will roll logs on active node every " +
          (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
          DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }
    
    sleepTimeMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    long maxSleepTimeMsTemp = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    if (maxSleepTimeMsTemp > 0 && maxSleepTimeMsTemp < sleepTimeMs) {
      LOG.warn("{} was configured to be {} ms, but this is less than {}."
              + "Disabling backoff when tailing edit logs.",
          DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_KEY,
          maxSleepTimeMsTemp, DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY);
      maxSleepTimeMs = 0;
    } else {
      maxSleepTimeMs = maxSleepTimeMsTemp;
    }

    rollEditsTimeoutMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    rollEditsRpcExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true).build());

    maxRetries = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY,
      DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
    if (maxRetries <= 0) {
      LOG.error("Specified a non-positive number of retries for the number of retries for the " +
          "namenode connection when manipulating the edit log (" +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY + "), setting to default: " +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
      maxRetries = DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT;
    }

    inProgressOk = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);

    this.maxTxnsPerLock = conf.getLong(
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY,
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT);

    nnCount = nns.size();
    // setup the iterator to endlessly loop the nns
    this.nnLookup = Iterators.cycle(nns);
    LOG.debug("logRollPeriodMs={} sleepTime={}.", logRollPeriodMs, sleepTimeMs);
  }

  public void start() {
    tailerThread.start();
  }
  
  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    } finally {
      rollEditsRpcExecutor.shutdown();
    }
  }
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  public void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }

  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    // Important to do tailing as the login user, in case the shared
    // edits storage is implemented by a JournalManager that depends
    // on security credentials to access the logs (eg QuorumJournalManager).
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        long editsTailed = 0;
        // Fully tail the journal to the end
        do {
          long startTime = timer.monotonicNow();
          try {
            NameNode.getNameNodeMetrics().addEditLogTailInterval(
                startTime - lastLoadTimeMs);
            // It is already under the name system lock and the checkpointer
            // thread is already stopped. No need to acquire any other lock.
            // HDFS-16689. Disable inProgress to use the streaming mechanism
            editsTailed = doTailEdits(false);
          } catch (InterruptedException e) {
            throw new IOException(e);
          } finally {
            NameNode.getNameNodeMetrics().addEditLogTailTime(
                timer.monotonicNow() - startTime);
          }
        } while(editsTailed > 0);
        return null;
      }
    });
  }

  @VisibleForTesting
  public long doTailEdits() throws IOException, InterruptedException {
    return doTailEdits(inProgressOk);
  }

  private long doTailEdits(boolean enableInProgress) throws IOException, InterruptedException {
    Collection<EditLogInputStream> streams;
    FSImage image = namesystem.getFSImage();

    long lastTxnId = image.getLastAppliedTxId();
    LOG.debug("lastTxnId: {}", lastTxnId);
    long startTime = timer.monotonicNow();
    try {
      streams = editLog.selectInputStreams(lastTxnId + 1, 0,
          null, enableInProgress, true);
    } catch (IOException ioe) {
      // This is acceptable. If we try to tail edits in the middle of an edits
      // log roll, i.e. the last one has been finalized but the new inprogress
      // edits file hasn't been started yet.
      LOG.warn("Edits tailer failed to find any streams. Will try again " +
          "later.", ioe);
      return 0;
    } finally {
      NameNode.getNameNodeMetrics().addEditLogFetchTime(
          timer.monotonicNow() - startTime);
    }
    // Write lock needs to be interruptible here because the 
    // transitionToActive RPC takes the write lock before calling
    // tailer.stop() -- so if we're not interruptible, it will
    // deadlock.
    namesystem.writeLockInterruptibly();
    try {
      long currentLastTxnId = image.getLastAppliedTxId();
      if (lastTxnId != currentLastTxnId) {
        LOG.warn("The currentLastTxnId({}) is different from preLastTxtId({})",
            currentLastTxnId, lastTxnId);
        return 0;
      }
      LOG.debug("edit streams to load from: {}.", streams.size());
      
      // Once we have streams to load, errors encountered are legitimate cause
      // for concern, so we don't catch them here. Simple errors reading from
      // disk are ignored.
      long editsLoaded = 0;
      try {
        editsLoaded = image.loadEdits(
            streams, namesystem, maxTxnsPerLock, null, null);
      } catch (EditLogInputException elie) {
        editsLoaded = elie.getNumEditsLoaded();
        throw elie;
      } finally {
        LOG.debug("Loaded {} edits starting from txid {}.", editsLoaded, lastTxnId);
        NameNode.getNameNodeMetrics().addNumEditLogLoaded(editsLoaded);
      }

      if (editsLoaded > 0) {
        lastLoadTimeMs = timer.monotonicNow();
      }
      lastLoadedTxnId = image.getLastAppliedTxId();
      return editsLoaded;
    } finally {
      namesystem.writeUnlock("doTailEdits");
    }
  }

  /**
   * @return time in msec of when we last loaded a non-zero number of edits.
   */
  public long getLastLoadTimeMs() {
    return lastLoadTimeMs;
  }

  /**
   * @return true if the configured log roll period has elapsed.
   */
  private boolean tooLongSinceLastLoad() {
    return logRollPeriodMs >= 0 && 
      (timer.monotonicNow() - lastRollTimeMs) > logRollPeriodMs;
  }

  /**
   * NameNodeProxy factory method.
   * @return a Callable to roll logs on remote NameNode.
   */
  @VisibleForTesting
  Callable<Void> getNameNodeProxy() {
    return new MultipleNameNodeProxy<Void>() {
      @Override
      protected Void doWork() throws IOException {
        LOG.info("Triggering log rolling to the remote NameNode, " +
            "active NameNode = {}", currentNN.getIpcAddress());
        cachedActiveProxy.rollEditLog();
        return null;
      }
    };
  }

  /**
   * Trigger the active node to roll its logs.
   */
  @VisibleForTesting
  void triggerActiveLogRoll() {
    Future<Void> future = null;
    try {
      future = rollEditsRpcExecutor.submit(getNameNodeProxy());
      future.get(rollEditsTimeoutMs, TimeUnit.MILLISECONDS);
      this.lastRollTimeMs = timer.monotonicNow();
      lastRollTriggerTxId = lastLoadedTxnId;
    } catch (ExecutionException | InterruptedException e) {
      LOG.warn("Unable to trigger a roll of the active NN", e);
    } catch (TimeoutException e) {
      if (future != null) {
        future.cancel(true);
      }
      LOG.warn(String.format(
          "Unable to finish rolling edits in %d ms", rollEditsTimeoutMs));
    }
  }

  /**
   * This is only to be used by tests. For source code, the only way to
   * set timer is by using EditLogTailer constructor.
   *
   * @param newTimer Timer instance provided by tests.
   */
  @VisibleForTesting
  void setTimerForTest(final Timer newTimer) {
    this.timer = newTimer;
  }

  /**
   * Used by tests. Return Timer instance used by EditLogTailer.
   *
   * @return Return Timer instance used by EditLogTailer.
   */
  @VisibleForTesting
  Timer getTimer() {
    return timer;
  }

  @VisibleForTesting
  void sleep(long sleepTimeMillis) throws InterruptedException {
    Thread.sleep(sleepTimeMillis);
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;
    
    private EditLogTailerThread() {
      super("Edit log tailer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    @Override
    public void run() {
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            doWork();
            return null;
          }
        });
    }
    
    private void doWork() {
      long currentSleepTimeMs = sleepTimeMs;
      while (shouldRun) {
        long editsTailed  = 0;
        try {
          // There's no point in triggering a log roll if the Standby hasn't
          // read any more transactions since the last time a roll was
          // triggered.
          boolean triggeredLogRoll = false;
          if (!namesystem.isInObserverState() && tooLongSinceLastLoad() &&
              lastRollTriggerTxId < lastLoadedTxnId) {
            triggerActiveLogRoll();
            triggeredLogRoll = true;
          }
          /**
           * Check again in case someone calls {@link EditLogTailer#stop} while
           * we're triggering an edit log roll, since ipc.Client catches and
           * ignores {@link InterruptedException} in a few places. This fixes
           * the bug described in HDFS-2823.
           */
          if (!shouldRun) {
            break;
          }
          // Prevent reading of name system while being modified. The full
          // name system lock will be acquired to further block even the block
          // state updates.
          namesystem.cpLockInterruptibly();
          long startTime = timer.monotonicNow();
          try {
            NameNode.getNameNodeMetrics().addEditLogTailInterval(
                startTime - lastLoadTimeMs);
            editsTailed = doTailEdits();
          } finally {
            namesystem.cpUnlock();
            NameNode.getNameNodeMetrics().addEditLogTailTime(
                timer.monotonicNow() - startTime);
          }
          //Update NameDirSize Metric
          if (triggeredLogRoll) {
            namesystem.getFSImage().getStorage().updateNameDirSize();
          }
        } catch (EditLogInputException elie) {
          LOG.warn("Error while reading edits from disk. Will try again.", elie);
        } catch (InterruptedException ie) {
          // interrupter should have already set shouldRun to false
          continue;
        } catch (Throwable t) {
          LOG.error("Unknown error encountered while tailing edits. " +
              "Shutting down standby NN.", t);
          terminate(1, t);
        }

        try {
          if (editsTailed == 0 && maxSleepTimeMs > 0) {
            // If no edits were tailed, apply exponential backoff
            // before tailing again. Double the current sleep time on each
            // empty response, but don't exceed the max. If the sleep time
            // was configured as 0, start the backoff at 1 ms.
            currentSleepTimeMs = Math.min(maxSleepTimeMs,
                (currentSleepTimeMs == 0 ? 1 : currentSleepTimeMs) * 2);
          } else {
            currentSleepTimeMs = sleepTimeMs; // reset to initial sleep time
          }
          EditLogTailer.this.sleep(currentSleepTimeMs);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted: {}", e.getMessage());
        }
      }
    }
  }
  /**
   * Manage the 'active namenode proxy'. This cannot just be the a single proxy since we could
   * failover across a number of NameNodes, rather than just between an active and a standby.
   * <p>
   * We - lazily - get a proxy to one of the configured namenodes and attempt to make the request
   * against it. If it doesn't succeed, either because the proxy failed to be created or the request
   * failed, we try the next NN in the list. We try this up to the configuration maximum number of
   * retries before throwing up our hands. A working proxy is retained across attempts since we
   * expect the active NameNode to switch rarely.
   * <p>
   * This mechanism is <b>very bad</b> for cases where we care about being <i>fast</i>; it just
   * blindly goes and tries namenodes.
   */
  @VisibleForTesting
  abstract class MultipleNameNodeProxy<T> implements Callable<T> {

    /**
     * Do the actual work to the remote namenode via the {@link #cachedActiveProxy}.
     * @return the result of the work, if there is one
     * @throws IOException if the actions done to the proxy throw an exception.
     */
    protected abstract T doWork() throws IOException;

    public T call() throws IOException {
      // reset the loop count on success
      nnLoopCount = 0;
      while ((cachedActiveProxy = getActiveNodeProxy()) != null) {
        try {
          T ret = doWork();
          return ret;
        } catch (IOException e) {
          LOG.warn("Exception from remote name node " + currentNN
              + ", try next.", e);

          // Try next name node if exception happens.
          cachedActiveProxy = null;
          nnLoopCount++;
        }
      }
      throw new IOException("Cannot find any valid remote NN to service request!");
    }

    private NamenodeProtocol getActiveNodeProxy() throws IOException {
      if (cachedActiveProxy == null) {
        while (true) {
          // If the thread is interrupted, quit by returning null.
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("Interrupted while trying to getActiveNodeProxy.");
            return null;
          }

          // if we have reached the max loop count, quit by returning null
          if ((nnLoopCount / nnCount) >= maxRetries) {
            LOG.warn("Have reached the max loop count ({}).", nnLoopCount);
            return null;
          }

          currentNN = nnLookup.next();
          try {
            int rpcTimeout = conf.getInt(
                DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT);
            NamenodeProtocolPB proxy = RPC.waitForProxy(NamenodeProtocolPB.class,
                RPC.getProtocolVersion(NamenodeProtocolPB.class), currentNN.getIpcAddress(), conf,
                rpcTimeout, Long.MAX_VALUE);
            cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
            break;
          } catch (IOException e) {
            LOG.info("Failed to reach " + currentNN, e);
            // couldn't even reach this NN, try the next one
            nnLoopCount++;
          }
        }
      }
      assert cachedActiveProxy != null;
      return cachedActiveProxy;
    }
  }

  @VisibleForTesting
  public NamenodeProtocol getCachedActiveProxy() {
    return cachedActiveProxy;
  }

  @VisibleForTesting
  public long getLastRollTimeMs() {
    return lastRollTimeMs;
  }

  @VisibleForTesting
  public RemoteNameNodeInfo getCurrentNN() {
    return currentNN;
  }

  @VisibleForTesting
  public void setShouldRunForTest(boolean shouldRun) {
    this.tailerThread.setShouldRun(shouldRun);
  }
}
