/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LeaseException;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionState.Status;
import org.apache.hadoop.util.Progressable;

/**
 * Regionserver which provides transactional support for atomic transactions.
 * This is achieved with optimistic concurrency control (see
 * http://www.seas.upenn.edu/~zives/cis650/papers/opt-cc.pdf). We keep track
 * read and write sets for each transaction, and hold off on processing the
 * writes. To decide to commit a transaction we check its read sets with all
 * transactions that have committed while it was running for overlaps.
 * <p>
 * Because transactions can span multiple regions, all regions must agree to
 * commit a transactions. The client side of this commit protocol is encoded in
 * org.apache.hadoop.hbase.client.transactional.TransactionManger
 * <p>
 * In the event of an failure of the client mid-commit, (after we voted yes), we
 * will have to consult the transaction log to determine the final decision of
 * the transaction. This is not yet implemented.
 */
public class TransactionalRegion extends HRegion {

  private static final String OLD_TRANSACTION_FLUSH = "hbase.transaction.flush";
  private static final int DEFAULT_OLD_TRANSACTION_FLUSH = 100; // Do a flush if
  // we have this
  // many old
  // transactions..

  static final Log LOG = LogFactory.getLog(TransactionalRegion.class);

  // Collection of active transactions (PENDING) keyed by id.
  protected Map<String, TransactionState> transactionsById = new HashMap<String, TransactionState>();

  // Map of recent transactions that are COMMIT_PENDING or COMMITED keyed by
  // their sequence number
  private SortedMap<Integer, TransactionState> commitedTransactionsBySequenceNumber = Collections
      .synchronizedSortedMap(new TreeMap<Integer, TransactionState>());

  // Collection of transactions that are COMMIT_PENDING
  private Set<TransactionState> commitPendingTransactions = Collections
      .synchronizedSet(new HashSet<TransactionState>());

  private AtomicInteger nextSequenceId = new AtomicInteger(0);
  private Object commitCheckLock = new Object();
  private THLog hlog;
  private final int oldTransactionFlushTrigger;
  private final Leases transactionLeases;

  /**
   * @param basedir
   * @param log
   * @param fs
   * @param conf
   * @param regionInfo
   * @param flushListener
   */
  public TransactionalRegion(final Path basedir, final HLog log,
      final FileSystem fs, final HBaseConfiguration conf,
      final HRegionInfo regionInfo, final FlushRequester flushListener,
      final Leases transactionalLeases) {
    super(basedir, log, fs, conf, regionInfo, flushListener);
    this.hlog = (THLog) log;
    oldTransactionFlushTrigger = conf.getInt(OLD_TRANSACTION_FLUSH,
        DEFAULT_OLD_TRANSACTION_FLUSH);
    this.transactionLeases = transactionalLeases;
  }

  @Override
  protected void doReconstructionLog(final Path oldLogFile,
      final long minSeqId, final long maxSeqId, final Progressable reporter)
      throws UnsupportedEncodingException, IOException {
    super.doReconstructionLog(oldLogFile, minSeqId, maxSeqId, reporter);

    THLogRecoveryManager recoveryManager = new THLogRecoveryManager(this);
    Map<Long, List<KeyValue>> commitedTransactionsById = recoveryManager
        .getCommitsFromLog(oldLogFile, minSeqId, reporter);

    if (commitedTransactionsById != null && commitedTransactionsById.size() > 0) {
      LOG.debug("found " + commitedTransactionsById.size()
          + " COMMITED transactions");

      for (Entry<Long, List<KeyValue>> entry : commitedTransactionsById
          .entrySet()) {
        LOG.debug("Writing " + entry.getValue().size()
            + " updates for transaction " + entry.getKey());
        for (KeyValue b : entry.getValue()) {
          Put put = new Put(b.getRow());
          put.add(b);
          super.put(put, true); // These are walled so they live forever
        }
      }

      // LOG.debug("Flushing cache"); // We must trigger a cache flush,
      // otherwise
      // we will would ignore the log on subsequent failure
      // if (!super.flushcache()) {
      // LOG.warn("Did not flush cache");
      // }
    }
  }

  /**
   * We need to make sure that we don't complete a cache flush between running
   * transactions. If we did, then we would not find all log messages needed to
   * restore the transaction, as some of them would be before the last
   * "complete" flush id.
   */
  @Override
  protected long getCompleteCacheFlushSequenceId(final long currentSequenceId) {
    LinkedList<TransactionState> transactionStates;
    synchronized (transactionsById) {
      transactionStates = new LinkedList<TransactionState>(transactionsById
          .values());
    }

    long minPendingStartSequenceId = currentSequenceId;
    for (TransactionState transactionState : transactionStates) {
      minPendingStartSequenceId = Math.min(minPendingStartSequenceId,
          transactionState.getHLogStartSequenceId());
    }
    return minPendingStartSequenceId;
  }

  /**
   * @param transactionId
   * @throws IOException
   */
  public void beginTransaction(final long transactionId) throws IOException {
    checkClosing();
    String key = String.valueOf(transactionId);
    if (transactionsById.get(key) != null) {
      TransactionState alias = getTransactionState(transactionId);
      if (alias != null) {
        alias.setStatus(Status.ABORTED);
        retireTransaction(alias);
      }
      LOG.error("Existing trasaction with id [" + key + "] in region ["
          + super.getRegionInfo().getRegionNameAsString() + "]");
      throw new IOException("Already exiting transaction id: " + key);
    }

    TransactionState state = new TransactionState(transactionId, super.getLog()
        .getSequenceNumber(), super.getRegionInfo());

    // Order is important here ...
    List<TransactionState> commitPendingCopy = new LinkedList<TransactionState>(
        commitPendingTransactions);
    for (TransactionState commitPending : commitPendingCopy) {
      state.addTransactionToCheck(commitPending);
    }
    state.setStartSequenceNumber(nextSequenceId.get());

    synchronized (transactionsById) {
      transactionsById.put(key, state);
    }
    try {
      transactionLeases.createLease(getLeaseId(transactionId),
          new TransactionLeaseListener(key));
    } catch (LeaseStillHeldException e) {
      LOG.error("Lease still held for [" + key + "] in region ["
          + super.getRegionInfo().getRegionNameAsString() + "]");
      throw new RuntimeException(e);
    }
    LOG.debug("Begining transaction " + key + " in region "
        + super.getRegionInfo().getRegionNameAsString());
    this.hlog.writeStartToLog(super.getRegionInfo(), transactionId);

    maybeTriggerOldTransactionFlush();
  }

  private String getLeaseId(long transactionId) {
    return super.getRegionInfo().getRegionNameAsString() + transactionId;
  }

  public Result get(final long transactionId, Get get) throws IOException {
    checkClosing();

    TransactionState state = getTransactionState(transactionId);

    state.addRead(get.getRow());

    Result superGet = super.get(get, null);
    Result localGet = state.localGet(get);

    if (localGet != null) {
      LOG
          .trace("Transactional get of something we've written in the same transaction "
              + transactionId);

      List<KeyValue> mergedGet = new ArrayList<KeyValue>(Arrays.asList(localGet
          .raw()));

      if (superGet != null && !superGet.isEmpty()) {
        for (KeyValue kv : superGet.raw()) {
          if (!localGet.containsColumn(kv.getFamily(), kv.getQualifier())) {
            mergedGet.add(kv);
          }
        }
      }
      return new Result(mergedGet);
    }

    return superGet;
  }

  /**
   * Get a transactional scanner.
   */
  public InternalScanner getScanner(final long transactionId, Scan scan)
      throws IOException {
    checkClosing();

    TransactionState state = getTransactionState(transactionId);
    state.addScan(scan);
    return new ScannerWrapper(transactionId, super.getScanner(scan));
  }

  /**
   * Add a write to the transaction. Does not get applied until commit process.
   * 
   * @param transactionId
   * @param b
   * @throws IOException
   */
  public void put(final long transactionId, final Put put) throws IOException {
    checkClosing();

    TransactionState state = getTransactionState(transactionId);
    state.addWrite(put);
    this.hlog.writeUpdateToLog(super.getRegionInfo(), transactionId, put);
  }

  /**
   * Add multiple writes to the transaction. Does not get applied until commit
   * process.
   * 
   * @param transactionId
   * @param puts
   * @throws IOException
   */
  public void put(final long transactionId, final Put[] puts)
      throws IOException {
    checkClosing();

    TransactionState state = getTransactionState(transactionId);
    for (Put put : puts) {
      state.addWrite(put);
      this.hlog.writeUpdateToLog(super.getRegionInfo(), transactionId, put);
    }
  }

  /**
   * Add a delete to the transaction. Does not get applied until commit process.
   * FIXME, not sure about this approach
   * 
   * @param transactionId
   * @param row
   * @param timestamp
   * @throws IOException
   */
  public void delete(final long transactionId, Delete delete)
      throws IOException {
    checkClosing();
    TransactionState state = getTransactionState(transactionId);
    state.addDelete(delete);
    this.hlog.writeDeleteToLog(super.getRegionInfo(), transactionId, delete);
  }

  /**
   * @param transactionId
   * @return TransactionRegionInterface commit code
   * @throws IOException
   */
  public int commitRequest(final long transactionId) throws IOException {
    checkClosing();

    synchronized (commitCheckLock) {
      TransactionState state = getTransactionState(transactionId);
      if (state == null) {
        return TransactionalRegionInterface.COMMIT_UNSUCESSFUL;
      }

      if (hasConflict(state)) {
        state.setStatus(Status.ABORTED);
        retireTransaction(state);
        return TransactionalRegionInterface.COMMIT_UNSUCESSFUL;
      }

      // No conflicts, we can commit.
      LOG.trace("No conflicts for transaction " + transactionId
          + " found in region " + super.getRegionInfo().getRegionNameAsString()
          + ". Voting for commit");

      // If there are writes we must keep record off the transaction
      if (state.getWriteSet().size() > 0) {
        // Order is important
        state.setStatus(Status.COMMIT_PENDING);
        commitPendingTransactions.add(state);
        state.setSequenceNumber(nextSequenceId.getAndIncrement());
        commitedTransactionsBySequenceNumber.put(state.getSequenceNumber(),
            state);
        return TransactionalRegionInterface.COMMIT_OK;
      }
      // Otherwise we were read-only and commitable, so we can forget it.
      state.setStatus(Status.COMMITED);
      retireTransaction(state);
      return TransactionalRegionInterface.COMMIT_OK_READ_ONLY;
    }
  }

  /**
   * @param transactionId
   * @return true if commit is successful
   * @throws IOException
   */
  public boolean commitIfPossible(final long transactionId) throws IOException {
    int status = commitRequest(transactionId);

    if (status == TransactionalRegionInterface.COMMIT_OK) {
      commit(transactionId);
      return true;
    } else if (status == TransactionalRegionInterface.COMMIT_OK_READ_ONLY) {
      return true;
    }
    return false;
  }

  private boolean hasConflict(final TransactionState state) {
    // Check transactions that were committed while we were running
    for (int i = state.getStartSequenceNumber(); i < nextSequenceId.get(); i++) {
      TransactionState other = commitedTransactionsBySequenceNumber.get(i);
      if (other == null) {
        continue;
      }
      state.addTransactionToCheck(other);
    }

    return state.hasConflict();
  }

  /**
   * Commit the transaction.
   * 
   * @param transactionId
   * @throws IOException
   */
  public void commit(final long transactionId) throws IOException {
    // Not checking closing...
    TransactionState state;
    try {
      state = getTransactionState(transactionId);
    } catch (UnknownTransactionException e) {
      LOG.fatal("Asked to commit unknown transaction: " + transactionId
          + " in region " + super.getRegionInfo().getRegionNameAsString());
      // FIXME Write to the transaction log that this transaction was corrupted
      throw e;
    }

    if (!state.getStatus().equals(Status.COMMIT_PENDING)) {
      LOG.fatal("Asked to commit a non pending transaction");
      // FIXME Write to the transaction log that this transaction was corrupted
      throw new IOException("commit failure");
    }

    commit(state);
  }

  /**
   * Commit the transaction.
   * 
   * @param transactionId
   * @throws IOException
   */
  public void abort(final long transactionId) throws IOException {
    // Not checking closing...
    TransactionState state;
    try {
      state = getTransactionState(transactionId);
    } catch (UnknownTransactionException e) {
      LOG.info("Asked to abort unknown transaction [" + transactionId
          + "] in region [" + getRegionInfo().getRegionNameAsString()
          + "], ignoring");
      return;
    }

    state.setStatus(Status.ABORTED);

    this.hlog.writeAbortToLog(super.getRegionInfo(), state.getTransactionId());

    // Following removes needed if we have voted
    if (state.getSequenceNumber() != null) {
      commitedTransactionsBySequenceNumber.remove(state.getSequenceNumber());
    }
    commitPendingTransactions.remove(state);

    retireTransaction(state);
  }

  private void commit(final TransactionState state) throws IOException {

    LOG.debug("Commiting transaction: " + state.toString() + " to "
        + super.getRegionInfo().getRegionNameAsString());

    this.hlog.writeCommitToLog(super.getRegionInfo(), state.getTransactionId());

    for (Put update : state.getWriteSet()) {
      this.put(update, false); // Don't need to WAL these
      // FIME, maybe should be walled so we don't need to look so far back.
    }

    state.setStatus(Status.COMMITED);
    if (state.getWriteSet().size() > 0
        && !commitPendingTransactions.remove(state)) {
      LOG
          .fatal("Commiting a non-query transaction that is not in commitPendingTransactions");
      throw new IOException("commit failure"); // FIXME, how to handle?
    }
    retireTransaction(state);
  }

  @Override
  public List<StoreFile> close(boolean abort) throws IOException {
    prepareToClose();
    if (!commitPendingTransactions.isEmpty()) {
      // FIXME, better way to handle?
      LOG.warn("Closing transactional region ["
          + getRegionInfo().getRegionNameAsString() + "], but still have ["
          + commitPendingTransactions.size()
          + "] transactions  that are pending commit");
    }
    return super.close(abort);
  }

  @Override
  protected void prepareToSplit() {
    prepareToClose();
  }

  boolean closing = false;

  /**
   * Get ready to close.
   * 
   */
  void prepareToClose() {
    LOG.info("Preparing to close region "
        + getRegionInfo().getRegionNameAsString());
    closing = true;

    while (!commitPendingTransactions.isEmpty()) {
      LOG.info("Preparing to closing transactional region ["
          + getRegionInfo().getRegionNameAsString() + "], but still have ["
          + commitPendingTransactions.size()
          + "] transactions that are pending commit. Sleeping");
      for (TransactionState s : commitPendingTransactions) {
        LOG.info(s.toString());
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

    }
  }

  private void checkClosing() throws IOException {
    if (closing) {
      throw new IOException("closing region, no more transaction allowed");
    }
  }

  // Cancel leases, and removed from lease lookup. This transaction may still
  // live in commitedTransactionsBySequenceNumber and commitPendingTransactions
  private void retireTransaction(final TransactionState state) {
    String key = String.valueOf(state.getTransactionId());
    try {
      transactionLeases.cancelLease(getLeaseId(state.getTransactionId()));
    } catch (LeaseException e) {
      // Ignore
    }

    transactionsById.remove(key);
  }

  protected TransactionState getTransactionState(final long transactionId)
      throws UnknownTransactionException {
    String key = String.valueOf(transactionId);
    TransactionState state = null;

    state = transactionsById.get(key);

    if (state == null) {
      LOG.debug("Unknown transaction: [" + key + "], region: ["
          + getRegionInfo().getRegionNameAsString() + "]");
      throw new UnknownTransactionException("transaction: [" + key
          + "], region: [" + getRegionInfo().getRegionNameAsString() + "]");
    }

    try {
      transactionLeases.renewLease(getLeaseId(transactionId));
    } catch (LeaseException e) {
      throw new RuntimeException(e);
    }

    return state;
  }

  private void maybeTriggerOldTransactionFlush() {
    if (commitedTransactionsBySequenceNumber.size() > oldTransactionFlushTrigger) {
      removeUnNeededCommitedTransactions();
    }
  }

  /**
   * Cleanup references to committed transactions that are no longer needed.
   * 
   */
  synchronized void removeUnNeededCommitedTransactions() {
    Integer minStartSeqNumber = getMinStartSequenceNumber();
    if (minStartSeqNumber == null) {
      minStartSeqNumber = Integer.MAX_VALUE; // Remove all
    }

    int numRemoved = 0;
    // Copy list to avoid conc update exception
    for (Entry<Integer, TransactionState> entry : new LinkedList<Entry<Integer, TransactionState>>(
        commitedTransactionsBySequenceNumber.entrySet())) {
      if (entry.getKey() >= minStartSeqNumber) {
        break;
      }
      numRemoved = numRemoved
          + (commitedTransactionsBySequenceNumber.remove(entry.getKey()) == null ? 0
              : 1);
      numRemoved++;
    }

    if (LOG.isDebugEnabled()) {
      StringBuilder debugMessage = new StringBuilder();
      if (numRemoved > 0) {
        debugMessage.append("Removed [").append(numRemoved).append(
            "] commited transactions");

        if (minStartSeqNumber == Integer.MAX_VALUE) {
          debugMessage.append("with any sequence number");
        } else {
          debugMessage.append("with sequence lower than [").append(
              minStartSeqNumber).append("].");
        }
        if (!commitedTransactionsBySequenceNumber.isEmpty()) {
          debugMessage.append(" Still have [").append(
              commitedTransactionsBySequenceNumber.size()).append("] left.");
        } else {
          debugMessage.append(" None left.");
        }
        LOG.debug(debugMessage.toString());
      } else if (commitedTransactionsBySequenceNumber.size() > 0) {
        debugMessage.append(
            "Could not remove any transactions, and still have ").append(
            commitedTransactionsBySequenceNumber.size()).append(" left");
        LOG.debug(debugMessage.toString());
      }
    }
  }

  private Integer getMinStartSequenceNumber() {
    LinkedList<TransactionState> transactionStates;
    synchronized (transactionsById) {
      transactionStates = new LinkedList<TransactionState>(transactionsById
          .values());
    }
    Integer min = null;
    for (TransactionState transactionState : transactionStates) {
      if (min == null || transactionState.getStartSequenceNumber() < min) {
        min = transactionState.getStartSequenceNumber();
      }
    }
    return min;
  }

  private void resolveTransactionFromLog(final TransactionState transactionState)
      throws IOException {
    LOG
        .error("Global transaction log is not Implemented. (Optimisticly) assuming transaction commit!");
    commit(transactionState);
    // throw new RuntimeException("Global transaction log is not Implemented");
  }


  private static final int MAX_COMMIT_PENDING_WAITS = 10;

  private class TransactionLeaseListener implements LeaseListener {
    private final String transactionName;

    TransactionLeaseListener(final String n) {
      this.transactionName = n;
    }

    public void leaseExpired() {
      LOG.info("Transaction [" + this.transactionName + "] expired in region ["
          + getRegionInfo().getRegionNameAsString() + "]");
      TransactionState s = null;
      synchronized (transactionsById) {
        s = transactionsById.remove(transactionName);
      }
      if (s == null) {
        LOG.warn("Unknown transaction expired " + this.transactionName);
        return;
      }

      switch (s.getStatus()) {
      case PENDING:
        s.setStatus(Status.ABORTED); // Other transactions may have a ref
        break;
      case COMMIT_PENDING:
        LOG.info("Transaction " + s.getTransactionId()
            + " expired in COMMIT_PENDING state");

        try {
          if (s.getCommitPendingWaits() > MAX_COMMIT_PENDING_WAITS) {
            LOG.info("Checking transaction status in transaction log");
            resolveTransactionFromLog(s);
            break;
          }
          LOG.info("renewing lease and hoping for commit");
          s.incrementCommitPendingWaits();
          String key = Long.toString(s.getTransactionId());
          transactionsById.put(key, s);
          try {
            transactionLeases.createLease(getLeaseId(s.getTransactionId()),
                this);
          } catch (LeaseStillHeldException e) {
            transactionLeases.renewLease(getLeaseId(s.getTransactionId()));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        break;
      default:
        LOG.warn("Unexpected status on expired lease");
      }
    }
  }

  /** Wrapper which keeps track of rows returned by scanner. */
  private class ScannerWrapper implements InternalScanner {
    private long transactionId;
    private InternalScanner scanner;

    /**
     * @param transactionId
     * @param scanner
     * @throws UnknownTransactionException
     */
    public ScannerWrapper(final long transactionId,
        final InternalScanner scanner) throws UnknownTransactionException {

      this.transactionId = transactionId;
      this.scanner = scanner;
    }

    public void close() throws IOException {
      scanner.close();
    }

    public boolean next(List<KeyValue> results) throws IOException {
      boolean result = scanner.next(results);
      TransactionState state = getTransactionState(transactionId);
      // FIXME need to weave in new stuff from this transaction too.
      return result;
    }
  }
}
