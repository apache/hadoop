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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.util.AutoCloseableLock;

/**
 * An in-memory cache of edits in their serialized form. This is used to serve
 * the {@link Journal#getJournaledEdits(long, int)} call, used by the
 * QJM when {@value DFSConfigKeys#DFS_HA_TAILEDITS_INPROGRESS_KEY} is
 * enabled.
 *
 * <p>When a batch of edits is received by the JournalNode, it is put into this
 * cache via {@link #storeEdits(byte[], long, long, int)}. Edits must be
 * stored contiguously; if a batch of edits is stored that does not align with
 * the previously stored edits, the cache will be cleared before storing new
 * edits to avoid gaps. This decision is made because gaps are only handled
 * when in recovery mode, which the cache is not intended to be used for.
 *
 * <p>Batches of edits are stored in a {@link TreeMap} mapping the starting
 * transaction ID of the batch to the data buffer. Upon retrieval, the
 * relevant data buffers are concatenated together and a header is added
 * to construct a fully-formed edit data stream.
 *
 * <p>The cache is of a limited size capacity determined by
 * {@value DFSConfigKeys#DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY}. If the capacity
 * is exceeded after adding a new batch of edits, batches of edits are removed
 * until the total size is less than the capacity, starting from the ones
 * containing the oldest transactions. Transactions range in size, but a
 * decent rule of thumb is that 200 bytes are needed per transaction. Monitoring
 * the {@link JournalMetrics#rpcRequestCacheMissAmount} metric is recommended
 * to determine if the cache is too small; it will indicate both how many
 * cache misses occurred, and how many more transactions would have been
 * needed in the cache to serve the request.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class JournaledEditsCache {

  private static final int INVALID_LAYOUT_VERSION = 0;
  private static final long INVALID_TXN_ID = -1;

  /** The capacity, in bytes, of this cache. */
  private final int capacity;

  /**
   * Read/write lock pair wrapped in AutoCloseable; these refer to the same
   * underlying lock.
   */
  private final AutoCloseableLock readLock;
  private final AutoCloseableLock writeLock;

  // ** Start lock-protected fields **

  /**
   * Stores the actual data as a mapping of the StartTxnId of a batch of edits
   * to the serialized batch of edits. Stores only contiguous ranges; that is,
   * the last transaction ID in one batch is always one less than the first
   * transaction ID in the next batch. Though the map is protected by the lock,
   * individual data buffers are immutable and can be accessed without locking.
   */
  private final NavigableMap<Long, byte[]> dataMap = new TreeMap<>();
  /** Stores the layout version currently present in the cache. */
  private int layoutVersion = INVALID_LAYOUT_VERSION;
  /** Stores the serialized version of the header for the current version. */
  private ByteBuffer layoutHeader;

  /**
   * The lowest/highest transaction IDs present in the cache.
   * {@value INVALID_TXN_ID} if there are no transactions in the cache.
   */
  private long lowestTxnId;
  private long highestTxnId;
  /**
   * The lowest transaction ID that was ever present in the cache since last
   * being reset (i.e. since initialization or since reset due to being out of
   * sync with the Journal). Until the cache size goes above capacity, this is
   * equal to lowestTxnId.
   */
  private long initialTxnId;
  /** The current total size of all buffers in this cache. */
  private int totalSize;

  // ** End lock-protected fields **

  JournaledEditsCache(Configuration conf) {
    capacity = conf.getInt(DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_DEFAULT);
    if (capacity > 0.9 * Runtime.getRuntime().maxMemory()) {
      Journal.LOG.warn(String.format("Cache capacity is set at %d bytes but " +
          "maximum JVM memory is only %d bytes. It is recommended that you " +
          "decrease the cache size or increase the heap size.",
          capacity, Runtime.getRuntime().maxMemory()));
    }
    Journal.LOG.info("Enabling the journaled edits cache with a capacity " +
        "of bytes: " + capacity);
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = new AutoCloseableLock(lock.readLock());
    writeLock = new AutoCloseableLock(lock.writeLock());
    initialize(INVALID_TXN_ID);
  }

  /**
   * Fetch the data for edits starting at the specific transaction ID, fetching
   * up to {@code maxTxns} transactions. Populates a list of output buffers
   * which contains a serialized version of the edits, and returns the count of
   * edits contained within the serialized buffers. The serialized edits are
   * prefixed with a standard edit log header containing information about the
   * layout version. The transactions returned are guaranteed to have contiguous
   * transaction IDs.
   *
   * If {@code requestedStartTxn} is higher than the highest transaction which
   * has been added to this cache, a response with an empty buffer and a
   * transaction count of 0 will be returned. If {@code requestedStartTxn} is
   * lower than the lowest transaction currently contained in this cache, or no
   * transactions have yet been added to the cache, an exception will be thrown.
   *
   * @param requestedStartTxn The ID of the first transaction to return. If any
   *                          transactions are returned, it is guaranteed that
   *                          the first one will have this ID.
   * @param maxTxns The maximum number of transactions to return.
   * @param outputBuffers A list to populate with output buffers. When
   *                      concatenated, these form a full response.
   * @return The number of transactions contained within the set of output
   *         buffers.
   * @throws IOException If transactions are requested which cannot be served
   *                     by this cache.
   */
  int retrieveEdits(long requestedStartTxn, int maxTxns,
      List<ByteBuffer> outputBuffers) throws IOException {
    int txnCount = 0;

    try (AutoCloseableLock l = readLock.acquire()) {
      if (lowestTxnId == INVALID_TXN_ID || requestedStartTxn < lowestTxnId) {
        throw getCacheMissException(requestedStartTxn);
      } else if (requestedStartTxn > highestTxnId) {
        return 0;
      }
      outputBuffers.add(layoutHeader);
      Iterator<Map.Entry<Long, byte[]>> incrBuffIter =
          dataMap.tailMap(dataMap.floorKey(requestedStartTxn), true)
              .entrySet().iterator();
      long prevTxn = requestedStartTxn;
      byte[] prevBuf = null;
      // Stop when maximum transactions reached...
      while ((txnCount < maxTxns) &&
          // ... or there are no more entries ...
          (incrBuffIter.hasNext() || prevBuf != null)) {
        long currTxn;
        byte[] currBuf;
        if (incrBuffIter.hasNext()) {
          Map.Entry<Long, byte[]> ent = incrBuffIter.next();
          currTxn = ent.getKey();
          currBuf = ent.getValue();
        } else {
          // This accounts for the trailing entry
          currTxn = highestTxnId + 1;
          currBuf = null;
        }
        if (prevBuf != null) { // True except for the first loop iteration
          outputBuffers.add(ByteBuffer.wrap(prevBuf));
          // if prevTxn < requestedStartTxn, the extra transactions will get
          // removed after the loop, so don't include them in the txn count
          txnCount += currTxn - Math.max(requestedStartTxn, prevTxn);
        }
        prevTxn = currTxn;
        prevBuf = currBuf;
      }
      // Release the lock before doing operations on the buffers (deserializing
      // to find transaction boundaries, and copying into an output buffer)
    }
    // Remove extra leading transactions in the first buffer
    ByteBuffer firstBuf = outputBuffers.get(1); // 0th is the header
    firstBuf.position(
        findTransactionPosition(firstBuf.array(), requestedStartTxn));
    // Remove trailing transactions in the last buffer if necessary
    if (txnCount > maxTxns) {
      ByteBuffer lastBuf = outputBuffers.get(outputBuffers.size() - 1);
      int limit =
          findTransactionPosition(lastBuf.array(), requestedStartTxn + maxTxns);
      lastBuf.limit(limit);
      txnCount = maxTxns;
    }

    return txnCount;
  }

  /**
   * Store a batch of serialized edits into this cache. Removes old batches
   * as necessary to keep the total size of the cache below the capacity.
   * See the class Javadoc for more info.
   *
   * This attempts to always handle malformed inputs gracefully rather than
   * throwing an exception, to allow the rest of the Journal's operations
   * to proceed normally.
   *
   * @param inputData A buffer containing edits in serialized form
   * @param newStartTxn The txn ID of the first edit in {@code inputData}
   * @param newEndTxn The txn ID of the last edit in {@code inputData}
   * @param newLayoutVersion The version of the layout used to serialize
   *                         the edits
   */
  void storeEdits(byte[] inputData, long newStartTxn, long newEndTxn,
      int newLayoutVersion) {
    if (newStartTxn < 0 || newEndTxn < newStartTxn) {
      Journal.LOG.error(String.format("Attempted to cache data of length %d " +
          "with newStartTxn %d and newEndTxn %d",
          inputData.length, newStartTxn, newEndTxn));
      return;
    }
    try (AutoCloseableLock l = writeLock.acquire()) {
      if (newLayoutVersion != layoutVersion) {
        try {
          updateLayoutVersion(newLayoutVersion, newStartTxn);
        } catch (IOException ioe) {
          Journal.LOG.error(String.format("Unable to save new edits [%d, %d] " +
              "due to exception when updating to new layout version %d",
              newStartTxn, newEndTxn, newLayoutVersion), ioe);
          return;
        }
      } else if (lowestTxnId == INVALID_TXN_ID) {
        Journal.LOG.info("Initializing edits cache starting from txn ID " +
            newStartTxn);
        initialize(newStartTxn);
      } else if (highestTxnId + 1 != newStartTxn) {
        // Cache is out of sync; clear to avoid storing noncontiguous regions
        Journal.LOG.error(String.format("Edits cache is out of sync; " +
            "looked for next txn id at %d but got start txn id for " +
            "cache put request at %d. Reinitializing at new request.",
            highestTxnId + 1, newStartTxn));
        initialize(newStartTxn);
      }

      while ((totalSize + inputData.length) > capacity && !dataMap.isEmpty()) {
        Map.Entry<Long, byte[]> lowest = dataMap.firstEntry();
        dataMap.remove(lowest.getKey());
        totalSize -= lowest.getValue().length;
      }
      if (inputData.length > capacity) {
        initialize(INVALID_TXN_ID);
        Journal.LOG.warn(String.format("A single batch of edits was too " +
                "large to fit into the cache: startTxn = %d, endTxn = %d, " +
                "input length = %d. The capacity of the cache (%s) must be " +
                "increased for it to work properly (current capacity %d)." +
                "Cache is now empty.",
            newStartTxn, newEndTxn, inputData.length,
            DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY, capacity));
        return;
      }
      if (dataMap.isEmpty()) {
        lowestTxnId = newStartTxn;
      } else {
        lowestTxnId = dataMap.firstKey();
      }

      dataMap.put(newStartTxn, inputData);
      highestTxnId = newEndTxn;
      totalSize += inputData.length;
    }
  }

  /**
   * Skip through a given stream of edits until the given transaction ID is
   * found. Return the number of bytes that appear prior to the given
   * transaction.
   *
   * @param buf A buffer containing a stream of serialized edits
   * @param txnId The transaction ID to search for
   * @return The number of bytes appearing in {@code buf} <i>before</i>
   *         the start of the transaction with ID {@code txnId}.
   */
  private int findTransactionPosition(byte[] buf, long txnId)
      throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(buf);
    FSEditLogLoader.PositionTrackingInputStream tracker =
        new FSEditLogLoader.PositionTrackingInputStream(bais);
    FSEditLogOp.Reader reader = FSEditLogOp.Reader.create(
        new DataInputStream(tracker), tracker, layoutVersion);
    long previousPos = 0;
    while (reader.scanOp() < txnId) {
      previousPos = tracker.getPos();
    }
    // tracker is backed by a byte[]; position cannot go above an integer
    return (int) previousPos;
  }

  /**
   * Update the layout version of the cache. This clears out all existing
   * entries, and populates the new layout version and header for that version.
   *
   * @param newLayoutVersion The new layout version to be stored in the cache
   * @param newStartTxn The new lowest transaction in the cache
   */
  private void updateLayoutVersion(int newLayoutVersion, long newStartTxn)
      throws IOException {
    StringBuilder logMsg = new StringBuilder()
        .append("Updating edits cache to use layout version ")
        .append(newLayoutVersion)
        .append(" starting from txn ID ")
        .append(newStartTxn);
    if (layoutVersion != INVALID_LAYOUT_VERSION) {
      logMsg.append("; previous version was ").append(layoutVersion)
          .append("; old entries will be cleared.");
    }
    Journal.LOG.info(logMsg.toString());
    initialize(newStartTxn);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EditLogFileOutputStream.writeHeader(newLayoutVersion,
        new DataOutputStream(baos));
    layoutVersion = newLayoutVersion;
    layoutHeader = ByteBuffer.wrap(baos.toByteArray());
  }

  /**
   * Initialize the cache back to a clear state.
   *
   * @param newInitialTxnId The new lowest transaction ID stored in the cache.
   *                        This should be {@value INVALID_TXN_ID} if the cache
   *                        is to remain empty at this time.
   */
  private void initialize(long newInitialTxnId) {
    dataMap.clear();
    totalSize = 0;
    initialTxnId = newInitialTxnId;
    lowestTxnId = initialTxnId;
    highestTxnId = INVALID_TXN_ID; // this will be set later
  }

  /**
   * Return the underlying data buffer used to store information about the
   * given transaction ID.
   *
   * @param txnId Transaction ID whose containing buffer should be fetched.
   * @return The data buffer for the transaction
   */
  @VisibleForTesting
  byte[] getRawDataForTests(long txnId) {
    try (AutoCloseableLock l = readLock.acquire()) {
      return dataMap.floorEntry(txnId).getValue();
    }
  }

  private CacheMissException getCacheMissException(long requestedTxnId) {
    if (lowestTxnId == INVALID_TXN_ID) {
      return new CacheMissException(0, "Cache is empty; either it was never " +
          "written to or the last write overflowed the cache capacity.");
    } else if (requestedTxnId < initialTxnId) {
      return new CacheMissException(initialTxnId - requestedTxnId,
          "Cache started at txn ID %d but requested txns starting at %d.",
          initialTxnId, requestedTxnId);
    } else {
      return new CacheMissException(lowestTxnId - requestedTxnId,
          "Oldest txn ID available in the cache is %d, but requested txns " +
              "starting at %d. The cache size (%s) may need to be increased " +
              "to hold more transactions (currently %d bytes containing %d " +
              "transactions)", lowestTxnId, requestedTxnId,
          DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY, capacity,
          highestTxnId - lowestTxnId + 1);
    }
  }

  static class CacheMissException extends IOException {

    private static final long serialVersionUID = 0L;

    private final long cacheMissAmount;

    CacheMissException(long cacheMissAmount, String msgFormat,
        Object... msgArgs) {
      super(String.format(msgFormat, msgArgs));
      this.cacheMissAmount = cacheMissAmount;
    }

    long getCacheMissAmount() {
      return cacheMissAmount;
    }

  }

}
