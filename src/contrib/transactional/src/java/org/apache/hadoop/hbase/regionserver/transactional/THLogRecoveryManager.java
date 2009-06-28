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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.transactional.TransactionLogger;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;

/**
 * Responsible recovering transactional information from the HLog.
 */
class THLogRecoveryManager {
  private static final Log LOG = LogFactory
      .getLog(THLogRecoveryManager.class);

  private final FileSystem fileSystem;
  private final HRegionInfo regionInfo;
  private final HBaseConfiguration conf;

  /**
   * @param region
   */
  public THLogRecoveryManager(final TransactionalRegion region) {
    this.fileSystem = region.getFilesystem();
    this.regionInfo = region.getRegionInfo();
    this.conf = region.getConf();
  }

  // For Testing
  THLogRecoveryManager(final FileSystem fileSystem,
      final HRegionInfo regionInfo, final HBaseConfiguration conf) {
    this.fileSystem = fileSystem;
    this.regionInfo = regionInfo;
    this.conf = conf;
  }

  

  /**
   * @param reconstructionLog
   * @param maxSeqID
   * @param reporter
   * @return map of batch updates
   * @throws UnsupportedEncodingException
   * @throws IOException
   */
  public Map<Long, List<KeyValue>> getCommitsFromLog(
      final Path reconstructionLog, final long maxSeqID,
      final Progressable reporter) throws UnsupportedEncodingException,
      IOException {
    if (reconstructionLog == null || !fileSystem.exists(reconstructionLog)) {
      // Nothing to do.
      return null;
    }
    // Check its not empty.
    FileStatus[] stats = fileSystem.listStatus(reconstructionLog);
    if (stats == null || stats.length == 0) {
      LOG.warn("Passed reconstruction log " + reconstructionLog
          + " is zero-length");
      return null;
    }

    SortedMap<Long, List<KeyValue>> pendingTransactionsById = new TreeMap<Long, List<KeyValue>>();
    SortedMap<Long, List<KeyValue>> commitedTransactionsById = new TreeMap<Long, List<KeyValue>>();
    Set<Long> abortedTransactions = new HashSet<Long>();

    SequenceFile.Reader logReader = new SequenceFile.Reader(fileSystem,
        reconstructionLog, conf);
    
      try {
      THLogKey key = new THLogKey();
      KeyValue val = new KeyValue();
      long skippedEdits = 0;
      long totalEdits = 0;
      long startCount = 0;
      long writeCount = 0;
      long abortCount = 0;
      long commitCount = 0;
      // How many edits to apply before we send a progress report.
      int reportInterval = conf.getInt("hbase.hstore.report.interval.edits",
          2000);

      while (logReader.next(key, val)) {
        LOG.debug("Processing edit: key: " + key.toString() + " val: "
            + val.toString());
        if (key.getLogSeqNum() < maxSeqID) {
          skippedEdits++;
          continue;
        }

        if (key.getTrxOp() == null || !Bytes.equals(key.getRegionName(), regionInfo.getRegionName())) {
          continue;
        }
        long transactionId = key.getTransactionId();

        List<KeyValue> updates = pendingTransactionsById.get(transactionId);
        switch (key.getTrxOp()) {

        case START:
          if (updates != null || abortedTransactions.contains(transactionId)
              || commitedTransactionsById.containsKey(transactionId)) {
            LOG.error("Processing start for transaction: " + transactionId
                + ", but have already seen start message");
            throw new IOException("Corrupted transaction log");
          }
          updates = new LinkedList<KeyValue>();
          pendingTransactionsById.put(transactionId, updates);
          startCount++;
          break;

        case OP:
          if (updates == null) {
            LOG.error("Processing edit for transaction: " + transactionId
                + ", but have not seen start message");
            throw new IOException("Corrupted transaction log");
          }

          updates.add(val);
          val = new KeyValue();
          writeCount++;
          break;

        case ABORT:
          if (updates == null) {
            LOG.error("Processing abort for transaction: " + transactionId
                + ", but have not seen start message");
            throw new IOException("Corrupted transaction log");
          }
          abortedTransactions.add(transactionId);
          pendingTransactionsById.remove(transactionId);
          abortCount++;
          break;

        case COMMIT:
          if (updates == null) {
            LOG.error("Processing commit for transaction: " + transactionId
                + ", but have not seen start message");
            throw new IOException("Corrupted transaction log");
          }
          if (abortedTransactions.contains(transactionId)) {
            LOG.error("Processing commit for transaction: " + transactionId
                + ", but also have abort message");
            throw new IOException("Corrupted transaction log");
          }
          if (updates.size() == 0) {
            LOG
                .warn("Transaciton " + transactionId
                    + " has no writes in log. ");
          }
          if (commitedTransactionsById.containsKey(transactionId)) {
            LOG.error("Processing commit for transaction: " + transactionId
                + ", but have already commited transaction with that id");
            throw new IOException("Corrupted transaction log");
          }
          pendingTransactionsById.remove(transactionId);
          commitedTransactionsById.put(transactionId, updates);
          commitCount++;
          break;
          default:
            throw new IllegalStateException("Unexpected log entry type");
        }
        totalEdits++;

        if (reporter != null && (totalEdits % reportInterval) == 0) {
          reporter.progress();
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read " + totalEdits + " tranasctional operations (skipped "
            + skippedEdits + " because sequence id <= " + maxSeqID + "): "
            + startCount + " starts, " + writeCount + " writes, " + abortCount
            + " aborts, and " + commitCount + " commits.");
      }
    } finally {
      logReader.close();
    }

    if (pendingTransactionsById.size() > 0) {
      resolvePendingTransaction(pendingTransactionsById, commitedTransactionsById);
    }
     

    return commitedTransactionsById;
  }
  
  private void resolvePendingTransaction(
      SortedMap<Long, List<KeyValue>> pendingTransactionsById,
      SortedMap<Long, List<KeyValue>> commitedTransactionsById) {
    LOG.info("Region log has " + pendingTransactionsById.size()
        + " unfinished transactions. Going to the transaction log to resolve");

    for (Entry<Long, List<KeyValue>> entry : pendingTransactionsById
        .entrySet()) {
      TransactionLogger.TransactionStatus transactionStatus = getGlobalTransactionLog()
          .getStatusForTransaction(entry.getKey());
      if (transactionStatus == null) {
        throw new RuntimeException("Cannot resolve tranasction ["
            + entry.getKey() + "] from global tx log.");
      }
      switch (transactionStatus) {
      case ABORTED:
        break;
      case COMMITTED:
        commitedTransactionsById.put(entry.getKey(), entry.getValue());
        break;
      case PENDING:
        break;
      }
    }
  }

  private TransactionLogger getGlobalTransactionLog() {
    return null;
  }
}
