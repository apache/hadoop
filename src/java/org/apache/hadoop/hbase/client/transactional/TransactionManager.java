/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.ipc.TransactionalRegionInterface;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Transaction Manager. Responsible for committing transactions.
 * 
 */
public class TransactionManager {
  static final Log LOG = LogFactory.getLog(TransactionManager.class);

  private final HConnection connection;
  private final TransactionLogger transactionLogger;

  /**
   * @param conf
   */
  public TransactionManager(final HBaseConfiguration conf) {
    this(LocalTransactionLogger.getInstance(), conf);
  }

  /**
   * @param transactionLogger
   * @param conf
   */
  public TransactionManager(final TransactionLogger transactionLogger,
      final HBaseConfiguration conf) {
    this.transactionLogger = transactionLogger;
    connection = HConnectionManager.getConnection(conf);
  }

  /**
   * Called to start a transaction.
   * 
   * @return new transaction state
   */
  public TransactionState beginTransaction() {
    long transactionId = transactionLogger.createNewTransactionLog();
    LOG.debug("Begining transaction " + transactionId);
    return new TransactionState(transactionId);
  }

  /**
   * Try and commit a transaction.
   * 
   * @param transactionState
   * @throws IOException
   * @throws CommitUnsuccessfulException
   */
  public void tryCommit(final TransactionState transactionState)
      throws CommitUnsuccessfulException, IOException {
    LOG.debug("atempting to commit trasaction: " + transactionState.toString());

    try {
      for (HRegionLocation location : transactionState
          .getParticipatingRegions()) {
        TransactionalRegionInterface transactionalRegionServer = (TransactionalRegionInterface) connection
            .getHRegionConnection(location.getServerAddress());
        boolean canCommit = transactionalRegionServer.commitRequest(location
            .getRegionInfo().getRegionName(), transactionState
            .getTransactionId());
        if (LOG.isTraceEnabled()) {
          LOG.trace("Region ["
              + location.getRegionInfo().getRegionNameAsString() + "] votes "
              + (canCommit ? "to commit" : "to abort") + " transaction "
              + transactionState.getTransactionId());
        }

        if (!canCommit) {
          LOG.debug("Aborting [" + transactionState.getTransactionId() + "]");
          abort(transactionState, location);
          throw new CommitUnsuccessfulException();
        }
      }

      LOG.debug("Commiting [" + transactionState.getTransactionId() + "]");

      transactionLogger.setStatusForTransaction(transactionState
          .getTransactionId(), TransactionLogger.TransactionStatus.COMMITTED);

      for (HRegionLocation location : transactionState
          .getParticipatingRegions()) {
        TransactionalRegionInterface transactionalRegionServer = (TransactionalRegionInterface) connection
            .getHRegionConnection(location.getServerAddress());
        transactionalRegionServer.commit(location.getRegionInfo()
            .getRegionName(), transactionState.getTransactionId());
      }
    } catch (RemoteException e) {
      LOG.debug("Commit of transaction [" + transactionState.getTransactionId()
          + "] was unsucsessful", e);
      // FIXME, think about the what ifs
      throw new CommitUnsuccessfulException(e);
    }
    // Tran log can be deleted now ...
  }

  /**
   * Abort a s transaction.
   * 
   * @param transactionState
   * @throws IOException
   */
  public void abort(final TransactionState transactionState) throws IOException {
    abort(transactionState, null);
  }

  private void abort(final TransactionState transactionState,
      final HRegionLocation locationToIgnore) throws IOException {
    transactionLogger.setStatusForTransaction(transactionState
        .getTransactionId(), TransactionLogger.TransactionStatus.ABORTED);

    for (HRegionLocation location : transactionState.getParticipatingRegions()) {
      if (locationToIgnore != null && location.equals(locationToIgnore)) {
        continue;
      }

      TransactionalRegionInterface transactionalRegionServer = (TransactionalRegionInterface) connection
          .getHRegionConnection(location.getServerAddress());

      transactionalRegionServer.abort(location.getRegionInfo().getRegionName(),
          transactionState.getTransactionId());
    }
  }
}
