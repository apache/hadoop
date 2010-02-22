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
package org.apache.hadoop.hbase.client.transactional;

/**
 * Simple interface used to provide a log about transaction status. Written to
 * by the client, and read by regionservers in case of failure. 
 * 
 */
public interface TransactionLogger {

  /** Transaction status values */
  enum TransactionStatus {
    /** Transaction is pending */
    PENDING,
    /** Transaction was committed */
    COMMITTED,
    /** Transaction was aborted */
    ABORTED
  }

  /**
   * Create a new transaction log. Return the transaction's globally unique id.
   * Log's initial value should be PENDING
   * 
   * @return transaction id
   */
  long createNewTransactionLog();

  /** Get the status of a transaction.
   * @param transactionId
   * @return transaction status
   */
  TransactionStatus getStatusForTransaction(long transactionId);

  /** Set the status for a transaction.
   * @param transactionId
   * @param status
   */
  void setStatusForTransaction(long transactionId, TransactionStatus status);
  
  /** This transaction's state is no longer needed.
   * 
   * @param transactionId
   */
  void forgetTransaction(long transactionId);

}
