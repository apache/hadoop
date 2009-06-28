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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * A local, in-memory implementation of the transaction logger. Does not provide a global view, so 
 * it can't be relighed on by 
 * 
 */
public class LocalTransactionLogger implements TransactionLogger {

  private static LocalTransactionLogger instance;

  /**
   * Creates singleton if it does not exist
   * 
   * @return reference to singleton
   */
  public synchronized static LocalTransactionLogger getInstance() {
    if (instance == null) {
      instance = new LocalTransactionLogger();
    }
    return instance;
  }

  private Random random = new Random();
  private Map<Long, TransactionStatus> transactionIdToStatusMap = Collections
      .synchronizedMap(new HashMap<Long, TransactionStatus>());

  private LocalTransactionLogger() {
    // Enforce singlton
  }

  /** @return random longs to minimize possibility of collision */
  public long createNewTransactionLog() {
    long id;
    do {
      id = random.nextLong();
    } while (transactionIdToStatusMap.containsKey(id));
    transactionIdToStatusMap.put(id, TransactionStatus.PENDING);
    return id;
  }

  public TransactionStatus getStatusForTransaction(final long transactionId) {
    return transactionIdToStatusMap.get(transactionId);
  }

  public void setStatusForTransaction(final long transactionId,
      final TransactionStatus status) {
    transactionIdToStatusMap.put(transactionId, status);
  }

  public void forgetTransaction(long transactionId) {
    transactionIdToStatusMap.remove(transactionId);
  }

}
