/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.block;

import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.hdds.scm.container.Mapping;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;

/**
 * A wrapper class to hold info about datanode and all deleted block
 * transactions that will be sent to this datanode.
 */
public class DatanodeDeletedBlockTransactions {
  private int nodeNum;
  // The throttle size for each datanode.
  private int maximumAllowedTXNum;
  // Current counter of inserted TX.
  private int currentTXNum;
  private Mapping mappingService;
  // A list of TXs mapped to a certain datanode ID.
  private final ArrayListMultimap<UUID, DeletedBlocksTransaction>
      transactions;

  DatanodeDeletedBlockTransactions(Mapping mappingService,
      int maximumAllowedTXNum, int nodeNum) {
    this.transactions = ArrayListMultimap.create();
    this.mappingService = mappingService;
    this.maximumAllowedTXNum = maximumAllowedTXNum;
    this.nodeNum = nodeNum;
  }

  public boolean addTransaction(DeletedBlocksTransaction tx,
      Set<UUID> dnsWithTransactionCommitted) {
    Pipeline pipeline = null;
    try {
      ContainerWithPipeline containerWithPipeline =
          mappingService.getContainerWithPipeline(tx.getContainerID());
      if (containerWithPipeline.getContainerInfo().isContainerOpen()) {
        return false;
      }
      pipeline = containerWithPipeline.getPipeline();
    } catch (IOException e) {
      SCMBlockDeletingService.LOG.warn("Got container info error.", e);
      return false;
    }

    if (pipeline == null) {
      SCMBlockDeletingService.LOG.warn(
          "Container {} not found, continue to process next",
          tx.getContainerID());
      return false;
    }

    for (DatanodeDetails dd : pipeline.getMachines()) {
      UUID dnID = dd.getUuid();
      if (dnsWithTransactionCommitted == null ||
          !dnsWithTransactionCommitted.contains(dnID)) {
        // Transaction need not be sent to dns which have already committed it
        addTransactionToDN(dnID, tx);
      }
    }
    return true;
  }

  private void addTransactionToDN(UUID dnID, DeletedBlocksTransaction tx) {
    if (transactions.containsKey(dnID)) {
      List<DeletedBlocksTransaction> txs = transactions.get(dnID);
      if (txs != null && txs.size() < maximumAllowedTXNum) {
        boolean hasContained = false;
        for (DeletedBlocksTransaction t : txs) {
          if (t.getContainerID() == tx.getContainerID()) {
            hasContained = true;
            break;
          }
        }

        if (!hasContained) {
          txs.add(tx);
          currentTXNum++;
        }
      }
    } else {
      currentTXNum++;
      transactions.put(dnID, tx);
    }
    SCMBlockDeletingService.LOG
        .debug("Transaction added: {} <- TX({})", dnID, tx.getTxID());
  }

  Set<UUID> getDatanodeIDs() {
    return transactions.keySet();
  }

  boolean isEmpty() {
    return transactions.isEmpty();
  }

  boolean hasTransactions(UUID dnId) {
    return transactions.containsKey(dnId) &&
        !transactions.get(dnId).isEmpty();
  }

  List<DeletedBlocksTransaction> getDatanodeTransactions(UUID dnId) {
    return transactions.get(dnId);
  }

  List<String> getTransactionIDList(UUID dnId) {
    if (hasTransactions(dnId)) {
      return transactions.get(dnId).stream()
          .map(DeletedBlocksTransaction::getTxID).map(String::valueOf)
          .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  boolean isFull() {
    return currentTXNum >= maximumAllowedTXNum * nodeNum;
  }

  int getTXNum() {
    return currentTXNum;
  }
}