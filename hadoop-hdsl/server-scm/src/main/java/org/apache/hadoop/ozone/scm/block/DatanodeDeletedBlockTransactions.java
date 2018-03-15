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
package org.apache.hadoop.ozone.scm.block;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;

import com.google.common.collect.ArrayListMultimap;

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
  private final ArrayListMultimap<DatanodeID, DeletedBlocksTransaction>
      transactions;

  DatanodeDeletedBlockTransactions(Mapping mappingService,
      int maximumAllowedTXNum, int nodeNum) {
    this.transactions = ArrayListMultimap.create();
    this.mappingService = mappingService;
    this.maximumAllowedTXNum = maximumAllowedTXNum;
    this.nodeNum = nodeNum;
  }

  public void addTransaction(DeletedBlocksTransaction tx) throws IOException {
    ContainerInfo info = null;
    try {
      info = mappingService.getContainer(tx.getContainerName());
    } catch (IOException e) {
      SCMBlockDeletingService.LOG.warn("Got container info error.", e);
    }

    if (info == null) {
      SCMBlockDeletingService.LOG.warn(
          "Container {} not found, continue to process next",
          tx.getContainerName());
      return;
    }

    for (DatanodeID dnID : info.getPipeline().getMachines()) {
      if (transactions.containsKey(dnID)) {
        List<DeletedBlocksTransaction> txs = transactions.get(dnID);
        if (txs != null && txs.size() < maximumAllowedTXNum) {
          boolean hasContained = false;
          for (DeletedBlocksTransaction t : txs) {
            if (t.getContainerName().equals(tx.getContainerName())) {
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
      SCMBlockDeletingService.LOG.debug("Transaction added: {} <- TX({})", dnID,
          tx.getTxID());
    }
  }

  Set<DatanodeID> getDatanodes() {
    return transactions.keySet();
  }

  boolean isEmpty() {
    return transactions.isEmpty();
  }

  boolean hasTransactions(DatanodeID dnID) {
    return transactions.containsKey(dnID) && !transactions.get(dnID).isEmpty();
  }

  List<DeletedBlocksTransaction> getDatanodeTransactions(
      DatanodeID dnID) {
    return transactions.get(dnID);
  }

  List<String> getTransactionIDList(DatanodeID dnID) {
    if (hasTransactions(dnID)) {
      return transactions.get(dnID).stream()
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