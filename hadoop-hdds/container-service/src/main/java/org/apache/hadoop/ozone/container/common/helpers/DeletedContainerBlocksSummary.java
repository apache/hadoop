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
package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A helper class to wrap the info about under deletion container blocks.
 */
public final class DeletedContainerBlocksSummary {

  private final List<DeletedBlocksTransaction> blocks;
  // key : txID
  // value : times of this tx has been processed
  private final Map<Long, Integer> txSummary;
  // key : container name
  // value : the number of blocks need to be deleted in this container
  // if the message contains multiple entries for same block,
  // blocks will be merged
  private final Map<Long, Integer> blockSummary;
  // total number of blocks in this message
  private int numOfBlocks;

  private DeletedContainerBlocksSummary(List<DeletedBlocksTransaction> blocks) {
    this.blocks = blocks;
    txSummary = Maps.newHashMap();
    blockSummary = Maps.newHashMap();
    blocks.forEach(entry -> {
      txSummary.put(entry.getTxID(), entry.getCount());
      if (blockSummary.containsKey(entry.getContainerID())) {
        blockSummary.put(entry.getContainerID(),
            blockSummary.get(entry.getContainerID())
                + entry.getLocalIDCount());
      } else {
        blockSummary.put(entry.getContainerID(), entry.getLocalIDCount());
      }
      numOfBlocks += entry.getLocalIDCount();
    });
  }

  public static DeletedContainerBlocksSummary getFrom(
      List<DeletedBlocksTransaction> blocks) {
    return new DeletedContainerBlocksSummary(blocks);
  }

  public int getNumOfBlocks() {
    return numOfBlocks;
  }

  public int getNumOfContainers() {
    return blockSummary.size();
  }

  public String getTXIDs() {
    return String.join(",", txSummary.keySet()
        .stream().map(String::valueOf).collect(Collectors.toList()));
  }

  public String getTxIDSummary() {
    List<String> txSummaryEntry = txSummary.entrySet().stream()
        .map(entry -> entry.getKey() + "(" + entry.getValue() + ")")
        .collect(Collectors.toList());
    return "[" + String.join(",", txSummaryEntry) + "]";
  }

  @Override public String toString() {
    StringBuffer sb = new StringBuffer();
    for (DeletedBlocksTransaction blks : blocks) {
      sb.append(" ")
          .append("TXID=")
          .append(blks.getTxID())
          .append(", ")
          .append("TimesProceed=")
          .append(blks.getCount())
          .append(", ")
          .append(blks.getContainerID())
          .append(" : [")
          .append(StringUtils.join(',', blks.getLocalIDList())).append("]")
          .append("\n");
    }
    return sb.toString();
  }
}
