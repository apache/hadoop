/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers
    .DeletedContainerBlocksSummary;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine
    .SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlockCommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_NOT_FOUND;

/**
 * Handle block deletion commands.
 */
public class DeleteBlocksCommandHandler implements CommandHandler {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeleteBlocksCommandHandler.class);

  private final ContainerSet containerSet;
  private final Configuration conf;
  private int invocationCount;
  private long totalTime;
  private boolean cmdExecuted;

  public DeleteBlocksCommandHandler(ContainerSet cset,
      Configuration conf) {
    this.containerSet = cset;
    this.conf = conf;
  }

  @Override
  public void handle(SCMCommand command, OzoneContainer container,
      StateContext context, SCMConnectionManager connectionManager) {
    cmdExecuted = false;
    long startTime = Time.monotonicNow();
    ContainerBlocksDeletionACKProto blockDeletionACK = null;
    try {
      if (command.getType() != SCMCommandProto.Type.deleteBlocksCommand) {
        LOG.warn("Skipping handling command, expected command "
                + "type {} but found {}",
            SCMCommandProto.Type.deleteBlocksCommand, command.getType());
        return;
      }
      LOG.debug("Processing block deletion command.");
      invocationCount++;

      // move blocks to deleting state.
      // this is a metadata update, the actual deletion happens in another
      // recycling thread.
      DeleteBlocksCommand cmd = (DeleteBlocksCommand) command;
      List<DeletedBlocksTransaction> containerBlocks = cmd.blocksTobeDeleted();

      DeletedContainerBlocksSummary summary =
          DeletedContainerBlocksSummary.getFrom(containerBlocks);
      LOG.info("Start to delete container blocks, TXIDs={}, "
              + "numOfContainers={}, numOfBlocks={}",
          summary.getTxIDSummary(),
          summary.getNumOfContainers(),
          summary.getNumOfBlocks());

      ContainerBlocksDeletionACKProto.Builder resultBuilder =
          ContainerBlocksDeletionACKProto.newBuilder();
      containerBlocks.forEach(entry -> {
        DeleteBlockTransactionResult.Builder txResultBuilder =
            DeleteBlockTransactionResult.newBuilder();
        txResultBuilder.setTxID(entry.getTxID());
        long containerId = entry.getContainerID();
        try {
          Container cont = containerSet.getContainer(containerId);
          if (cont == null) {
            throw new StorageContainerException("Unable to find the container "
                + containerId, CONTAINER_NOT_FOUND);
          }
          ContainerProtos.ContainerType containerType = cont.getContainerType();
          switch (containerType) {
          case KeyValueContainer:
            KeyValueContainerData containerData = (KeyValueContainerData)
                cont.getContainerData();
            deleteKeyValueContainerBlocks(containerData, entry);
            txResultBuilder.setContainerID(containerId)
                .setSuccess(true);
            break;
          default:
            LOG.error(
                "Delete Blocks Command Handler is not implemented for " +
                    "containerType {}", containerType);
          }
        } catch (IOException e) {
          LOG.warn("Failed to delete blocks for container={}, TXID={}",
              entry.getContainerID(), entry.getTxID(), e);
          txResultBuilder.setContainerID(containerId)
              .setSuccess(false);
        }
        resultBuilder.addResults(txResultBuilder.build())
            .setDnId(context.getParent().getDatanodeDetails()
                .getUuid().toString());
      });
      blockDeletionACK = resultBuilder.build();

      // Send ACK back to SCM as long as meta updated
      // TODO Or we should wait until the blocks are actually deleted?
      if (!containerBlocks.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending following block deletion ACK to SCM");
          for (DeleteBlockTransactionResult result : blockDeletionACK
              .getResultsList()) {
            LOG.debug(result.getTxID() + " : " + result.getSuccess());
          }
        }
      }
      cmdExecuted = true;
    } finally {
      final ContainerBlocksDeletionACKProto deleteAck =
          blockDeletionACK;
      Consumer<CommandStatus> statusUpdater = (cmdStatus) -> {
        cmdStatus.setStatus(cmdExecuted);
        ((DeleteBlockCommandStatus) cmdStatus).setBlocksDeletionAck(deleteAck);
      };
      updateCommandStatus(context, command, statusUpdater, LOG);
      long endTime = Time.monotonicNow();
      totalTime += endTime - startTime;
    }
  }

  /**
   * Move a bunch of blocks from a container to deleting state. This is a meta
   * update, the actual deletes happen in async mode.
   *
   * @param containerData - KeyValueContainerData
   * @param delTX a block deletion transaction.
   * @throws IOException if I/O error occurs.
   */
  private void deleteKeyValueContainerBlocks(
      KeyValueContainerData containerData, DeletedBlocksTransaction delTX)
      throws IOException {
    long containerId = delTX.getContainerID();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing Container : {}, DB path : {}", containerId,
          containerData.getMetadataPath());
    }

    if (delTX.getTxID() < containerData.getDeleteTransactionId()) {
      LOG.debug(String.format("Ignoring delete blocks for containerId: %d."
              + " Outdated delete transactionId %d < %d", containerId,
          delTX.getTxID(), containerData.getDeleteTransactionId()));
      return;
    }

    int newDeletionBlocks = 0;
    try(ReferenceCountedDB containerDB =
            BlockUtils.getDB(containerData, conf)) {
      for (Long blk : delTX.getLocalIDList()) {
        BatchOperation batch = new BatchOperation();
        byte[] blkBytes = Longs.toByteArray(blk);
        byte[] blkInfo = containerDB.getStore().get(blkBytes);
        if (blkInfo != null) {
          byte[] deletingKeyBytes =
              DFSUtil.string2Bytes(OzoneConsts.DELETING_KEY_PREFIX + blk);
          byte[] deletedKeyBytes =
              DFSUtil.string2Bytes(OzoneConsts.DELETED_KEY_PREFIX + blk);
          if (containerDB.getStore().get(deletingKeyBytes) != null
              || containerDB.getStore().get(deletedKeyBytes) != null) {
            LOG.debug(String.format(
                "Ignoring delete for block %d in container %d."
                    + " Entry already added.", blk, containerId));
            continue;
          }
          // Found the block in container db,
          // use an atomic update to change its state to deleting.
          batch.put(deletingKeyBytes, blkInfo);
          batch.delete(blkBytes);
          try {
            containerDB.getStore().writeBatch(batch);
            newDeletionBlocks++;
            LOG.debug("Transited Block {} to DELETING state in container {}",
                blk, containerId);
          } catch (IOException e) {
            // if some blocks failed to delete, we fail this TX,
            // without sending this ACK to SCM, SCM will resend the TX
            // with a certain number of retries.
            throw new IOException(
                "Failed to delete blocks for TXID = " + delTX.getTxID(), e);
          }
        } else {
          LOG.debug("Block {} not found or already under deletion in"
              + " container {}, skip deleting it.", blk, containerId);
        }
      }

      containerDB.getStore()
          .put(DFSUtil.string2Bytes(OzoneConsts.DELETE_TRANSACTION_KEY_PREFIX),
              Longs.toByteArray(delTX.getTxID()));
      containerData
          .updateDeleteTransactionId(delTX.getTxID());
      // update pending deletion blocks count in in-memory container status
      containerData.incrPendingDeletionBlocks(newDeletionBlocks);
    }
  }

  @Override
  public SCMCommandProto.Type getCommandType() {
    return SCMCommandProto.Type.deleteBlocksCommand;
  }

  @Override
  public int getInvocationCount() {
    return this.invocationCount;
  }

  @Override
  public long getAverageRunTime() {
    if (invocationCount > 0) {
      return totalTime / invocationCount;
    }
    return 0;
  }
}
