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

package org.apache.hadoop.hdfs.server.common.sps;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatching block replica moves between datanodes to satisfy the storage
 * policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockDispatcher {
  private static final Logger LOG = LoggerFactory
      .getLogger(BlockDispatcher.class);

  private final boolean connectToDnViaHostname;
  private final int socketTimeout;
  private final int ioFileBufferSize;

  /**
   * Construct block dispatcher details.
   *
   * @param sockTimeout
   *          soTimeout
   * @param ioFileBuffSize
   *          file io buffer size
   * @param connectToDatanodeViaHostname
   *          true represents connect via hostname, false otw
   */
  public BlockDispatcher(int sockTimeout, int ioFileBuffSize,
      boolean connectToDatanodeViaHostname) {
    this.socketTimeout = sockTimeout;
    this.ioFileBufferSize = ioFileBuffSize;
    this.connectToDnViaHostname = connectToDatanodeViaHostname;
  }

  /**
   * Moves the given block replica to the given target node and wait for the
   * response.
   *
   * @param blkMovingInfo
   *          block to storage info
   * @param saslClient
   *          SASL for DataTransferProtocol on behalf of a client
   * @param eb
   *          extended block info
   * @param sock
   *          target node's socket
   * @param km
   *          for creation of an encryption key
   * @param accessToken
   *          connection block access token
   * @return status of the block movement
   */
  public BlockMovementStatus moveBlock(BlockMovingInfo blkMovingInfo,
      SaslDataTransferClient saslClient, ExtendedBlock eb, Socket sock,
      DataEncryptionKeyFactory km, Token<BlockTokenIdentifier> accessToken) {
    LOG.info("Start moving block:{} from src:{} to destin:{} to satisfy "
        + "storageType, sourceStoragetype:{} and destinStoragetype:{}",
        blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
        blkMovingInfo.getTarget(), blkMovingInfo.getSourceStorageType(),
        blkMovingInfo.getTargetStorageType());
    DataOutputStream out = null;
    DataInputStream in = null;
    try {
      NetUtils.connect(sock,
          NetUtils.createSocketAddr(
              blkMovingInfo.getTarget().getXferAddr(connectToDnViaHostname)),
          socketTimeout);
      // Set read timeout so that it doesn't hang forever against
      // unresponsive nodes. Datanode normally sends IN_PROGRESS response
      // twice within the client read timeout period (every 30 seconds by
      // default). Here, we make it give up after "socketTimeout * 5" period
      // of no response.
      sock.setSoTimeout(socketTimeout * 5);
      sock.setKeepAlive(true);
      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();
      LOG.debug("Connecting to datanode {}", blkMovingInfo.getTarget());

      IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, km, accessToken, blkMovingInfo.getTarget());
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(
          new BufferedOutputStream(unbufOut, ioFileBufferSize));
      in = new DataInputStream(
          new BufferedInputStream(unbufIn, ioFileBufferSize));
      sendRequest(out, eb, accessToken, blkMovingInfo.getSource(),
          blkMovingInfo.getTargetStorageType());
      receiveResponse(in);

      LOG.info(
          "Successfully moved block:{} from src:{} to destin:{} for"
              + " satisfying storageType:{}",
          blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
          blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType());
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
    } catch (BlockPinningException e) {
      // Pinned block won't be able to move to a different node. So, its not
      // required to do retries, just marked as SUCCESS.
      LOG.debug("Pinned block can't be moved, so skipping block:{}",
          blkMovingInfo.getBlock(), e);
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
    } catch (IOException e) {
      // TODO: handle failure retries
      LOG.warn(
          "Failed to move block:{} from src:{} to destin:{} to satisfy "
              + "storageType:{}",
          blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
          blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType(), e);
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_FAILURE;
    } finally {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      IOUtils.closeSocket(sock);
    }
  }

  /** Send a reportedBlock replace request to the output stream. */
  private static void sendRequest(DataOutputStream out, ExtendedBlock eb,
      Token<BlockTokenIdentifier> accessToken, DatanodeInfo source,
      StorageType targetStorageType) throws IOException {
    new Sender(out).replaceBlock(eb, targetStorageType, accessToken,
        source.getDatanodeUuid(), source, null);
  }

  /** Receive a reportedBlock copy response from the input stream. */
  private static void receiveResponse(DataInputStream in) throws IOException {
    BlockOpResponseProto response = BlockOpResponseProto
        .parseFrom(vintPrefixed(in));
    while (response.getStatus() == Status.IN_PROGRESS) {
      // read intermediate responses
      response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
    }
    String logInfo = "reportedBlock move is failed";
    DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
  }
}
