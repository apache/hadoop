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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.toProto;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.toProtos;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.toProto;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.Message;

/** Sender */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Sender {
  /** Initialize a operation. */
  private static void op(final DataOutput out, final Op op
      ) throws IOException {
    out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    op.write(out);
  }

  private static void send(final DataOutputStream out, final Op opcode,
      final Message proto) throws IOException {
    op(out, opcode);
    proto.writeDelimitedTo(out);
    out.flush();
  }

  /** Send OP_READ_BLOCK */
  public static void opReadBlock(DataOutputStream out, ExtendedBlock blk,
      long blockOffset, long blockLen, String clientName,
      Token<BlockTokenIdentifier> blockToken)
      throws IOException {

    OpReadBlockProto proto = OpReadBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildClientHeader(blk, clientName, blockToken))
      .setOffset(blockOffset)
      .setLen(blockLen)
      .build();

    send(out, Op.READ_BLOCK, proto);
  }
  

  /** Send OP_WRITE_BLOCK */
  public static void opWriteBlock(DataOutputStream out, ExtendedBlock blk,
      int pipelineSize, BlockConstructionStage stage, long newGs,
      long minBytesRcvd, long maxBytesRcvd, String client, DatanodeInfo src,
      DatanodeInfo[] targets, Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    ClientOperationHeaderProto header = DataTransferProtoUtil.buildClientHeader(blk, client,
        blockToken);
    
    OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
      .setHeader(header)
      .addAllTargets(
          toProtos(targets, 1))
      .setStage(toProto(stage))
      .setPipelineSize(pipelineSize)
      .setMinBytesRcvd(minBytesRcvd)
      .setMaxBytesRcvd(maxBytesRcvd)
      .setLatestGenerationStamp(newGs);
    
    if (src != null) {
      proto.setSource(toProto(src));
    }

    send(out, Op.WRITE_BLOCK, proto.build());
  }

  /** Send {@link Op#TRANSFER_BLOCK} */
  public static void opTransferBlock(DataOutputStream out, ExtendedBlock blk,
      String client, DatanodeInfo[] targets,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    
    OpTransferBlockProto proto = OpTransferBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildClientHeader(
          blk, client, blockToken))
      .addAllTargets(toProtos(targets, 0))
      .build();

    send(out, Op.TRANSFER_BLOCK, proto);
  }

  /** Send OP_REPLACE_BLOCK */
  public static void opReplaceBlock(DataOutputStream out,
      ExtendedBlock blk, String delHint, DatanodeInfo src,
      Token<BlockTokenIdentifier> blockToken) throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .setDelHint(delHint)
      .setSource(toProto(src))
      .build();
    
    send(out, Op.REPLACE_BLOCK, proto);
  }

  /** Send OP_COPY_BLOCK */
  public static void opCopyBlock(DataOutputStream out, ExtendedBlock blk,
      Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .build();
    
    send(out, Op.COPY_BLOCK, proto);
  }

  /** Send OP_BLOCK_CHECKSUM */
  public static void opBlockChecksum(DataOutputStream out, ExtendedBlock blk,
      Token<BlockTokenIdentifier> blockToken)
      throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.newBuilder()
      .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
      .build();
    
    send(out, Op.BLOCK_CHECKSUM, proto);
  }
}
