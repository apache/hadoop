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

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.toProto;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferTraceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockGroupChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockCrossNamespaceProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmRequestProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.tracing.TraceUtils;

import org.apache.hadoop.thirdparty.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Sender */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Sender implements DataTransferProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

  private final DataOutputStream out;

  /** Create a sender for DataTransferProtocol with a output stream. */
  public Sender(final DataOutputStream out) {
    this.out = out;
  }

  /** Initialize a operation. */
  private static void op(final DataOutput out, final Op op) throws IOException {
    out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    op.write(out);
  }

  private static void send(final DataOutputStream out, final Op opcode,
      final Message proto) throws IOException {
    LOG.trace("Sending DataTransferOp {}: {}",
        proto.getClass().getSimpleName(), proto);
    op(out, opcode);
    proto.writeDelimitedTo(out);
    out.flush();
  }

  static private CachingStrategyProto getCachingStrategy(
      CachingStrategy cachingStrategy) {
    CachingStrategyProto.Builder builder = CachingStrategyProto.newBuilder();
    if (cachingStrategy.getReadahead() != null) {
      builder.setReadahead(cachingStrategy.getReadahead());
    }
    if (cachingStrategy.getDropBehind() != null) {
      builder.setDropBehind(cachingStrategy.getDropBehind());
    }
    return builder.build();
  }

  @Override
  public void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException {

    OpReadBlockProto proto = OpReadBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(blk, clientName,
            blockToken))
        .setOffset(blockOffset)
        .setLen(length)
        .setSendChecksums(sendChecksum)
        .setCachingStrategy(getCachingStrategy(cachingStrategy))
        .build();

    send(out, Op.READ_BLOCK, proto);
  }


  @Override
  public void writeBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      final CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final boolean[] targetPinnings,
      final String storageId,
      final String[] targetStorageIds) throws IOException {
    ClientOperationHeaderProto header = DataTransferProtoUtil.buildClientHeader(
        blk, clientName, blockToken);

    ChecksumProto checksumProto =
        DataTransferProtoUtil.toProto(requestedChecksum);

    OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
        .setHeader(header)
        .setStorageType(PBHelperClient.convertStorageType(storageType))
        .addAllTargets(PBHelperClient.convert(targets, 1))
        .addAllTargetStorageTypes(
            PBHelperClient.convertStorageTypes(targetStorageTypes, 1))
        .setStage(toProto(stage))
        .setPipelineSize(pipelineSize)
        .setMinBytesRcvd(minBytesRcvd)
        .setMaxBytesRcvd(maxBytesRcvd)
        .setLatestGenerationStamp(latestGenerationStamp)
        .setRequestedChecksum(checksumProto)
        .setCachingStrategy(getCachingStrategy(cachingStrategy))
        .setAllowLazyPersist(allowLazyPersist)
        .setPinning(pinning)
        .addAllTargetPinnings(PBHelperClient.convert(targetPinnings, 1))
        .addAllTargetStorageIds(PBHelperClient.convert(targetStorageIds, 1));
    if (source != null) {
      proto.setSource(PBHelperClient.convertDatanodeInfo(source));
    }
    if (storageId != null) {
      proto.setStorageId(storageId);
    }

    send(out, Op.WRITE_BLOCK, proto.build());
  }

  @Override
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final String[] targetStorageIds) throws IOException {

    OpTransferBlockProto proto = OpTransferBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(
            blk, clientName, blockToken))
        .addAllTargets(PBHelperClient.convert(targets))
        .addAllTargetStorageTypes(
            PBHelperClient.convertStorageTypes(targetStorageTypes))
        .addAllTargetStorageIds(Arrays.asList(targetStorageIds))
        .build();

    send(out, Op.TRANSFER_BLOCK, proto);
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
      throws IOException {
    OpRequestShortCircuitAccessProto.Builder builder =
        OpRequestShortCircuitAccessProto.newBuilder()
            .setHeader(DataTransferProtoUtil.buildBaseHeader(
                blk, blockToken)).setMaxVersion(maxVersion);
    if (slotId != null) {
      builder.setSlotId(PBHelperClient.convert(slotId));
    }
    builder.setSupportsReceiptVerification(supportsReceiptVerification);
    OpRequestShortCircuitAccessProto proto = builder.build();
    send(out, Op.REQUEST_SHORT_CIRCUIT_FDS, proto);
  }

  @Override
  public void releaseShortCircuitFds(SlotId slotId) throws IOException {
    ReleaseShortCircuitAccessRequestProto.Builder builder =
        ReleaseShortCircuitAccessRequestProto.newBuilder().
            setSlotId(PBHelperClient.convert(slotId));
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      DataTransferTraceInfoProto.Builder traceInfoProtoBuilder =
          DataTransferTraceInfoProto.newBuilder().setSpanContext(
              TraceUtils.spanContextToByteString(span.getContext()));
      builder.setTraceInfo(traceInfoProtoBuilder);
    }
    ReleaseShortCircuitAccessRequestProto proto = builder.build();
    send(out, Op.RELEASE_SHORT_CIRCUIT_FDS, proto);
  }

  @Override
  public void requestShortCircuitShm(String clientName) throws IOException {
    ShortCircuitShmRequestProto.Builder builder =
        ShortCircuitShmRequestProto.newBuilder().
            setClientName(clientName);
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      DataTransferTraceInfoProto.Builder traceInfoProtoBuilder =
          DataTransferTraceInfoProto.newBuilder().setSpanContext(
              TraceUtils.spanContextToByteString(span.getContext()));
      builder.setTraceInfo(traceInfoProtoBuilder);
    }
    ShortCircuitShmRequestProto proto = builder.build();
    send(out, Op.REQUEST_SHORT_CIRCUIT_SHM, proto);
  }

  @Override
  public void replaceBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source,
      final String storageId) throws IOException {
    OpReplaceBlockProto.Builder proto = OpReplaceBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .setStorageType(PBHelperClient.convertStorageType(storageType))
        .setDelHint(delHint)
        .setSource(PBHelperClient.convertDatanodeInfo(source));
    if (storageId != null) {
      proto.setStorageId(storageId);
    }

    send(out, Op.REPLACE_BLOCK, proto.build());
  }

  @Override
  public void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .build();

    send(out, Op.COPY_BLOCK, proto);
  }

  @Override
  public void blockChecksum(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      BlockChecksumOptions blockChecksumOptions) throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .setBlockChecksumOptions(PBHelperClient.convert(blockChecksumOptions))
        .build();

    send(out, Op.BLOCK_CHECKSUM, proto);
  }

  @Override
  public void blockGroupChecksum(StripedBlockInfo stripedBlockInfo,
      Token<BlockTokenIdentifier> blockToken,
      long requestedNumBytes,
      BlockChecksumOptions blockChecksumOptions) throws IOException {
    OpBlockGroupChecksumProto proto = OpBlockGroupChecksumProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(
            stripedBlockInfo.getBlock(), blockToken))
        .setDatanodes(PBHelperClient.convertToProto(
            stripedBlockInfo.getDatanodes()))
        .addAllBlockTokens(PBHelperClient.convert(
            stripedBlockInfo.getBlockTokens()))
        .addAllBlockIndices(PBHelperClient
            .convertBlockIndices(stripedBlockInfo.getBlockIndices()))
        .setEcPolicy(PBHelperClient.convertErasureCodingPolicy(
            stripedBlockInfo.getErasureCodingPolicy()))
        .setRequestedNumBytes(requestedNumBytes)
        .setBlockChecksumOptions(PBHelperClient.convert(blockChecksumOptions))
        .build();

    send(out, Op.BLOCK_GROUP_CHECKSUM, proto);
  }

  @Override
  public void copyBlockCrossNamespace(ExtendedBlock sourceBlk,
      Token<BlockTokenIdentifier> sourceBlockToken, ExtendedBlock targetBlk,
      Token<BlockTokenIdentifier> targetBlockToken, DatanodeInfo targetDatanode)
      throws IOException {
    OpCopyBlockCrossNamespaceProto proto = OpCopyBlockCrossNamespaceProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(sourceBlk, sourceBlockToken))
        .setTargetBlock(PBHelperClient.convert(targetBlk))
        .setTargetToken(PBHelperClient.convert(targetBlockToken))
        .setTargetDatanode(PBHelperClient.convert(targetDatanode)).build();

    send(out, Op.COPY_BLOCK_CROSSNAMESPACE, proto);
  }
}
