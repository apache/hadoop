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

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.continueTraceSpan;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmRequestProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.htrace.TraceScope;

/** Receiver */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Receiver implements DataTransferProtocol {
  protected DataInputStream in;
  
  /** Initialize a receiver for DataTransferProtocol with a socket. */
  protected void initialize(final DataInputStream in) {
    this.in = in;
  }

  /** Read an Op.  It also checks protocol version. */
  protected final Op readOp() throws IOException {
    final short version = in.readShort();
    if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
      throw new IOException( "Version Mismatch (Expected: " +
          DataTransferProtocol.DATA_TRANSFER_VERSION  +
          ", Received: " +  version + " )");
    }
    return Op.read(in);
  }

  /** Process op by the corresponding method. */
  protected final void processOp(Op op) throws IOException {
    switch(op) {
    case READ_BLOCK:
      opReadBlock();
      break;
    case WRITE_BLOCK:
      opWriteBlock(in);
      break;
    case REPLACE_BLOCK:
      opReplaceBlock(in);
      break;
    case COPY_BLOCK:
      opCopyBlock(in);
      break;
    case BLOCK_CHECKSUM:
      opBlockChecksum(in);
      break;
    case TRANSFER_BLOCK:
      opTransferBlock(in);
      break;
    case REQUEST_SHORT_CIRCUIT_FDS:
      opRequestShortCircuitFds(in);
      break;
    case RELEASE_SHORT_CIRCUIT_FDS:
      opReleaseShortCircuitFds(in);
      break;
    case REQUEST_SHORT_CIRCUIT_SHM:
      opRequestShortCircuitShm(in);
      break;
    default:
      throw new IOException("Unknown op " + op + " in data stream");
    }
  }

  static private CachingStrategy getCachingStrategy(CachingStrategyProto strategy) {
    Boolean dropBehind = strategy.hasDropBehind() ?
        strategy.getDropBehind() : null;
    Long readahead = strategy.hasReadahead() ?
        strategy.getReadahead() : null;
    return new CachingStrategy(dropBehind, readahead);
  }

  /** Receive OP_READ_BLOCK */
  private void opReadBlock() throws IOException {
    OpReadBlockProto proto = OpReadBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      readBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        proto.getOffset(),
        proto.getLen(),
        proto.getSendChecksums(),
        (proto.hasCachingStrategy() ?
            getCachingStrategy(proto.getCachingStrategy()) :
          CachingStrategy.newDefaultStrategy()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }
  
  /** Receive OP_WRITE_BLOCK */
  private void opWriteBlock(DataInputStream in) throws IOException {
    final OpWriteBlockProto proto = OpWriteBlockProto.parseFrom(vintPrefixed(in));
    final DatanodeInfo[] targets = PBHelper.convert(proto.getTargetsList());
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      writeBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
          PBHelper.convertStorageType(proto.getStorageType()),
          PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
          proto.getHeader().getClientName(),
          targets,
          PBHelper.convertStorageTypes(proto.getTargetStorageTypesList(), targets.length),
          PBHelper.convert(proto.getSource()),
          fromProto(proto.getStage()),
          proto.getPipelineSize(),
          proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
          proto.getLatestGenerationStamp(),
          fromProto(proto.getRequestedChecksum()),
          (proto.hasCachingStrategy() ?
              getCachingStrategy(proto.getCachingStrategy()) :
            CachingStrategy.newDefaultStrategy()),
          (proto.hasAllowLazyPersist() ? proto.getAllowLazyPersist() : false),
          (proto.hasPinning() ? proto.getPinning(): false),
          (PBHelper.convertBooleanList(proto.getTargetPinningsList())));
    } finally {
     if (traceScope != null) traceScope.close();
    }
  }

  /** Receive {@link Op#TRANSFER_BLOCK} */
  private void opTransferBlock(DataInputStream in) throws IOException {
    final OpTransferBlockProto proto =
      OpTransferBlockProto.parseFrom(vintPrefixed(in));
    final DatanodeInfo[] targets = PBHelper.convert(proto.getTargetsList());
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      transferBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
          PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
          proto.getHeader().getClientName(),
          targets,
          PBHelper.convertStorageTypes(proto.getTargetStorageTypesList(), targets.length));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive {@link Op#REQUEST_SHORT_CIRCUIT_FDS} */
  private void opRequestShortCircuitFds(DataInputStream in) throws IOException {
    final OpRequestShortCircuitAccessProto proto =
      OpRequestShortCircuitAccessProto.parseFrom(vintPrefixed(in));
    SlotId slotId = (proto.hasSlotId()) ? 
        PBHelper.convert(proto.getSlotId()) : null;
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      requestShortCircuitFds(PBHelper.convert(proto.getHeader().getBlock()),
          PBHelper.convert(proto.getHeader().getToken()),
          slotId, proto.getMaxVersion(),
          proto.getSupportsReceiptVerification());
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive {@link Op#RELEASE_SHORT_CIRCUIT_FDS} */
  private void opReleaseShortCircuitFds(DataInputStream in)
      throws IOException {
    final ReleaseShortCircuitAccessRequestProto proto =
      ReleaseShortCircuitAccessRequestProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getTraceInfo(),
        proto.getClass().getSimpleName());
    try {
      releaseShortCircuitFds(PBHelper.convert(proto.getSlotId()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive {@link Op#REQUEST_SHORT_CIRCUIT_SHM} */
  private void opRequestShortCircuitShm(DataInputStream in) throws IOException {
    final ShortCircuitShmRequestProto proto =
        ShortCircuitShmRequestProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getTraceInfo(),
        proto.getClass().getSimpleName());
    try {
      requestShortCircuitShm(proto.getClientName());
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive OP_REPLACE_BLOCK */
  private void opReplaceBlock(DataInputStream in) throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      replaceBlock(PBHelper.convert(proto.getHeader().getBlock()),
          PBHelper.convertStorageType(proto.getStorageType()),
          PBHelper.convert(proto.getHeader().getToken()),
          proto.getDelHint(),
          PBHelper.convert(proto.getSource()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive OP_COPY_BLOCK */
  private void opCopyBlock(DataInputStream in) throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      copyBlock(PBHelper.convert(proto.getHeader().getBlock()),
          PBHelper.convert(proto.getHeader().getToken()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /** Receive OP_BLOCK_CHECKSUM */
  private void opBlockChecksum(DataInputStream in) throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
    blockChecksum(PBHelper.convert(proto.getHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getToken()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }
}
