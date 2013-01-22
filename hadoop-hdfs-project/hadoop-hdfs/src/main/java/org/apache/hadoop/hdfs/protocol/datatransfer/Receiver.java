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

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;

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
    default:
      throw new IOException("Unknown op " + op + " in data stream");
    }
  }

  /** Receive OP_READ_BLOCK */
  private void opReadBlock() throws IOException {
    OpReadBlockProto proto = OpReadBlockProto.parseFrom(vintPrefixed(in));
    readBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        proto.getOffset(),
        proto.getLen(),
        proto.getSendChecksums());
  }
  
  /** Receive OP_WRITE_BLOCK */
  private void opWriteBlock(DataInputStream in) throws IOException {
    final OpWriteBlockProto proto = OpWriteBlockProto.parseFrom(vintPrefixed(in));
    writeBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        PBHelper.convert(proto.getTargetsList()),
        PBHelper.convert(proto.getSource()),
        fromProto(proto.getStage()),
        proto.getPipelineSize(),
        proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
        proto.getLatestGenerationStamp(),
        fromProto(proto.getRequestedChecksum()));
  }

  /** Receive {@link Op#TRANSFER_BLOCK} */
  private void opTransferBlock(DataInputStream in) throws IOException {
    final OpTransferBlockProto proto =
      OpTransferBlockProto.parseFrom(vintPrefixed(in));
    transferBlock(PBHelper.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        PBHelper.convert(proto.getTargetsList()));
  }

  /** Receive OP_REPLACE_BLOCK */
  private void opReplaceBlock(DataInputStream in) throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.parseFrom(vintPrefixed(in));
    replaceBlock(PBHelper.convert(proto.getHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getToken()),
        proto.getDelHint(),
        PBHelper.convert(proto.getSource()));
  }

  /** Receive OP_COPY_BLOCK */
  private void opCopyBlock(DataInputStream in) throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.parseFrom(vintPrefixed(in));
    copyBlock(PBHelper.convert(proto.getHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getToken()));
  }

  /** Receive OP_BLOCK_CHECKSUM */
  private void opBlockChecksum(DataInputStream in) throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.parseFrom(vintPrefixed(in));
    
    blockChecksum(PBHelper.convert(proto.getHeader().getBlock()),
        PBHelper.convert(proto.getHeader().getToken()));
  }
}
