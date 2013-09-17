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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PacketHeaderProto;
import org.apache.hadoop.hdfs.util.ByteBufferOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Header data for each packet that goes through the read/write pipelines.
 * Includes all of the information about the packet, excluding checksums and
 * actual data.
 * 
 * This data includes:
 *  - the offset in bytes into the HDFS block of the data in this packet
 *  - the sequence number of this packet in the pipeline
 *  - whether or not this is the last packet in the pipeline
 *  - the length of the data in this packet
 *  - whether or not this packet should be synced by the DNs.
 *  
 * When serialized, this header is written out as a protocol buffer, preceded
 * by a 4-byte integer representing the full packet length, and a 2-byte short
 * representing the header length.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PacketHeader {
  private static final int MAX_PROTO_SIZE = 
    PacketHeaderProto.newBuilder()
      .setOffsetInBlock(0)
      .setSeqno(0)
      .setLastPacketInBlock(false)
      .setDataLen(0)
      .setSyncBlock(false)
      .build().getSerializedSize();
  public static final int PKT_LENGTHS_LEN =
      Ints.BYTES + Shorts.BYTES;
  public static final int PKT_MAX_HEADER_LEN =
      PKT_LENGTHS_LEN + MAX_PROTO_SIZE;

  private int packetLen;
  private PacketHeaderProto proto;

  public PacketHeader() {
  }

  public PacketHeader(int packetLen, long offsetInBlock, long seqno,
                      boolean lastPacketInBlock, int dataLen, boolean syncBlock) {
    this.packetLen = packetLen;
    Preconditions.checkArgument(packetLen >= Ints.BYTES,
        "packet len %s should always be at least 4 bytes",
        packetLen);
    
    PacketHeaderProto.Builder builder = PacketHeaderProto.newBuilder()
      .setOffsetInBlock(offsetInBlock)
      .setSeqno(seqno)
      .setLastPacketInBlock(lastPacketInBlock)
      .setDataLen(dataLen);
      
    if (syncBlock) {
      // Only set syncBlock if it is specified.
      // This is wire-incompatible with Hadoop 2.0.0-alpha due to HDFS-3721
      // because it changes the length of the packet header, and BlockReceiver
      // in that version did not support variable-length headers.
      builder.setSyncBlock(syncBlock);
    }
      
    proto = builder.build();
  }

  public int getDataLen() {
    return proto.getDataLen();
  }

  public boolean isLastPacketInBlock() {
    return proto.getLastPacketInBlock();
  }

  public long getSeqno() {
    return proto.getSeqno();
  }

  public long getOffsetInBlock() {
    return proto.getOffsetInBlock();
  }

  public int getPacketLen() {
    return packetLen;
  }

  public boolean getSyncBlock() {
    return proto.getSyncBlock();
  }

  @Override
  public String toString() {
    return "PacketHeader with packetLen=" + packetLen +
      " header data: " + 
      proto.toString();
  }
  
  public void setFieldsFromData(
      int packetLen, byte[] headerData) throws InvalidProtocolBufferException {
    this.packetLen = packetLen;
    proto = PacketHeaderProto.parseFrom(headerData);
  }
  
  public void readFields(ByteBuffer buf) throws IOException {
    packetLen = buf.getInt();
    short protoLen = buf.getShort();
    byte[] data = new byte[protoLen];
    buf.get(data);
    proto = PacketHeaderProto.parseFrom(data);
  }
  
  public void readFields(DataInputStream in) throws IOException {
    this.packetLen = in.readInt();
    short protoLen = in.readShort();
    byte[] data = new byte[protoLen];
    in.readFully(data);
    proto = PacketHeaderProto.parseFrom(data);
  }

  /**
   * @return the number of bytes necessary to write out this header,
   * including the length-prefixing of the payload and header
   */
  public int getSerializedSize() {
    return PKT_LENGTHS_LEN + proto.getSerializedSize();
  }

  /**
   * Write the header into the buffer.
   * This requires that PKT_HEADER_LEN bytes are available.
   */
  public void putInBuffer(final ByteBuffer buf) {
    assert proto.getSerializedSize() <= MAX_PROTO_SIZE
      : "Expected " + (MAX_PROTO_SIZE) + " got: " + proto.getSerializedSize();
    try {
      buf.putInt(packetLen);
      buf.putShort((short) proto.getSerializedSize());
      proto.writeTo(new ByteBufferOutputStream(buf));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void write(DataOutputStream out) throws IOException {
    assert proto.getSerializedSize() <= MAX_PROTO_SIZE
    : "Expected " + (MAX_PROTO_SIZE) + " got: " + proto.getSerializedSize();
    out.writeInt(packetLen);
    out.writeShort(proto.getSerializedSize());
    proto.writeTo(out);
  }
  
  public byte[] getBytes() {
    ByteBuffer buf = ByteBuffer.allocate(getSerializedSize());
    putInBuffer(buf);
    return buf.array();
  }

  /**
   * Perform a sanity check on the packet, returning true if it is sane.
   * @param lastSeqNo the previous sequence number received - we expect the current
   * sequence number to be larger by 1.
   */
  public boolean sanityCheck(long lastSeqNo) {
    // We should only have a non-positive data length for the last packet
    if (proto.getDataLen() <= 0 && !proto.getLastPacketInBlock()) return false;
    // The last packet should not contain data
    if (proto.getLastPacketInBlock() && proto.getDataLen() != 0) return false;
    // Seqnos should always increase by 1 with each packet received
    if (proto.getSeqno() != lastSeqNo + 1) return false;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PacketHeader)) return false;
    PacketHeader other = (PacketHeader)o;
    return this.proto.equals(other.proto);
  }

  @Override
  public int hashCode() {
    return (int)proto.getSeqno();
  }
}
