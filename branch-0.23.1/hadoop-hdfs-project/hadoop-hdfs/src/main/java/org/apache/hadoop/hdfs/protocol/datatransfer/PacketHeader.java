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

/**
 * Header data for each packet that goes through the read/write pipelines.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PacketHeader {
  /** Header size for a packet */
  private static final int PROTO_SIZE = 
    PacketHeaderProto.newBuilder()
      .setOffsetInBlock(0)
      .setSeqno(0)
      .setLastPacketInBlock(false)
      .setDataLen(0)
      .build().getSerializedSize();
  public static final int PKT_HEADER_LEN =
    6 + PROTO_SIZE;

  private int packetLen;
  private PacketHeaderProto proto;

  public PacketHeader() {
  }

  public PacketHeader(int packetLen, long offsetInBlock, long seqno,
                      boolean lastPacketInBlock, int dataLen) {
    this.packetLen = packetLen;
    proto = PacketHeaderProto.newBuilder()
      .setOffsetInBlock(offsetInBlock)
      .setSeqno(seqno)
      .setLastPacketInBlock(lastPacketInBlock)
      .setDataLen(dataLen)
      .build();
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

  @Override
  public String toString() {
    return "PacketHeader with packetLen=" + packetLen +
      "Header data: " + 
      proto.toString();
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
   * Write the header into the buffer.
   * This requires that PKT_HEADER_LEN bytes are available.
   */
  public void putInBuffer(final ByteBuffer buf) {
    assert proto.getSerializedSize() == PROTO_SIZE
      : "Expected " + (PROTO_SIZE) + " got: " + proto.getSerializedSize();
    try {
      buf.putInt(packetLen);
      buf.putShort((short) proto.getSerializedSize());
      proto.writeTo(new ByteBufferOutputStream(buf));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void write(DataOutputStream out) throws IOException {
    assert proto.getSerializedSize() == PROTO_SIZE
    : "Expected " + (PROTO_SIZE) + " got: " + proto.getSerializedSize();
    out.writeInt(packetLen);
    out.writeShort(proto.getSerializedSize());
    proto.writeTo(out);
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
