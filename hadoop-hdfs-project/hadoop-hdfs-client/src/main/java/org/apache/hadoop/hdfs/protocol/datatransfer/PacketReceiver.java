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

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle reading packets one-at-a-time from the wire.
 * These packets are used both for reading and writing data to/from
 * DataNodes.
 */
@InterfaceAudience.Private
public class PacketReceiver implements Closeable {

  /**
   * The max size of any single packet. This prevents OOMEs when
   * invalid data is sent.
   */
  public static final int MAX_PACKET_SIZE = 16 * 1024 * 1024;

  static final Logger LOG = LoggerFactory.getLogger(PacketReceiver.class);

  private static final DirectBufferPool bufferPool = new DirectBufferPool();
  private final boolean useDirectBuffers;

  /**
   * The entirety of the most recently read packet.
   * The first PKT_LENGTHS_LEN bytes of this buffer are the
   * length prefixes.
   */
  private ByteBuffer curPacketBuf = null;

  /**
   * A slice of {@link #curPacketBuf} which contains just the checksums.
   */
  private ByteBuffer curChecksumSlice = null;

  /**
   * A slice of {@link #curPacketBuf} which contains just the data.
   */
  private ByteBuffer curDataSlice = null;

  /**
   * The packet header of the most recently read packet.
   */
  private PacketHeader curHeader;

  public PacketReceiver(boolean useDirectBuffers) {
    this.useDirectBuffers = useDirectBuffers;
    reallocPacketBuf(PacketHeader.PKT_LENGTHS_LEN);
  }

  public PacketHeader getHeader() {
    return curHeader;
  }

  public ByteBuffer getDataSlice() {
    return curDataSlice;
  }

  public ByteBuffer getChecksumSlice() {
    return curChecksumSlice;
  }

  /**
   * Reads all of the data for the next packet into the appropriate buffers.
   *
   * The data slice and checksum slice members will be set to point to the
   * user data and corresponding checksums. The header will be parsed and
   * set.
   */
  public void receiveNextPacket(ReadableByteChannel in) throws IOException {
    doRead(in, null);
  }

  /**
   * @see #receiveNextPacket(ReadableByteChannel)
   */
  public void receiveNextPacket(InputStream in) throws IOException {
    doRead(null, in);
  }

  private void doRead(ReadableByteChannel ch, InputStream in)
      throws IOException {
    // Each packet looks like:
    //   PLEN    HLEN      HEADER     CHECKSUMS  DATA
    //   32-bit  16-bit   <protobuf>  <variable length>
    //
    // PLEN:      Payload length
    //            = length(PLEN) + length(CHECKSUMS) + length(DATA)
    //            This length includes its own encoded length in
    //            the sum for historical reasons.
    //
    // HLEN:      Header length
    //            = length(HEADER)
    //
    // HEADER:    the actual packet header fields, encoded in protobuf
    // CHECKSUMS: the crcs for the data chunk. May be missing if
    //            checksums were not requested
    // DATA       the actual block data
    Preconditions.checkState(curHeader == null || !curHeader.isLastPacketInBlock());

    curPacketBuf.clear();
    curPacketBuf.limit(PacketHeader.PKT_LENGTHS_LEN);
    doReadFully(ch, in, curPacketBuf);
    curPacketBuf.flip();
    int payloadLen = curPacketBuf.getInt();

    if (payloadLen < Ints.BYTES) {
      // The "payload length" includes its own length. Therefore it
      // should never be less than 4 bytes
      throw new IOException("Invalid payload length " +
          payloadLen);
    }
    int dataPlusChecksumLen = payloadLen - Ints.BYTES;
    int headerLen = curPacketBuf.getShort();
    if (headerLen < 0) {
      throw new IOException("Invalid header length " + headerLen);
    }

    LOG.trace("readNextPacket: dataPlusChecksumLen={}, headerLen={}",
        dataPlusChecksumLen, headerLen);

    // Sanity check the buffer size so we don't allocate too much memory
    // and OOME.
    int totalLen = payloadLen + headerLen;
    if (totalLen < 0 || totalLen > MAX_PACKET_SIZE) {
      throw new IOException("Incorrect value for packet payload size: " +
                            payloadLen);
    }

    // Make sure we have space for the whole packet, and
    // read it.
    reallocPacketBuf(PacketHeader.PKT_LENGTHS_LEN +
        dataPlusChecksumLen + headerLen);
    curPacketBuf.clear();
    curPacketBuf.position(PacketHeader.PKT_LENGTHS_LEN);
    curPacketBuf.limit(PacketHeader.PKT_LENGTHS_LEN +
        dataPlusChecksumLen + headerLen);
    doReadFully(ch, in, curPacketBuf);
    curPacketBuf.flip();
    curPacketBuf.position(PacketHeader.PKT_LENGTHS_LEN);

    // Extract the header from the front of the buffer (after the length prefixes)
    byte[] headerBuf = new byte[headerLen];
    curPacketBuf.get(headerBuf);
    if (curHeader == null) {
      curHeader = new PacketHeader();
    }
    curHeader.setFieldsFromData(payloadLen, headerBuf);

    // Compute the sub-slices of the packet
    int checksumLen = dataPlusChecksumLen - curHeader.getDataLen();
    if (checksumLen < 0) {
      throw new IOException("Invalid packet: data length in packet header " +
          "exceeds data length received. dataPlusChecksumLen=" +
          dataPlusChecksumLen + " header: " + curHeader);
    }

    reslicePacket(headerLen, checksumLen, curHeader.getDataLen());
  }

  /**
   * Rewrite the last-read packet on the wire to the given output stream.
   */
  public void mirrorPacketTo(DataOutputStream mirrorOut) throws IOException {
    Preconditions.checkState(!useDirectBuffers,
        "Currently only supported for non-direct buffers");
    mirrorOut.write(curPacketBuf.array(),
        curPacketBuf.arrayOffset(),
        curPacketBuf.remaining());
  }


  private static void doReadFully(ReadableByteChannel ch, InputStream in,
      ByteBuffer buf) throws IOException {
    if (ch != null) {
      readChannelFully(ch, buf);
    } else {
      Preconditions.checkState(!buf.isDirect(),
          "Must not use direct buffers with InputStream API");
      IOUtils.readFully(in, buf.array(),
          buf.arrayOffset() + buf.position(),
          buf.remaining());
      buf.position(buf.position() + buf.remaining());
    }
  }

  private void reslicePacket(
      int headerLen, int checksumsLen, int dataLen) {
    // Packet structure (refer to doRead() for details):
    //   PLEN    HLEN      HEADER     CHECKSUMS  DATA
    //   32-bit  16-bit   <protobuf>  <variable length>
    //   |--- lenThroughHeader ----|
    //   |----------- lenThroughChecksums   ----|
    //   |------------------- lenThroughData    ------|
    int lenThroughHeader = PacketHeader.PKT_LENGTHS_LEN + headerLen;
    int lenThroughChecksums = lenThroughHeader + checksumsLen;
    int lenThroughData = lenThroughChecksums + dataLen;

    assert dataLen >= 0 : "invalid datalen: " + dataLen;
    assert curPacketBuf.position() == lenThroughHeader;
    assert curPacketBuf.limit() == lenThroughData :
      "headerLen= " + headerLen + " clen=" + checksumsLen + " dlen=" + dataLen +
      " rem=" + curPacketBuf.remaining();

    // Slice the checksums.
    curPacketBuf.position(lenThroughHeader);
    curPacketBuf.limit(lenThroughChecksums);
    curChecksumSlice = curPacketBuf.slice();

    // Slice the data.
    curPacketBuf.position(lenThroughChecksums);
    curPacketBuf.limit(lenThroughData);
    curDataSlice = curPacketBuf.slice();

    // Reset buffer to point to the entirety of the packet (including
    // length prefixes)
    curPacketBuf.position(0);
    curPacketBuf.limit(lenThroughData);
  }


  private static void readChannelFully(ReadableByteChannel ch, ByteBuffer buf)
      throws IOException {
    while (buf.remaining() > 0) {
      int n = ch.read(buf);
      if (n < 0) {
        throw new IOException("Premature EOF reading from " + ch);
      }
    }
  }

  private void reallocPacketBuf(int atLeastCapacity) {
    // Realloc the buffer if this packet is longer than the previous
    // one.
    if (curPacketBuf == null ||
        curPacketBuf.capacity() < atLeastCapacity) {
      ByteBuffer newBuf;
      if (useDirectBuffers) {
        newBuf = bufferPool.getBuffer(atLeastCapacity);
      } else {
        newBuf = ByteBuffer.allocate(atLeastCapacity);
      }
      // If reallocing an existing buffer, copy the old packet length
      // prefixes over
      if (curPacketBuf != null) {
        curPacketBuf.flip();
        newBuf.put(curPacketBuf);
      }

      returnPacketBufToPool();
      curPacketBuf = newBuf;
    }
  }

  private void returnPacketBufToPool() {
    if (curPacketBuf != null && curPacketBuf.isDirect()) {
      bufferPool.returnBuffer(curPacketBuf);
      curPacketBuf = null;
    }
  }

  @Override // Closeable
  public void close() {
    returnPacketBufToPool();
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      // just in case it didn't get closed, we
      // may as well still try to return the buffer
      returnPacketBufToPool();
    } finally {
      super.finalize();
    }
  }
}
