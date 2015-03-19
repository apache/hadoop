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
package org.apache.hadoop.hdfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.htrace.Span;

/****************************************************************
 * DFSPacket is used by DataStreamer and DFSOutputStream.
 * DFSOutputStream generates packets and then ask DatStreamer
 * to send them to datanodes.
 ****************************************************************/

class DFSPacket {
  public static final long HEART_BEAT_SEQNO = -1L;
  private static long[] EMPTY = new long[0];
  private final long seqno; // sequence number of buffer in block
  private final long offsetInBlock; // offset in block
  private boolean syncBlock; // this packet forces the current block to disk
  private int numChunks; // number of chunks currently in packet
  private final int maxChunks; // max chunks in packet
  private byte[] buf;
  private final boolean lastPacketInBlock; // is this the last packet in block?

  /**
   * buf is pointed into like follows:
   *  (C is checksum data, D is payload data)
   *
   * [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
   *           ^        ^               ^               ^
   *           |        checksumPos     dataStart       dataPos
   *           checksumStart
   *
   * Right before sending, we move the checksum data to immediately precede
   * the actual data, and then insert the header into the buffer immediately
   * preceding the checksum data, so we make sure to keep enough space in
   * front of the checksum data to support the largest conceivable header.
   */
  private int checksumStart;
  private int checksumPos;
  private final int dataStart;
  private int dataPos;
  private long[] traceParents = EMPTY;
  private int traceParentsUsed;
  private Span span;

  /**
   * Create a new packet.
   *
   * @param buf the buffer storing data and checksums
   * @param chunksPerPkt maximum number of chunks per packet.
   * @param offsetInBlock offset in bytes into the HDFS block.
   * @param seqno the sequence number of this packet
   * @param checksumSize the size of checksum
   * @param lastPacketInBlock if this is the last packet
   */
  DFSPacket(byte[] buf, int chunksPerPkt, long offsetInBlock, long seqno,
                   int checksumSize, boolean lastPacketInBlock) {
    this.lastPacketInBlock = lastPacketInBlock;
    this.numChunks = 0;
    this.offsetInBlock = offsetInBlock;
    this.seqno = seqno;

    this.buf = buf;

    checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
    checksumPos = checksumStart;
    dataStart = checksumStart + (chunksPerPkt * checksumSize);
    dataPos = dataStart;
    maxChunks = chunksPerPkt;
  }

  /**
   * Write data to this packet.
   *
   * @param inarray input array of data
   * @param off the offset of data to write
   * @param len the length of data to write
   * @throws ClosedChannelException
   */
  synchronized void writeData(byte[] inarray, int off, int len)
      throws ClosedChannelException {
    checkBuffer();
    if (dataPos + len > buf.length) {
      throw new BufferOverflowException();
    }
    System.arraycopy(inarray, off, buf, dataPos, len);
    dataPos += len;
  }

  /**
   * Write checksums to this packet
   *
   * @param inarray input array of checksums
   * @param off the offset of checksums to write
   * @param len the length of checksums to write
   * @throws ClosedChannelException
   */
  synchronized void writeChecksum(byte[] inarray, int off, int len)
      throws ClosedChannelException {
    checkBuffer();
    if (len == 0) {
      return;
    }
    if (checksumPos + len > dataStart) {
      throw new BufferOverflowException();
    }
    System.arraycopy(inarray, off, buf, checksumPos, len);
    checksumPos += len;
  }

  /**
   * Write the full packet, including the header, to the given output stream.
   *
   * @param stm
   * @throws IOException
   */
  synchronized void writeTo(DataOutputStream stm) throws IOException {
    checkBuffer();

    final int dataLen = dataPos - dataStart;
    final int checksumLen = checksumPos - checksumStart;
    final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;

    PacketHeader header = new PacketHeader(
        pktLen, offsetInBlock, seqno, lastPacketInBlock, dataLen, syncBlock);

    if (checksumPos != dataStart) {
      // Move the checksum to cover the gap. This can happen for the last
      // packet or during an hflush/hsync call.
      System.arraycopy(buf, checksumStart, buf,
          dataStart - checksumLen , checksumLen);
      checksumPos = dataStart;
      checksumStart = checksumPos - checksumLen;
    }

    final int headerStart = checksumStart - header.getSerializedSize();
    assert checksumStart + 1 >= header.getSerializedSize();
    assert headerStart >= 0;
    assert headerStart + header.getSerializedSize() == checksumStart;

    // Copy the header data into the buffer immediately preceding the checksum
    // data.
    System.arraycopy(header.getBytes(), 0, buf, headerStart,
        header.getSerializedSize());

    // corrupt the data for testing.
    if (DFSClientFaultInjector.get().corruptPacket()) {
      buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
    }

    // Write the now contiguous full packet to the output stream.
    stm.write(buf, headerStart, header.getSerializedSize() + checksumLen + dataLen);

    // undo corruption.
    if (DFSClientFaultInjector.get().uncorruptPacket()) {
      buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
    }
  }

  private synchronized void checkBuffer() throws ClosedChannelException {
    if (buf == null) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Release the buffer in this packet to ByteArrayManager.
   *
   * @param bam
   */
  synchronized void releaseBuffer(ByteArrayManager bam) {
    bam.release(buf);
    buf = null;
  }

  /**
   * get the packet's last byte's offset in the block
   *
   * @return the packet's last byte's offset in the block
   */
  synchronized long getLastByteOffsetBlock() {
    return offsetInBlock + dataPos - dataStart;
  }

  /**
   * Check if this packet is a heart beat packet
   *
   * @return true if the sequence number is HEART_BEAT_SEQNO
   */
  boolean isHeartbeatPacket() {
    return seqno == HEART_BEAT_SEQNO;
  }

  /**
   * check if this packet is the last packet in block
   *
   * @return true if the packet is the last packet
   */
  boolean isLastPacketInBlock(){
    return lastPacketInBlock;
  }

  /**
   * get sequence number of this packet
   *
   * @return the sequence number of this packet
   */
  long getSeqno(){
    return seqno;
  }

  /**
   * get the number of chunks this packet contains
   *
   * @return the number of chunks in this packet
   */
  synchronized int getNumChunks(){
    return numChunks;
  }

  /**
   * increase the number of chunks by one
   */
  synchronized void incNumChunks(){
    numChunks++;
  }

  /**
   * get the maximum number of packets
   *
   * @return the maximum number of packets
   */
  int getMaxChunks(){
    return maxChunks;
  }

  /**
   * set if to sync block
   *
   * @param syncBlock if to sync block
   */
  synchronized void setSyncBlock(boolean syncBlock){
    this.syncBlock = syncBlock;
  }

  @Override
  public String toString() {
    return "packet seqno: " + this.seqno +
        " offsetInBlock: " + this.offsetInBlock +
        " lastPacketInBlock: " + this.lastPacketInBlock +
        " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
  }

  /**
   * Add a trace parent span for this packet.<p/>
   *
   * Trace parent spans for a packet are the trace spans responsible for
   * adding data to that packet.  We store them as an array of longs for
   * efficiency.<p/>
   *
   * Protected by the DFSOutputStream dataQueue lock.
   */
  public void addTraceParent(Span span) {
    if (span == null) {
      return;
    }
    addTraceParent(span.getSpanId());
  }

  public void addTraceParent(long id) {
    if (traceParentsUsed == traceParents.length) {
      int newLength = (traceParents.length == 0) ? 8 :
          traceParents.length * 2;
      traceParents = Arrays.copyOf(traceParents, newLength);
    }
    traceParents[traceParentsUsed] = id;
    traceParentsUsed++;
  }

  /**
   * Get the trace parent spans for this packet.<p/>
   *
   * Will always be non-null.<p/>
   *
   * Protected by the DFSOutputStream dataQueue lock.
   */
  public long[] getTraceParents() {
    // Remove duplicates from the array.
    int len = traceParentsUsed;
    Arrays.sort(traceParents, 0, len);
    int i = 0, j = 0;
    long prevVal = 0; // 0 is not a valid span id
    while (true) {
      if (i == len) {
        break;
      }
      long val = traceParents[i];
      if (val != prevVal) {
        traceParents[j] = val;
        j++;
        prevVal = val;
      }
      i++;
    }
    if (j < traceParents.length) {
      traceParents = Arrays.copyOf(traceParents, j);
      traceParentsUsed = traceParents.length;
    }
    return traceParents;
  }

  public void setTraceSpan(Span span) {
    this.span = span;
  }

  public Span getTraceSpan() {
    return span;
  }
}
