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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class TestPacketReceiver {

  private static final long OFFSET_IN_BLOCK = 12345L;
  private static final int SEQNO = 54321;

  /**
   * Restore the default MAX_PACKET_SIZE after each test so tests are isolated.
   */
  @AfterEach
  public void restoreDefaultMaxPacketSize() {
    PacketReceiver.setMaxPacketSize(new HdfsConfiguration());
  }

  private byte[] prepareFakePacket(byte[] data, byte[] sums) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    int packetLen = data.length + sums.length + Ints.BYTES;
    PacketHeader header = new PacketHeader(
        packetLen, OFFSET_IN_BLOCK, SEQNO, false, data.length, false);
    header.write(dos);
    
    dos.write(sums);
    dos.write(data);
    dos.flush();
    return baos.toByteArray();
  }
  
  private static byte[] remainingAsArray(ByteBuffer buf) {
    byte[] b = new byte[buf.remaining()];
    buf.get(b);
    return b;
  }

  @Test
  public void testPacketSize() {
    assertEquals(PacketReceiver.MAX_PACKET_SIZE,
            HdfsClientConfigKeys.DFS_DATA_TRANSFER_MAX_PACKET_SIZE_DEFAULT);
  }

  /**
   * Verify that MAX_PACKET_SIZE is initialized to the default constant (not 0)
   * even before any Configuration-based initialization takes place. This guards
   * against the circular class-loading deadlock described in HDFS-17630, where
   * the previous static initializer called {@code new HdfsConfiguration()},
   * which could re-enter PacketReceiver before it finished initializing and
   * leave MAX_PACKET_SIZE as 0.
   */
  @Test
  public void testMaxPacketSizeDefaultIsNonZero() {
    assertNotEquals(0, PacketReceiver.MAX_PACKET_SIZE,
        "MAX_PACKET_SIZE must not be 0; a zero value causes all block reads "
            + "to fail with 'Incorrect value for packet payload size'");
    assertEquals(HdfsClientConfigKeys.DFS_DATA_TRANSFER_MAX_PACKET_SIZE_DEFAULT,
        PacketReceiver.MAX_PACKET_SIZE,
        "MAX_PACKET_SIZE should equal the configured default before any "
            + "explicit setMaxPacketSize() call");
  }

  /**
   * Verify that {@link PacketReceiver#setMaxPacketSize(Configuration)} correctly
   * reads and applies the configured value, allowing operators to tune the limit
   * without triggering the class-loading race described in HDFS-17630.
   */
  @Test
  public void testSetMaxPacketSizeFromConfig() {
    int customSize = 4 * 1024 * 1024; // 4 MB
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.DFS_DATA_TRANSFER_MAX_PACKET_SIZE,
        customSize);
    PacketReceiver.setMaxPacketSize(conf);
    assertEquals(customSize, PacketReceiver.MAX_PACKET_SIZE,
        "MAX_PACKET_SIZE should reflect the value set in Configuration");
  }

  @Test
  public void testReceiveAndMirror() throws IOException {
    PacketReceiver pr = new PacketReceiver(false);

    // Test three different lengths, to force reallocing
    // the buffer as it grows.
    doTestReceiveAndMirror(pr, 100, 10);
    doTestReceiveAndMirror(pr, 50, 10);
    doTestReceiveAndMirror(pr, 150, 10);

    pr.close();
  }
  
  private void doTestReceiveAndMirror(PacketReceiver pr,
      int dataLen, int checksumsLen) throws IOException {
    final byte[] DATA = AppendTestUtil.initBuffer(dataLen);
    final byte[] CHECKSUMS = AppendTestUtil.initBuffer(checksumsLen);

    byte[] packet = prepareFakePacket(DATA, CHECKSUMS);
    ByteArrayInputStream in = new ByteArrayInputStream(packet);
    
    pr.receiveNextPacket(in);
    
    ByteBuffer parsedData = pr.getDataSlice();
    assertArrayEquals(DATA, remainingAsArray(parsedData));

    ByteBuffer parsedChecksums = pr.getChecksumSlice();
    assertArrayEquals(CHECKSUMS, remainingAsArray(parsedChecksums));
    
    PacketHeader header = pr.getHeader();
    assertEquals(SEQNO, header.getSeqno());
    assertEquals(OFFSET_IN_BLOCK, header.getOffsetInBlock());
    assertEquals(dataLen + checksumsLen + Ints.BYTES, header.getPacketLen());
    
    // Mirror the packet to an output stream and make sure it matches
    // the packet we sent.
    ByteArrayOutputStream mirrored = new ByteArrayOutputStream();
    mirrored = Mockito.spy(mirrored);

    pr.mirrorPacketTo(new DataOutputStream(mirrored));
    // The write should be done in a single call. Otherwise we may hit
    // nasty interactions with nagling (eg HDFS-4049).
    Mockito.verify(mirrored, Mockito.times(1))
      .write(Mockito.<byte[]>any(), Mockito.anyInt(),
          Mockito.eq(packet.length));
    Mockito.verifyNoMoreInteractions(mirrored);

    assertArrayEquals(packet, mirrored.toByteArray());
  }
}
