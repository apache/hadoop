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

import java.util.Random;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

public class TestDFSPacket {
  private static final int chunkSize = 512;
  private static final int checksumSize = 4;
  private static final int maxChunksPerPacket = 4;

  @Test
  public void testPacket() throws Exception {
    Random r = new Random(12345L);
    byte[] data =  new byte[chunkSize];
    r.nextBytes(data);
    byte[] checksum = new byte[checksumSize];
    r.nextBytes(checksum);

    DataOutputBuffer os =  new DataOutputBuffer(data.length * 2);

    byte[] packetBuf = new byte[data.length * 2];
    DFSPacket p = new DFSPacket(packetBuf, maxChunksPerPacket,
                                0, 0, checksumSize, false);
    p.setSyncBlock(true);
    p.writeData(data, 0, data.length);
    p.writeChecksum(checksum, 0, checksum.length);
    p.writeTo(os);

    //we have set syncBlock to true, so the header has the maximum length
    int headerLen = PacketHeader.PKT_MAX_HEADER_LEN;
    byte[] readBuf = os.getData();

    assertArrayRegionsEqual(readBuf, headerLen, checksum, 0, checksum.length);
    assertArrayRegionsEqual(readBuf, headerLen + checksum.length, data, 0, data.length);

  }

  public static void assertArrayRegionsEqual(byte []buf1, int off1, byte []buf2,
                                             int off2, int len) {
    for (int i = 0; i < len; i++) {
      if (buf1[off1 + i] != buf2[off2 + i]) {
        Assert.fail("arrays differ at byte " + i + ". " +
            "The first array has " + (int) buf1[off1 + i] +
            ", but the second array has " + (int) buf2[off2 + i]);
      }
    }
  }

  @Test
  public void testAddParentsGetParents() throws Exception {
    DFSPacket p = new DFSPacket(null, maxChunksPerPacket,
                                0, 0, checksumSize, false);
    long parents[] = p.getTraceParents();
    Assert.assertEquals(0, parents.length);
    p.addTraceParent(123);
    p.addTraceParent(123);
    parents = p.getTraceParents();
    Assert.assertEquals(1, parents.length);
    Assert.assertEquals(123, parents[0]);
    parents = p.getTraceParents(); // test calling 'get' again.
    Assert.assertEquals(1, parents.length);
    Assert.assertEquals(123, parents[0]);
    p.addTraceParent(1);
    p.addTraceParent(456);
    p.addTraceParent(789);
    parents = p.getTraceParents();
    Assert.assertEquals(4, parents.length);
    Assert.assertEquals(1, parents[0]);
    Assert.assertEquals(123, parents[1]);
    Assert.assertEquals(456, parents[2]);
    Assert.assertEquals(789, parents[3]);
  }
}
