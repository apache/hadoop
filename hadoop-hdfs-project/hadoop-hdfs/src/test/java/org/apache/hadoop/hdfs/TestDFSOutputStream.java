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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DataStreamer.LastExceptionInStreamer;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.htrace.core.SpanId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class TestDFSOutputStream {
  static MiniDFSCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).build();
  }

  /**
   * The close() method of DFSOutputStream should never throw the same exception
   * twice. See HDFS-5335 for details.
   */
  @Test
  public void testCloseTwice() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/test"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");
    DataStreamer streamer = (DataStreamer) Whitebox
        .getInternalState(dos, "streamer");
    @SuppressWarnings("unchecked")
    LastExceptionInStreamer ex = (LastExceptionInStreamer) Whitebox
        .getInternalState(streamer, "lastException");
    Throwable thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
    Assert.assertNull(thrown);

    dos.close();

    IOException dummy = new IOException("dummy");
    ex.set(dummy);
    try {
      dos.close();
    } catch (IOException e) {
      Assert.assertEquals(e, dummy);
    }
    thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
    Assert.assertNull(thrown);
    dos.close();
  }

  /**
   * The computePacketChunkSize() method of DFSOutputStream should set the actual
   * packet size < 64kB. See HDFS-7308 for details.
   */
  @Test
  public void testComputePacketChunkSize()
      throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/test"));
    DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
        "wrappedStream");

    final int packetSize = 64*1024;
    final int bytesPerChecksum = 512;

    Method method = dos.getClass().getDeclaredMethod("computePacketChunkSize",
        int.class, int.class);
    method.setAccessible(true);
    method.invoke(dos, packetSize, bytesPerChecksum);

    Field field = dos.getClass().getDeclaredField("packetSize");
    field.setAccessible(true);

    Assert.assertTrue((Integer) field.get(dos) + 33 < packetSize);
    // If PKT_MAX_HEADER_LEN is 257, actual packet size come to over 64KB
    // without a fix on HDFS-7308.
    Assert.assertTrue((Integer) field.get(dos) + 257 < packetSize);
  }

  @Test
  public void testCongestionBackoff() throws IOException {
    DfsClientConf dfsClientConf = mock(DfsClientConf.class);
    DFSClient client = mock(DFSClient.class);
    when(client.getConf()).thenReturn(dfsClientConf);
    when(client.getTracer()).thenReturn(FsTracer.get(new Configuration()));
    client.clientRunning = true;
    DataStreamer stream = new DataStreamer(
        mock(HdfsFileStatus.class),
        mock(ExtendedBlock.class),
        client,
        "foo", null, null, null, null, null);

    DataOutputStream blockStream = mock(DataOutputStream.class);
    doThrow(new IOException()).when(blockStream).flush();
    Whitebox.setInternalState(stream, "blockStream", blockStream);
    Whitebox.setInternalState(stream, "stage",
                              BlockConstructionStage.PIPELINE_CLOSE);
    @SuppressWarnings("unchecked")
    LinkedList<DFSPacket> dataQueue = (LinkedList<DFSPacket>)
        Whitebox.getInternalState(stream, "dataQueue");
    @SuppressWarnings("unchecked")
    ArrayList<DatanodeInfo> congestedNodes = (ArrayList<DatanodeInfo>)
        Whitebox.getInternalState(stream, "congestedNodes");
    congestedNodes.add(mock(DatanodeInfo.class));
    DFSPacket packet = mock(DFSPacket.class);
    when(packet.getTraceParents()).thenReturn(new SpanId[] {});
    dataQueue.add(packet);
    stream.run();
    Assert.assertTrue(congestedNodes.isEmpty());
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
