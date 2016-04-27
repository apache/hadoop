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
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DataStreamer.LastExceptionInStreamer;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.htrace.core.SpanId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestDFSOutputStream {
  static MiniDFSCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
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
      assertEquals(e, dummy);
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
        "foo", null, null, null, null, null, null);

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

  @Test
  public void testNoLocalWriteFlag() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.NO_LOCAL_WRITE,
        CreateFlag.CREATE);
    BlockManager bm = cluster.getNameNode().getNamesystem().getBlockManager();
    DatanodeManager dm = bm.getDatanodeManager();
    try(FSDataOutputStream os = fs.create(new Path("/test-no-local"),
        FsPermission.getDefault(),
        flags, 512, (short)2, 512, null)) {
      // Inject a DatanodeManager that returns one DataNode as local node for
      // the client.
      DatanodeManager spyDm = spy(dm);
      DatanodeDescriptor dn1 = dm.getDatanodeListForReport
          (HdfsConstants.DatanodeReportType.LIVE).get(0);
      doReturn(dn1).when(spyDm).getDatanodeByHost("127.0.0.1");
      Whitebox.setInternalState(bm, "datanodeManager", spyDm);
      byte[] buf = new byte[512 * 16];
      new Random().nextBytes(buf);
      os.write(buf);
    } finally {
      Whitebox.setInternalState(bm, "datanodeManager", dm);
    }
    cluster.triggerBlockReports();
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    // Total number of DataNodes is 3.
    assertEquals(3, cluster.getAllBlockReports(bpid).size());
    int numDataNodesWithData = 0;
    for (Map<DatanodeStorage, BlockListAsLongs> dnBlocks :
        cluster.getAllBlockReports(bpid)) {
      for (BlockListAsLongs blocks : dnBlocks.values()) {
        if (blocks.getNumberOfBlocks() > 0) {
          numDataNodesWithData++;
          break;
        }
      }
    }
    // Verify that only one DN has no data.
    assertEquals(1, 3 - numDataNodesWithData);
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
