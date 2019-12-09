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

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities.StreamCapability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DataStreamer.LastExceptionInStreamer;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.htrace.core.SpanId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;

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
  public void testComputePacketChunkSize() throws Exception {
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

  /**
   * This tests preventing overflows of package size and bodySize.
   * <p>
   * See also https://issues.apache.org/jira/browse/HDFS-11608.
   * </p>
   * @throws IOException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws NoSuchMethodException
   */
  @Test(timeout=60000)
  public void testPreventOverflow() throws IOException, NoSuchFieldException,
      SecurityException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException {

    final int defaultWritePacketSize = DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
    int configuredWritePacketSize = defaultWritePacketSize;
    int finalWritePacketSize = defaultWritePacketSize;

    /* test default WritePacketSize, e.g. 64*1024 */
    runAdjustChunkBoundary(configuredWritePacketSize, finalWritePacketSize);

    /* test large WritePacketSize, e.g. 1G */
    configuredWritePacketSize = 1000 * 1024 * 1024;
    finalWritePacketSize = PacketReceiver.MAX_PACKET_SIZE;
    runAdjustChunkBoundary(configuredWritePacketSize, finalWritePacketSize);
  }

  /**
   * @configuredWritePacketSize the configured WritePacketSize.
   * @finalWritePacketSize the final WritePacketSize picked by
   *                       {@link DFSOutputStream#adjustChunkBoundary}
   */
  private void runAdjustChunkBoundary(
      final int configuredWritePacketSize,
      final int finalWritePacketSize) throws IOException, NoSuchFieldException,
      SecurityException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchMethodException {

    final boolean appendChunk = false;
    final long blockSize = 3221225500L;
    final long bytesCurBlock = 1073741824L;
    final int bytesPerChecksum = 512;
    final int checksumSize = 4;
    final int chunkSize = bytesPerChecksum + checksumSize;
    final int packateMaxHeaderLength = 33;

    MiniDFSCluster dfsCluster = null;
    final File baseDir = new File(PathUtils.getTestDir(getClass()),
        GenericTestUtils.getMethodName());

    try {
      final Configuration dfsConf = new Configuration();
      dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
          baseDir.getAbsolutePath());
      dfsConf.setInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
          configuredWritePacketSize);
      dfsCluster = new MiniDFSCluster.Builder(dfsConf).numDataNodes(1).build();
      dfsCluster.waitActive();

      final FSDataOutputStream os = dfsCluster.getFileSystem()
          .create(new Path(baseDir.getPath(), "testPreventOverflow"));
      final DFSOutputStream dos = (DFSOutputStream) Whitebox
          .getInternalState(os, "wrappedStream");

      /* set appendChunk */
      final Method setAppendChunkMethod = dos.getClass()
          .getDeclaredMethod("setAppendChunk", boolean.class);
      setAppendChunkMethod.setAccessible(true);
      setAppendChunkMethod.invoke(dos, appendChunk);

      /* set bytesCurBlock */
      final Method setBytesCurBlockMethod = dos.getClass()
          .getDeclaredMethod("setBytesCurBlock", long.class);
      setBytesCurBlockMethod.setAccessible(true);
      setBytesCurBlockMethod.invoke(dos, bytesCurBlock);

      /* set blockSize */
      final Field blockSizeField = dos.getClass().getDeclaredField("blockSize");
      blockSizeField.setAccessible(true);
      blockSizeField.setLong(dos, blockSize);

      /* call adjustChunkBoundary */
      final Method method = dos.getClass()
          .getDeclaredMethod("adjustChunkBoundary");
      method.setAccessible(true);
      method.invoke(dos);

      /* get and verify writePacketSize */
      final Field writePacketSizeField = dos.getClass()
          .getDeclaredField("writePacketSize");
      writePacketSizeField.setAccessible(true);
      Assert.assertEquals(writePacketSizeField.getInt(dos),
          finalWritePacketSize);

      /* get and verify chunksPerPacket */
      final Field chunksPerPacketField = dos.getClass()
          .getDeclaredField("chunksPerPacket");
      chunksPerPacketField.setAccessible(true);
      Assert.assertEquals(chunksPerPacketField.getInt(dos),
          (finalWritePacketSize - packateMaxHeaderLength) / chunkSize);

      /* get and verify packetSize */
      final Field packetSizeField = dos.getClass()
          .getDeclaredField("packetSize");
      packetSizeField.setAccessible(true);
      Assert.assertEquals(packetSizeField.getInt(dos),
          chunksPerPacketField.getInt(dos) * chunkSize);
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
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

  @Test
  public void testEndLeaseCall() throws Exception {
    Configuration conf = new Configuration();
    DFSClient client = new DFSClient(cluster.getNameNode(0)
        .getNameNodeAddress(), conf);
    DFSClient spyClient = Mockito.spy(client);
    DFSOutputStream dfsOutputStream = spyClient.create("/file2",
        FsPermission.getFileDefault(),
        EnumSet.of(CreateFlag.CREATE), (short) 3, 1024, null , 1024, null);
    DFSOutputStream spyDFSOutputStream = Mockito.spy(dfsOutputStream);
    spyDFSOutputStream.closeThreads(anyBoolean());
    verify(spyClient, times(1)).endFileLease(anyLong());
  }

  @Test
  public void testStreamFlush() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path("/normal-file"));
    // Verify output stream supports hsync() and hflush().
    assertTrue("DFSOutputStream should support hflush()!",
        os.hasCapability(StreamCapability.HFLUSH.getValue()));
    assertTrue("DFSOutputStream should support hsync()!",
        os.hasCapability(StreamCapability.HSYNC.getValue()));
    byte[] bytes = new byte[1024];
    InputStream is = new ByteArrayInputStream(bytes);
    IOUtils.copyBytes(is, os, bytes.length);
    os.hflush();
    IOUtils.copyBytes(is, os, bytes.length);
    os.hsync();
    os.close();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
