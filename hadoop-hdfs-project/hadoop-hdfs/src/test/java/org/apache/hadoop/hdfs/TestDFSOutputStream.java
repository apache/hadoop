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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;

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
    @SuppressWarnings("unchecked")
    AtomicReference<IOException> ex = (AtomicReference<IOException>) Whitebox
        .getInternalState(dos, "lastException");
    Assert.assertEquals(null, ex.get());

    dos.close();

    IOException dummy = new IOException("dummy");
    ex.set(dummy);
    try {
      dos.close();
    } catch (IOException e) {
      Assert.assertEquals(e, dummy);
    }
    Assert.assertEquals(null, ex.get());
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
          .create(new Path(baseDir.getAbsolutePath(), "testPreventOverflow"));
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

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }
}
