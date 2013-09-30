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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.ClientMmap;
import org.apache.hadoop.hdfs.client.ClientMmapManager;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * This class tests if EnhancedByteBufferAccess works correctly.
 */
public class TestEnhancedByteBufferAccess {
  private static final Log LOG =
      LogFactory.getLog(TestEnhancedByteBufferAccess.class.getName());

  static TemporarySocketDirectory sockDir;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  private static byte[] byteBufferToArray(ByteBuffer buf) {
    byte resultArray[] = new byte[buf.remaining()];
    buf.get(resultArray);
    buf.flip();
    return resultArray;
  }
  
  public static HdfsConfiguration initZeroCopyTest() {
    Assume.assumeTrue(NativeIO.isAvailable());
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE, 3);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS, 100);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestRequestMmapAccess._PORT.sock").getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
    return conf;
  }

  @Test
  public void testZeroCopyReads() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    FSDataInputStream fsIn = null;
    final int TEST_FILE_LENGTH = 12345;
    
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, 7567L);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);
      ByteBuffer result = fsIn.read(null, 4096,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(4096, result.remaining());
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, 4096),
          byteBufferToArray(result));
      fsIn.releaseBuffer(result);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }
  
  @Test
  public void testShortZeroCopyReads() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    FSDataInputStream fsIn = null;
    final int TEST_FILE_LENGTH = 12345;
    
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, TEST_FILE_LENGTH, (short)1, 7567L);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);

      // Try to read 8192, but only get 4096 because of the block size.
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      ByteBuffer result =
        dfsIn.read(null, 8192, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(4096, result.remaining());
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, 4096),
          byteBufferToArray(result));
      dfsIn.releaseBuffer(result);
      
      // Try to read 4097, but only get 4096 because of the block size.
      result = 
          dfsIn.read(null, 4097, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(4096, result.remaining());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 4096, 8192),
          byteBufferToArray(result));
      dfsIn.releaseBuffer(result);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  @Test
  public void testZeroCopyReadsNoFallback() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    FSDataInputStream fsIn = null;
    final int TEST_FILE_LENGTH = 12345;
    
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, 7567L);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      ByteBuffer result;
      try {
        result = dfsIn.read(null, 4097, EnumSet.noneOf(ReadOption.class));
        Assert.fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
        // expected
      }
      result = dfsIn.read(null, 4096, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(4096, result.remaining());
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(4096,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, 4096),
          byteBufferToArray(result));
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  private static class CountingVisitor
      implements ClientMmapManager.ClientMmapVisitor {
    int count = 0;

    @Override
    public void accept(ClientMmap mmap) {
      count++;
    }

    public void reset() {
      count = 0;
    }
  }

  @Test
  public void testZeroCopyMmapCache() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 16385;
    final int RANDOM_SEED = 23453;
    FSDataInputStream fsIn = null;
    ByteBuffer results[] = { null, null, null, null, null };
    
    DistributedFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);
      final ClientMmapManager mmapManager = fs.getClient().getMmapManager();
      final CountingVisitor countingVisitor = new CountingVisitor();
      mmapManager.visitMmaps(countingVisitor);
      Assert.assertEquals(0, countingVisitor.count);
      mmapManager.visitEvictable(countingVisitor);
      Assert.assertEquals(0, countingVisitor.count);
      results[0] = fsIn.read(null, 4096,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      fsIn.seek(0);
      results[1] = fsIn.read(null, 4096,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      mmapManager.visitMmaps(countingVisitor);
      Assert.assertEquals(1, countingVisitor.count);
      countingVisitor.reset();
      mmapManager.visitEvictable(countingVisitor);
      Assert.assertEquals(0, countingVisitor.count);
      countingVisitor.reset();

      // The mmaps should be of the first block of the file.
      final ExtendedBlock firstBlock = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      mmapManager.visitMmaps(new ClientMmapManager.ClientMmapVisitor() {
        @Override
        public void accept(ClientMmap mmap) {
          Assert.assertEquals(firstBlock, mmap.getBlock());
        }
      });

      // Read more blocks.
      results[2] = fsIn.read(null, 4096,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      results[3] = fsIn.read(null, 4096,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      try {
        results[4] = fsIn.read(null, 4096,
            EnumSet.of(ReadOption.SKIP_CHECKSUMS));
        Assert.fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
        // expected
      }

      // we should have 3 mmaps, 0 evictable
      mmapManager.visitMmaps(countingVisitor);
      Assert.assertEquals(3, countingVisitor.count);
      countingVisitor.reset();
      mmapManager.visitEvictable(countingVisitor);
      Assert.assertEquals(0, countingVisitor.count);

      // After we close the cursors, the mmaps should be evictable for 
      // a brief period of time.  Then, they should be closed (we're 
      // using a very quick timeout)
      for (ByteBuffer buffer : results) {
        if (buffer != null) {
          fsIn.releaseBuffer(buffer);
        }
      }
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        public Boolean get() {
          countingVisitor.reset();
          try {
            mmapManager.visitEvictable(countingVisitor);
          } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
          }
          return (0 == countingVisitor.count);
        }
      }, 10, 10000);
      countingVisitor.reset();
      mmapManager.visitMmaps(countingVisitor);
      Assert.assertEquals(0, countingVisitor.count);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  /**
   * Test HDFS fallback reads.  HDFS streams support the ByteBufferReadable
   * interface.
   */
  @Test
  public void testHdfsFallbackReads() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 16385;
    final int RANDOM_SEED = 23453;
    FSDataInputStream fsIn = null;
    
    DistributedFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);
      testFallbackImpl(fsIn, original);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  private static class RestrictedAllocatingByteBufferPool
      implements ByteBufferPool {
    private final boolean direct;

    RestrictedAllocatingByteBufferPool(boolean direct) {
      this.direct = direct;
    }
    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      Preconditions.checkArgument(this.direct == direct);
      return direct ? ByteBuffer.allocateDirect(length) :
        ByteBuffer.allocate(length);
    }
    @Override
    public void putBuffer(ByteBuffer buffer) {
    }
  }
  
  private static void testFallbackImpl(InputStream stream,
      byte original[]) throws Exception {
    RestrictedAllocatingByteBufferPool bufferPool =
        new RestrictedAllocatingByteBufferPool(
            stream instanceof ByteBufferReadable);

    ByteBuffer result = ByteBufferUtil.fallbackRead(stream, bufferPool, 10);
    Assert.assertEquals(10, result.remaining());
    Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, 10),
        byteBufferToArray(result));

    result = ByteBufferUtil.fallbackRead(stream, bufferPool, 5000);
    Assert.assertEquals(5000, result.remaining());
    Assert.assertArrayEquals(Arrays.copyOfRange(original, 10, 5010),
        byteBufferToArray(result));

    result = ByteBufferUtil.fallbackRead(stream, bufferPool, 9999999);
    Assert.assertEquals(11375, result.remaining());
    Assert.assertArrayEquals(Arrays.copyOfRange(original, 5010, 16385),
        byteBufferToArray(result));

    result = ByteBufferUtil.fallbackRead(stream, bufferPool, 10);
    Assert.assertNull(result);
  }

  /**
   * Test the {@link ByteBufferUtil#fallbackRead} function directly.
   */
  @Test
  public void testFallbackRead() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 16385;
    final int RANDOM_SEED = 23453;
    FSDataInputStream fsIn = null;
    
    DistributedFileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      byte original[] = new byte[TEST_FILE_LENGTH];
      IOUtils.readFully(fsIn, original, 0, TEST_FILE_LENGTH);
      fsIn.close();
      fsIn = fs.open(TEST_PATH);
      testFallbackImpl(fsIn, original);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  /**
   * Test fallback reads on a stream which does not support the
   * ByteBufferReadable * interface.
   */
  @Test
  public void testIndirectFallbackReads() throws Exception {
    final File TEST_DIR = new File(
      System.getProperty("test.build.data","build/test/data"));
    final String TEST_PATH = TEST_DIR + File.separator +
        "indirectFallbackTestFile";
    final int TEST_FILE_LENGTH = 16385;
    final int RANDOM_SEED = 23453;
    FileOutputStream fos = null;
    FileInputStream fis = null;
    try {
      fos = new FileOutputStream(TEST_PATH);
      Random random = new Random(RANDOM_SEED);
      byte original[] = new byte[TEST_FILE_LENGTH];
      random.nextBytes(original);
      fos.write(original);
      fos.close();
      fos = null;
      fis = new FileInputStream(TEST_PATH);
      testFallbackImpl(fis, original);
    } finally {
      IOUtils.cleanup(LOG, fos, fis);
      new File(TEST_PATH).delete();
    }
  }
}
