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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MMAP_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache.CacheVisitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
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

  static private TemporarySocketDirectory sockDir;

  static private CacheManipulator prevCacheManipulator;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new CacheManipulator() {
      @Override
      public void mlock(String identifier,
          ByteBuffer mmap, long length) throws IOException {
        LOG.info("mlocking " + identifier);
      }
    });
  }

  @AfterClass
  public static void teardown() {
    // Restore the original CacheManipulator
    NativeIO.POSIX.setCacheManipulator(prevCacheManipulator);
  }

  private static byte[] byteBufferToArray(ByteBuffer buf) {
    byte resultArray[] = new byte[buf.remaining()];
    buf.get(resultArray);
    buf.flip();
    return resultArray;
  }
  
  private static final int BLOCK_SIZE = 
      (int) NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();
  
  public static HdfsConfiguration initZeroCopyTest() {
    Assume.assumeTrue(NativeIO.isAvailable());
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_SIZE, 3);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS, 100);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestRequestMmapAccess._PORT.sock").getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, true);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setLong(DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 1000);
    return conf;
  }

  @Test
  public void testZeroCopyReads() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    FSDataInputStream fsIn = null;
    final int TEST_FILE_LENGTH = 3 * BLOCK_SIZE;
    
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
      ByteBuffer result = fsIn.read(null, BLOCK_SIZE,
          EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(BLOCK_SIZE, result.remaining());
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, BLOCK_SIZE),
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
    final int TEST_FILE_LENGTH = 3 * BLOCK_SIZE;
    
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

      // Try to read (2 * ${BLOCK_SIZE}), but only get ${BLOCK_SIZE} because of the block size.
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      ByteBuffer result =
        dfsIn.read(null, 2 * BLOCK_SIZE, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(BLOCK_SIZE, result.remaining());
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, BLOCK_SIZE),
          byteBufferToArray(result));
      dfsIn.releaseBuffer(result);
      
      // Try to read (1 + ${BLOCK_SIZE}), but only get ${BLOCK_SIZE} because of the block size.
      result = 
          dfsIn.read(null, 1 + BLOCK_SIZE, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(BLOCK_SIZE, result.remaining());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, BLOCK_SIZE, 2 * BLOCK_SIZE),
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
    final int TEST_FILE_LENGTH = 3 * BLOCK_SIZE;
    
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
        result = dfsIn.read(null, BLOCK_SIZE + 1, EnumSet.noneOf(ReadOption.class));
        Assert.fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
        // expected
      }
      result = dfsIn.read(null, BLOCK_SIZE, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(BLOCK_SIZE, result.remaining());
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BLOCK_SIZE,
          dfsIn.getReadStatistics().getTotalZeroCopyBytesRead());
      Assert.assertArrayEquals(Arrays.copyOfRange(original, 0, BLOCK_SIZE),
          byteBufferToArray(result));
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }

  private static class CountingVisitor implements CacheVisitor {
    private final int expectedNumOutstandingMmaps;
    private final int expectedNumReplicas;
    private final int expectedNumEvictable;
    private final int expectedNumMmapedEvictable;

    CountingVisitor(int expectedNumOutstandingMmaps,
        int expectedNumReplicas, int expectedNumEvictable,
        int expectedNumMmapedEvictable) {
      this.expectedNumOutstandingMmaps = expectedNumOutstandingMmaps;
      this.expectedNumReplicas = expectedNumReplicas;
      this.expectedNumEvictable = expectedNumEvictable;
      this.expectedNumMmapedEvictable = expectedNumMmapedEvictable;
    }

    @Override
    public void visit(int numOutstandingMmaps,
        Map<ExtendedBlockId, ShortCircuitReplica> replicas,
        Map<ExtendedBlockId, InvalidToken> failedLoads,
        Map<Long, ShortCircuitReplica> evictable,
        Map<Long, ShortCircuitReplica> evictableMmapped) {
      if (expectedNumOutstandingMmaps >= 0) {
        Assert.assertEquals(expectedNumOutstandingMmaps, numOutstandingMmaps);
      }
      if (expectedNumReplicas >= 0) {
        Assert.assertEquals(expectedNumReplicas, replicas.size());
      }
      if (expectedNumEvictable >= 0) {
        Assert.assertEquals(expectedNumEvictable, evictable.size());
      }
      if (expectedNumMmapedEvictable >= 0) {
        Assert.assertEquals(expectedNumMmapedEvictable, evictableMmapped.size());
      }
    }
  }

  @Test
  public void testZeroCopyMmapCache() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 5 * BLOCK_SIZE;
    final int RANDOM_SEED = 23453;
    final String CONTEXT = "testZeroCopyMmapCacheContext";
    FSDataInputStream fsIn = null;
    ByteBuffer results[] = { null, null, null, null };

    DistributedFileSystem fs = null;
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);
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
    final ShortCircuitCache cache = ClientContext.get(
        CONTEXT, new DFSClient.Conf(conf)). getShortCircuitCache();
    cache.accept(new CountingVisitor(0, 5, 5, 0));
    results[0] = fsIn.read(null, BLOCK_SIZE,
        EnumSet.of(ReadOption.SKIP_CHECKSUMS));
    fsIn.seek(0);
    results[1] = fsIn.read(null, BLOCK_SIZE,
        EnumSet.of(ReadOption.SKIP_CHECKSUMS));

    // The mmap should be of the first block of the file.
    final ExtendedBlock firstBlock =
        DFSTestUtil.getFirstBlock(fs, TEST_PATH);
    cache.accept(new CacheVisitor() {
      @Override
      public void visit(int numOutstandingMmaps,
          Map<ExtendedBlockId, ShortCircuitReplica> replicas,
          Map<ExtendedBlockId, InvalidToken> failedLoads, 
          Map<Long, ShortCircuitReplica> evictable,
          Map<Long, ShortCircuitReplica> evictableMmapped) {
        ShortCircuitReplica replica = replicas.get(
            new ExtendedBlockId(firstBlock.getBlockId(), firstBlock.getBlockPoolId()));
        Assert.assertNotNull(replica);
        Assert.assertTrue(replica.hasMmap());
        // The replica should not yet be evictable, since we have it open.
        Assert.assertNull(replica.getEvictableTimeNs());
      }
    });

    // Read more blocks.
    results[2] = fsIn.read(null, BLOCK_SIZE,
        EnumSet.of(ReadOption.SKIP_CHECKSUMS));
    results[3] = fsIn.read(null, BLOCK_SIZE,
        EnumSet.of(ReadOption.SKIP_CHECKSUMS));

    // we should have 3 mmaps, 1 evictable
    cache.accept(new CountingVisitor(3, 5, 2, 0));

    // After we close the cursors, the mmaps should be evictable for 
    // a brief period of time.  Then, they should be closed (we're 
    // using a very quick timeout)
    for (ByteBuffer buffer : results) {
      if (buffer != null) {
        fsIn.releaseBuffer(buffer);
      }
    }
    fsIn.close();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        final MutableBoolean finished = new MutableBoolean(false);
        cache.accept(new CacheVisitor() {
          @Override
          public void visit(int numOutstandingMmaps,
              Map<ExtendedBlockId, ShortCircuitReplica> replicas,
              Map<ExtendedBlockId, InvalidToken> failedLoads,
              Map<Long, ShortCircuitReplica> evictable,
              Map<Long, ShortCircuitReplica> evictableMmapped) {
            finished.setValue(evictableMmapped.isEmpty());
          }
        });
        return finished.booleanValue();
      }
    }, 10, 60000);

    cache.accept(new CountingVisitor(0, -1, -1, -1));
    
    fs.close();
    cluster.shutdown();
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

  /**
   * Test that we can zero-copy read cached data even without disabling
   * checksums.
   */
  @Test(timeout=120000)
  public void testZeroCopyReadOfCachedData() throws Exception {
    BlockReaderTestUtil.enableShortCircuitShmTracing();
    BlockReaderTestUtil.enableBlockReaderFactoryTracing();
    BlockReaderTestUtil.enableHdfsCachingTracing();

    final int TEST_FILE_LENGTH = BLOCK_SIZE;
    final Path TEST_PATH = new Path("/a");
    final int RANDOM_SEED = 23453;
    HdfsConfiguration conf = initZeroCopyTest();
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, false);
    final String CONTEXT = "testZeroCopyReadOfCachedData";
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        DFSTestUtil.roundUpToMultiple(TEST_FILE_LENGTH,
          (int) NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize()));
    MiniDFSCluster cluster = null;
    ByteBuffer result = null, result2 = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    FsDatasetSpi<?> fsd = cluster.getDataNodes().get(0).getFSDataset();
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, TEST_PATH,
        TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
    DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
    byte original[] = DFSTestUtil.
        calculateFileContentsFromSeed(RANDOM_SEED, TEST_FILE_LENGTH);

    // Prior to caching, the file can't be read via zero-copy
    FSDataInputStream fsIn = fs.open(TEST_PATH);
    try {
      result = fsIn.read(null, TEST_FILE_LENGTH / 2,
          EnumSet.noneOf(ReadOption.class));
      Assert.fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    // Cache the file
    fs.addCachePool(new CachePoolInfo("pool1"));
    long directiveId = fs.addCacheDirective(new CacheDirectiveInfo.Builder().
        setPath(TEST_PATH).
        setReplication((short)1).
        setPool("pool1").
        build());
    int numBlocks = (int)Math.ceil((double)TEST_FILE_LENGTH / BLOCK_SIZE);
    DFSTestUtil.verifyExpectedCacheUsage(
        DFSTestUtil.roundUpToMultiple(TEST_FILE_LENGTH, BLOCK_SIZE),
        numBlocks, cluster.getDataNodes().get(0).getFSDataset());
    try {
      result = fsIn.read(null, TEST_FILE_LENGTH,
          EnumSet.noneOf(ReadOption.class));
    } catch (UnsupportedOperationException e) {
      Assert.fail("expected to be able to read cached file via zero-copy");
    }
    Assert.assertArrayEquals(Arrays.copyOfRange(original, 0,
        BLOCK_SIZE), byteBufferToArray(result));
    // Test that files opened after the cache operation has finished
    // still get the benefits of zero-copy (regression test for HDFS-6086)
    FSDataInputStream fsIn2 = fs.open(TEST_PATH);
    try {
      result2 = fsIn2.read(null, TEST_FILE_LENGTH,
          EnumSet.noneOf(ReadOption.class));
    } catch (UnsupportedOperationException e) {
      Assert.fail("expected to be able to read cached file via zero-copy");
    }
    Assert.assertArrayEquals(Arrays.copyOfRange(original, 0,
        BLOCK_SIZE), byteBufferToArray(result2));
    fsIn2.releaseBuffer(result2);
    fsIn2.close();
    
    // check that the replica is anchored 
    final ExtendedBlock firstBlock =
        DFSTestUtil.getFirstBlock(fs, TEST_PATH);
    final ShortCircuitCache cache = ClientContext.get(
        CONTEXT, new DFSClient.Conf(conf)). getShortCircuitCache();
    waitForReplicaAnchorStatus(cache, firstBlock, true, true, 1);
    // Uncache the replica
    fs.removeCacheDirective(directiveId);
    waitForReplicaAnchorStatus(cache, firstBlock, false, true, 1);
    fsIn.releaseBuffer(result);
    waitForReplicaAnchorStatus(cache, firstBlock, false, false, 1);
    DFSTestUtil.verifyExpectedCacheUsage(0, 0, fsd);

    fsIn.close();
    fs.close();
    cluster.shutdown();
  }
  
  private void waitForReplicaAnchorStatus(final ShortCircuitCache cache,
      final ExtendedBlock block, final boolean expectedIsAnchorable,
        final boolean expectedIsAnchored, final int expectedOutstandingMmaps)
          throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        final MutableBoolean result = new MutableBoolean(false);
        cache.accept(new CacheVisitor() {
          @Override
          public void visit(int numOutstandingMmaps,
              Map<ExtendedBlockId, ShortCircuitReplica> replicas,
              Map<ExtendedBlockId, InvalidToken> failedLoads,
              Map<Long, ShortCircuitReplica> evictable,
              Map<Long, ShortCircuitReplica> evictableMmapped) {
            Assert.assertEquals(expectedOutstandingMmaps, numOutstandingMmaps);
            ShortCircuitReplica replica =
                replicas.get(ExtendedBlockId.fromExtendedBlock(block));
            Assert.assertNotNull(replica);
            Slot slot = replica.getSlot();
            if ((expectedIsAnchorable != slot.isAnchorable()) ||
                (expectedIsAnchored != slot.isAnchored())) {
              LOG.info("replica " + replica + " has isAnchorable = " +
                slot.isAnchorable() + ", isAnchored = " + slot.isAnchored() + 
                ".  Waiting for isAnchorable = " + expectedIsAnchorable + 
                ", isAnchored = " + expectedIsAnchored);
              return;
            }
            result.setValue(true);
          }
        });
        return result.toBoolean();
      }
    }, 10, 60000);
  }
  
  @Test
  public void testClientMmapDisable() throws Exception {
    HdfsConfiguration conf = initZeroCopyTest();
    conf.setBoolean(DFS_CLIENT_MMAP_ENABLED, false);
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final int TEST_FILE_LENGTH = 16385;
    final int RANDOM_SEED = 23453;
    final String CONTEXT = "testClientMmapDisable";
    FSDataInputStream fsIn = null;
    DistributedFileSystem fs = null;
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);

    try {
      // With DFS_CLIENT_MMAP_ENABLED set to false, we should not do memory
      // mapped reads.
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
      DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      fsIn = fs.open(TEST_PATH);
      try {
        fsIn.read(null, 1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
        Assert.fail("expected zero-copy read to fail when client mmaps " +
            "were disabled.");
      } catch (UnsupportedOperationException e) {
      }
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }

    fsIn = null;
    fs = null;
    cluster = null;
    try {
      // Now try again with DFS_CLIENT_MMAP_CACHE_SIZE == 0.  It should work.
      conf.setBoolean(DFS_CLIENT_MMAP_ENABLED, true);
      conf.setInt(DFS_CLIENT_MMAP_CACHE_SIZE, 0);
      conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT + ".1");
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          TEST_FILE_LENGTH, (short)1, RANDOM_SEED);
      DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      fsIn = fs.open(TEST_PATH);
      ByteBuffer buf = fsIn.read(null, 1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      fsIn.releaseBuffer(buf);
      // Test EOF behavior
      IOUtils.skipFully(fsIn, TEST_FILE_LENGTH - 1);
      buf = fsIn.read(null, 1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(null, buf);
    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
    }
  }
  
  @Test
  public void test2GBMmapLimit() throws Exception {
    Assume.assumeTrue(BlockReaderTestUtil.shouldTestLargeFiles());
    HdfsConfiguration conf = initZeroCopyTest();
    final long TEST_FILE_LENGTH = 2469605888L;
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "NULL");
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, TEST_FILE_LENGTH);
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final String CONTEXT = "test2GBMmapLimit";
    conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT, CONTEXT);

    FSDataInputStream fsIn = null, fsIn2 = null;
    ByteBuffer buf1 = null, buf2 = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, TEST_FILE_LENGTH, (short)1, 0xB);
      DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      
      fsIn = fs.open(TEST_PATH);
      buf1 = fsIn.read(null, 1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(1, buf1.remaining());
      fsIn.releaseBuffer(buf1);
      buf1 = null;
      fsIn.seek(2147483640L);
      buf1 = fsIn.read(null, 1024, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(7, buf1.remaining());
      Assert.assertEquals(Integer.MAX_VALUE, buf1.limit());
      fsIn.releaseBuffer(buf1);
      buf1 = null;
      Assert.assertEquals(2147483647L, fsIn.getPos());
      try {
        buf1 = fsIn.read(null, 1024,
            EnumSet.of(ReadOption.SKIP_CHECKSUMS));
        Assert.fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
        // expected; can't read past 2GB boundary.
      }
      fsIn.close();
      fsIn = null;

      // Now create another file with normal-sized blocks, and verify we
      // can read past 2GB
      final Path TEST_PATH2 = new Path("/b");
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 268435456L);
      DFSTestUtil.createFile(fs, TEST_PATH2, 1024 * 1024, TEST_FILE_LENGTH,
          268435456L, (short)1, 0xA);
      
      fsIn2 = fs.open(TEST_PATH2);
      fsIn2.seek(2147483640L);
      buf2 = fsIn2.read(null, 1024, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(8, buf2.remaining());
      Assert.assertEquals(2147483648L, fsIn2.getPos());
      fsIn2.releaseBuffer(buf2);
      buf2 = null;
      buf2 = fsIn2.read(null, 1024, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
      Assert.assertEquals(1024, buf2.remaining());
      Assert.assertEquals(2147484672L, fsIn2.getPos());
      fsIn2.releaseBuffer(buf2);
      buf2 = null;
    } finally {
      if (buf1 != null) {
        fsIn.releaseBuffer(buf1);
      }
      if (buf2 != null) {
        fsIn2.releaseBuffer(buf2);
      }
      IOUtils.cleanup(null, fsIn, fsIn2);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
