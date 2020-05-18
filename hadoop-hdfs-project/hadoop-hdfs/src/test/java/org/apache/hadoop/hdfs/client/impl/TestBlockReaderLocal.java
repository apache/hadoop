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
package org.apache.hadoop.hdfs.client.impl;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ReadStatistics;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.ShmId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockReaderLocal {
  private static TemporarySocketDirectory sockDir;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }

  public static void assertArrayRegionsEqual(byte []buf1, int off1, byte []buf2,
      int off2, int len) {
    for (int i = 0; i < len; i++) {
      if (buf1[off1 + i] != buf2[off2 + i]) {
        Assert.fail("arrays differ at byte " +  i + ". " +
          "The first array has " + (int)buf1[off1 + i] +
          ", but the second array has " + (int)buf2[off2 + i]);
      }
    }
  }

  /**
   * Similar to IOUtils#readFully(). Reads bytes in a loop.
   *
   * @param reader           The BlockReaderLocal to read bytes from
   * @param buf              The ByteBuffer to read into
   * @param off              The offset in the buffer to read into
   * @param len              The number of bytes to read.
   *
   * @throws IOException     If it could not read the requested number of bytes
   */
  private static void readFully(BlockReaderLocal reader,
      ByteBuffer buf, int off, int len) throws IOException {
    int amt = len;
    while (amt > 0) {
      buf.limit(off + len);
      buf.position(off);
      long ret = reader.read(buf);
      if (ret < 0) {
        throw new EOFException( "Premature EOF from BlockReaderLocal " +
            "after reading " + (len - amt) + " byte(s).");
      }
      amt -= ret;
      off += ret;
    }
  }

  private static class BlockReaderLocalTest {
    final static int TEST_LENGTH = 1234567;
    final static int BYTES_PER_CHECKSUM = 512;

    public void setConfiguration(HdfsConfiguration conf) {
      // default: no-op
    }
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
      // default: no-op
    }
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      // default: no-op
    }
    public void doTest(BlockReaderLocal reader, byte[] original, int shift)
            throws IOException {
      // default: no-op
    }  }

  public void runBlockReaderLocalTest(BlockReaderLocalTest test,
      boolean checksum, long readahead, int shortCircuitCachesNum)
          throws IOException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        !checksum);
    conf.setLong(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD, readahead);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_NUM,
        shortCircuitCachesNum);
    test.setConfiguration(conf);
    FileInputStream dataIn = null, metaIn = null;
    final Path TEST_PATH = new Path("/a");
    final long RANDOM_SEED = 4567L;
    final int blockSize = 10 * 1024;
    BlockReaderLocal blockReaderLocal = null;
    FSDataInputStream fsIn = null;
    byte original[] = new byte[BlockReaderLocalTest.TEST_LENGTH];

    FileSystem fs = null;
    ShortCircuitShm shm = null;
    RandomAccessFile raf = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH, 1024,
          BlockReaderLocalTest.TEST_LENGTH, blockSize, (short)1, RANDOM_SEED);
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
      IOUtils.readFully(fsIn, original, 0,
          BlockReaderLocalTest.TEST_LENGTH);
      fsIn.close();
      fsIn = null;
      for (int i = 0; i < shortCircuitCachesNum; i++) {
        ExtendedBlock block = DFSTestUtil.getAllBlocks(
                fs, TEST_PATH).get(i).getBlock();
        File dataFile = cluster.getBlockFile(0, block);
        File metaFile = cluster.getBlockMetadataFile(0, block);

        ShortCircuitCache shortCircuitCache =
                ClientContext.getFromConf(conf).getShortCircuitCache(
                        block.getBlockId());
        test.setup(dataFile, checksum);
        FileInputStream[] streams = {
            new FileInputStream(dataFile),
            new FileInputStream(metaFile)
        };
        dataIn = streams[0];
        metaIn = streams[1];
        ExtendedBlockId key = new ExtendedBlockId(block.getBlockId(),
                block.getBlockPoolId());
        raf = new RandomAccessFile(
                new File(sockDir.getDir().getAbsolutePath(),
                        UUID.randomUUID().toString()), "rw");
        raf.setLength(8192);
        FileInputStream shmStream = new FileInputStream(raf.getFD());
        shm = new ShortCircuitShm(ShmId.createRandom(), shmStream);
        ShortCircuitReplica replica =
                new ShortCircuitReplica(key, dataIn, metaIn, shortCircuitCache,
                        Time.now(), shm.allocAndRegisterSlot(
                        ExtendedBlockId.fromExtendedBlock(block)));
        blockReaderLocal = new BlockReaderLocal.Builder(
                new DfsClientConf.ShortCircuitConf(conf)).
                setFilename(TEST_PATH.getName()).
                setBlock(block).
                setShortCircuitReplica(replica).
                setCachingStrategy(new CachingStrategy(false, readahead)).
                setVerifyChecksum(checksum).
                build();
        dataIn = null;
        metaIn = null;
        test.doTest(blockReaderLocal, original, i * blockSize);
        // BlockReaderLocal should not alter the file position.
        Assert.assertEquals(0, streams[0].getChannel().position());
        Assert.assertEquals(0, streams[1].getChannel().position());
      }
      cluster.shutdown();
      cluster = null;

    } finally {
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
      if (dataIn != null) dataIn.close();
      if (metaIn != null) metaIn.close();
      if (blockReaderLocal != null) blockReaderLocal.close();
      if (shm != null) shm.free();
      if (raf != null) raf.close();
    }
  }

  public void runBlockReaderLocalTest(BlockReaderLocalTest test,
      boolean checksum, long readahead) throws IOException {
    runBlockReaderLocalTest(test, checksum, readahead, 1);
  }

  private static class TestBlockReaderLocalImmediateClose
      extends BlockReaderLocalTest {
  }

  @Test
  public void testBlockReaderLocalImmediateClose() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), true, 0);
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), false, 0);
  }

  private static class TestBlockReaderSimpleReads
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte[] buf = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 512);
      assertArrayRegionsEqual(original, 0, buf, 0, 512);
      reader.readFully(buf, 512, 512);
      assertArrayRegionsEqual(original, 512, buf, 512, 512);
      reader.readFully(buf, 1024, 513);
      assertArrayRegionsEqual(original, 1024, buf, 1024, 513);
      reader.readFully(buf, 1537, 514);
      assertArrayRegionsEqual(original, 1537, buf, 1537, 514);
      // Readahead is always at least the size of one chunk in this test.
      Assert.assertTrue(reader.getMaxReadaheadLength() >=
          BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    }
  }

  @Test
  public void testBlockReaderSimpleReads() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderSimpleReadsShortReadahead() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true,
        BlockReaderLocalTest.BYTES_PER_CHECKSUM - 1);
  }

  @Test
  public void testBlockReaderSimpleReadsNoChecksum() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), false,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderSimpleReadsNoReadahead() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true, 0);
  }

  @Test
  public void testBlockReaderSimpleReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), false, 0);
  }

  private static class TestBlockReaderLocalArrayReads2
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte[] buf = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 10);
      assertArrayRegionsEqual(original, 0, buf, 0, 10);
      reader.readFully(buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf, 10, 100);
      reader.readFully(buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf, 110, 700);
      reader.readFully(buf, 810, 1); // from offset 810 to offset 811
      reader.readFully(buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf, 811, 5);
      reader.readFully(buf, 816, 900); // skip from offset 816 to offset 1716
      reader.readFully(buf, 1716, 5);
      assertArrayRegionsEqual(original, 1716, buf, 1716, 5);
    }
  }

  @Test
  public void testBlockReaderLocalArrayReads2() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(),
        true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(),
        false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(), true, 0);
  }

  @Test
  public void testBlockReaderLocalArrayReads2NoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReads2(), false, 0);
  }

  private static class TestBlockReaderLocalByteBufferReads
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      readFully(reader, buf, 0, 10);
      assertArrayRegionsEqual(original, 0, buf.array(), 0, 10);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }

  @Test
  public void testBlockReaderLocalByteBufferReads()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
        true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferReads(),
        false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
         true, 0);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferReads(),
        false, 0);
  }

  /**
   * Test reads that bypass the bounce buffer (because they are aligned
   * and bigger than the readahead).
   */
  private static class TestBlockReaderLocalByteBufferFastLaneReads
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      ByteBuffer buf = ByteBuffer.allocateDirect(TEST_LENGTH);
      readFully(reader, buf, 0, 5120);
      buf.flip();
      assertArrayRegionsEqual(original, 0,
          DFSTestUtil.asArray(buf), 0,
          5120);
      reader.skip(1537);
      readFully(reader, buf, 0, 1);
      buf.flip();
      assertArrayRegionsEqual(original, 6657,
          DFSTestUtil.asArray(buf), 0,
          1);
      reader.forceAnchorable();
      readFully(reader, buf, 0, 5120);
      buf.flip();
      assertArrayRegionsEqual(original, 6658,
          DFSTestUtil.asArray(buf), 0,
          5120);
      reader.forceUnanchorable();
      readFully(reader, buf, 0, 513);
      buf.flip();
      assertArrayRegionsEqual(original, 11778,
          DFSTestUtil.asArray(buf), 0,
          513);
      reader.skip(3);
      readFully(reader, buf, 0, 50);
      buf.flip();
      assertArrayRegionsEqual(original, 12294,
          DFSTestUtil.asArray(buf), 0,
          50);
    }
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReads()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        true, 2 * BlockReaderLocalTest.BYTES_PER_CHECKSUM);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferFastLaneReads(),
        false, 2 * BlockReaderLocalTest.BYTES_PER_CHECKSUM);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalByteBufferFastLaneReadsNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalByteBufferFastLaneReads(),
        false, 0);
  }

  private static class TestBlockReaderLocalReadCorruptStart
      extends BlockReaderLocalTest {
    boolean usingChecksums = false;
    @Override
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
      RandomAccessFile bf = null;
      this.usingChecksums = usingChecksums;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte[] buf = new byte[TEST_LENGTH];
      if (usingChecksums) {
        try {
          reader.readFully(buf, 0, 10);
          Assert.fail("did not detect corruption");
        } catch (IOException e) {
          // expected
        }
      } else {
        reader.readFully(buf, 0, 10);
      }
    }
  }

  @Test
  public void testBlockReaderLocalReadCorruptStart()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorruptStart(), true,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  private static class TestBlockReaderLocalReadCorrupt
      extends BlockReaderLocalTest {
    boolean usingChecksums = false;
    @Override
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
      RandomAccessFile bf = null;
      this.usingChecksums = usingChecksums;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.seek(1539);
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte[] buf = new byte[TEST_LENGTH];
      try {
        reader.readFully(buf, 0, 10);
        assertArrayRegionsEqual(original, 0, buf, 0, 10);
        reader.readFully(buf, 10, 100);
        assertArrayRegionsEqual(original, 10, buf, 10, 100);
        reader.readFully(buf, 110, 700);
        assertArrayRegionsEqual(original, 110, buf, 110, 700);
        reader.skip(1); // skip from offset 810 to offset 811
        reader.readFully(buf, 811, 5);
        assertArrayRegionsEqual(original, 811, buf, 811, 5);
        reader.readFully(buf, 816, 900);
        if (usingChecksums) {
          // We should detect the corruption when using a checksum file.
          Assert.fail("did not detect corruption");
        }
      } catch (ChecksumException e) {
        if (!usingChecksums) {
          Assert.fail("didn't expect to get ChecksumException: not " +
              "using checksums.");
        }
      }
    }
  }

  @Test
  public void testBlockReaderLocalReadCorrupt()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), true,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), false,
        HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), true, 0);
  }

  @Test
  public void testBlockReaderLocalReadCorruptNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), false, 0);
  }

  private static class TestBlockReaderLocalWithMlockChanges
      extends BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, boolean usingChecksums)
        throws IOException {
    }

    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      reader.skip(1);
      readFully(reader, buf, 1, 9);
      assertArrayRegionsEqual(original, 1, buf.array(), 1, 9);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      reader.forceAnchorable();
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.forceUnanchorable();
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }

  @Test
  public void testBlockReaderLocalWithMlockChanges()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalWithMlockChangesNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalWithMlockChanges(),
        false, 0);
  }

  private static class TestBlockReaderLocalOnFileWithoutChecksum
      extends BlockReaderLocalTest {
    @Override
    public void setConfiguration(HdfsConfiguration conf) {
      conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "NULL");
    }

    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      Assert.assertTrue(!reader.getVerifyChecksum());
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      reader.skip(1);
      readFully(reader, buf, 1, 9);
      assertArrayRegionsEqual(original, 1, buf.array(), 1, 9);
      readFully(reader, buf, 10, 100);
      assertArrayRegionsEqual(original, 10, buf.array(), 10, 100);
      reader.forceAnchorable();
      readFully(reader, buf, 110, 700);
      assertArrayRegionsEqual(original, 110, buf.array(), 110, 700);
      reader.forceUnanchorable();
      reader.skip(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      assertArrayRegionsEqual(original, 811, buf.array(), 811, 5);
    }
  }

  private static class TestBlockReaderLocalReadZeroBytes
      extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException {
      byte emptyArr[] = new byte[0];
      Assert.assertEquals(0, reader.read(emptyArr, 0, 0));
      ByteBuffer emptyBuf = ByteBuffer.wrap(emptyArr);
      Assert.assertEquals(0, reader.read(emptyBuf));
      reader.skip(1);
      Assert.assertEquals(0, reader.read(emptyArr, 0, 0));
      Assert.assertEquals(0, reader.read(emptyBuf));
      reader.skip(BlockReaderLocalTest.TEST_LENGTH - 1);
      Assert.assertEquals(-1, reader.read(emptyArr, 0, 0));
      Assert.assertEquals(-1, reader.read(emptyBuf));
    }
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalOnFileWithoutChecksumNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalOnFileWithoutChecksum(),
        false, 0);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytes()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        true, 0);
  }

  @Test
  public void testBlockReaderLocalReadZeroBytesNoChecksumNoReadahead()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadZeroBytes(),
        false, 0);
  }


  @Test(timeout=60000)
  public void TestStatisticsForShortCircuitLocalRead() throws Exception {
    testStatistics(true);
  }

  @Test(timeout=60000)
  public void TestStatisticsForLocalRead() throws Exception {
    testStatistics(false);
  }

  private void testStatistics(boolean isShortCircuit) throws Exception {
    Assume.assumeTrue(DomainSocket.getLoadingFailureReason() == null);
    HdfsConfiguration conf = new HdfsConfiguration();
    TemporarySocketDirectory sockDir = null;
    if (isShortCircuit) {
      DFSInputStream.tcpReadsDisabledForTesting = true;
      sockDir = new TemporarySocketDirectory();
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(), "TestStatisticsForLocalRead.%d.sock").
          getAbsolutePath());
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      DomainSocket.disableBindPathValidation();
    } else {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    }
    MiniDFSCluster cluster = null;
    final Path TEST_PATH = new Path("/a");
    final long RANDOM_SEED = 4567L;
    FSDataInputStream fsIn = null;
    byte original[] = new byte[BlockReaderLocalTest.TEST_LENGTH];
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          hosts(new String[] {NetUtils.getLocalHostname()}).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          BlockReaderLocalTest.TEST_LENGTH, (short)1, RANDOM_SEED);
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
      IOUtils.readFully(fsIn, original, 0,
          BlockReaderLocalTest.TEST_LENGTH);
      HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
      Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH,
          dfsIn.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH,
          dfsIn.getReadStatistics().getTotalLocalBytesRead());
      if (isShortCircuit) {
        Assert.assertEquals(BlockReaderLocalTest.TEST_LENGTH,
            dfsIn.getReadStatistics().getTotalShortCircuitBytesRead());
      } else {
        Assert.assertEquals(0,
            dfsIn.getReadStatistics().getTotalShortCircuitBytesRead());
      }
      fsIn.close();
      fsIn = null;
    } finally {
      DFSInputStream.tcpReadsDisabledForTesting = false;
      if (fsIn != null) fsIn.close();
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
      if (sockDir != null) sockDir.close();
    }
  }

  @Test(timeout = 60000)
  public void testStatisticsForErasureCodingRead() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();

    final ErasureCodingPolicy ecPolicy =
        StripedFileTestUtil.getDefaultECPolicy();
    final int numDataNodes =
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
    // The length of test file is one full strip + one partial stripe. And
    // it is not bound to the stripe cell size.
    final int length = ecPolicy.getCellSize() * (numDataNodes + 1) + 123;
    final long randomSeed = 4567L;
    final short repl = 1;
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.enableErasureCodingPolicy(ecPolicy.getName());

      Path ecDir = new Path("/ec");
      fs.mkdirs(ecDir);
      fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
      Path nonEcDir = new Path("/noEc");
      fs.mkdirs(nonEcDir);

      byte[] buf = new byte[length];

      Path nonEcFile = new Path(nonEcDir, "file1");
      DFSTestUtil.createFile(fs, nonEcFile, length, repl, randomSeed);
      try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(nonEcFile)) {
        IOUtils.readFully(in, buf, 0, length);

        ReadStatistics stats = in.getReadStatistics();
        Assert.assertEquals(BlockType.CONTIGUOUS, stats.getBlockType());
        Assert.assertEquals(length, stats.getTotalBytesRead());
        Assert.assertEquals(length, stats.getTotalLocalBytesRead());
      }

      Path ecFile = new Path(ecDir, "file2");
      DFSTestUtil.createFile(fs, ecFile, length, repl, randomSeed);

      // Shutdown a DataNode that holds a data block, to trigger EC decoding.
      final BlockLocation[] locs = fs.getFileBlockLocations(ecFile, 0, length);
      final String[] nodes = locs[0].getNames();
      cluster.stopDataNode(nodes[0]);

      try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(ecFile)) {
        IOUtils.readFully(in, buf, 0, length);

        ReadStatistics stats = in.getReadStatistics();
        Assert.assertEquals(BlockType.STRIPED, stats.getBlockType());
        Assert.assertEquals(length, stats.getTotalLocalBytesRead());
        Assert.assertEquals(length, stats.getTotalBytesRead());
        Assert.assertTrue(stats.getTotalEcDecodingTimeMillis() > 0);
      }
    }
  }

  private static class TestBlockReaderFiveShortCircutCachesReads
          extends BlockReaderLocalTest {
    @Override
    public void doTest(BlockReaderLocal reader, byte[] original, int shift)
            throws IOException {
      byte[] buf = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 512);
      assertArrayRegionsEqual(original, shift, buf, 0, 512);
      reader.readFully(buf, 512, 512);
      assertArrayRegionsEqual(original, 512 + shift, buf, 512, 512);
      reader.readFully(buf, 1024, 513);
      assertArrayRegionsEqual(original, 1024 + shift, buf, 1024, 513);
      reader.readFully(buf, 1537, 514);
      assertArrayRegionsEqual(original, 1537 + shift, buf, 1537, 514);
      // Readahead is always at least the size of one chunk in this test.
      Assert.assertTrue(reader.getMaxReadaheadLength() >=
              BlockReaderLocalTest.BYTES_PER_CHECKSUM);
    }
  }

  @Test
  public void testBlockReaderFiveShortCircutCachesReads() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT,
            5);
  }

  @Test
  public void testBlockReaderFiveShortCircutCachesReadsShortReadahead()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            true, BlockReaderLocalTest.BYTES_PER_CHECKSUM - 1,
            5);
  }

  @Test
  public void testBlockReaderFiveShortCircutCachesReadsNoChecksum()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            false, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT,
            5);
  }

  @Test
  public void testBlockReaderFiveShortCircutCachesReadsNoReadahead()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            true, 0, 5);
  }

  @Test
  public void testBlockReaderFiveShortCircutCachesReadsNoChecksumNoReadahead()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            false, 0, 5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBlockReaderShortCircutCachesOutOfRangeBelow()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT,
            0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBlockReaderShortCircutCachesOutOfRangeAbove()
          throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderFiveShortCircutCachesReads(),
            true, HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT,
            555);
  }

}
