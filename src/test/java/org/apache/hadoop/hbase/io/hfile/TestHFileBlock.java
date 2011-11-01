/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.Compressor;

import static org.apache.hadoop.hbase.io.hfile.Compression.Algorithm.*;
import org.junit.Before;
import org.junit.Test;

public class TestHFileBlock {
  // change this value to activate more logs
  private static final boolean detailedLogging = false;
  private static final boolean[] BOOLEAN_VALUES = new boolean[] { false, true };

  private static final Log LOG = LogFactory.getLog(TestHFileBlock.class);

  static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = {
      NONE, GZ };

  // In case we need to temporarily switch some test cases to just test gzip.
  static final Compression.Algorithm[] GZIP_ONLY  = { GZ };

  private static final int NUM_TEST_BLOCKS = 1000;

  private static final int NUM_READER_THREADS = 26;

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private FileSystem fs;
  private int uncompressedSizeV1;

  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
  }

  public void writeTestBlockContents(DataOutputStream dos) throws IOException {
    // This compresses really well.
    for (int i = 0; i < 1000; ++i)
      dos.writeInt(i / 100);
  }

  public byte[] createTestV1Block(Compression.Algorithm algo)
      throws IOException {
    Compressor compressor = algo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = algo.createCompressionStream(baos, compressor, 0);
    DataOutputStream dos = new DataOutputStream(os);
    BlockType.META.write(dos); // Let's make this a meta block.
    writeTestBlockContents(dos);
    uncompressedSizeV1 = dos.size();
    dos.flush();
    algo.returnCompressor(compressor);
    return baos.toByteArray();
  }

  private byte[] createTestV2Block(Compression.Algorithm algo)
      throws IOException {
    final BlockType blockType = BlockType.DATA;
    HFileBlock.Writer hbw = new HFileBlock.Writer(algo);
    DataOutputStream dos = hbw.startWriting(blockType, false);
    writeTestBlockContents(dos);
    byte[] headerAndData = hbw.getHeaderAndData();
    assertEquals(1000 * 4, hbw.getUncompressedSizeWithoutHeader());
    hbw.releaseCompressor();
    return headerAndData;
  }

  public String createTestBlockStr(Compression.Algorithm algo)
      throws IOException {
    byte[] testV2Block = createTestV2Block(algo);
    int osOffset = HFileBlock.HEADER_SIZE + 9;
    if (osOffset < testV2Block.length) {
      // Force-set the "OS" field of the gzip header to 3 (Unix) to avoid
      // variations across operating systems.
      // See http://www.gzip.org/zlib/rfc-gzip.html for gzip format.
      testV2Block[osOffset] = 3;
    }
    return Bytes.toStringBinary(testV2Block);
  }

  @Test
  public void testNoCompression() throws IOException {
    assertEquals(4000 + HFileBlock.HEADER_SIZE, createTestV2Block(NONE).length);
  }

  @Test
  public void testGzipCompression() throws IOException {
    assertEquals(
        "DATABLK*\\x00\\x00\\x00:\\x00\\x00\\x0F\\xA0\\xFF\\xFF\\xFF\\xFF"
            + "\\xFF\\xFF\\xFF\\xFF"
            // gzip-compressed block: http://www.gzip.org/zlib/rfc-gzip.html
            + "\\x1F\\x8B"  // gzip magic signature
            + "\\x08"  // Compression method: 8 = "deflate"
            + "\\x00"  // Flags
            + "\\x00\\x00\\x00\\x00"  // mtime
            + "\\x00"  // XFL (extra flags)
            // OS (0 = FAT filesystems, 3 = Unix). However, this field
            // sometimes gets set to 0 on Linux and Mac, so we reset it to 3.
            + "\\x03"
            + "\\xED\\xC3\\xC1\\x11\\x00 \\x08\\xC00DD\\xDD\\x7Fa"
            + "\\xD6\\xE8\\xA3\\xB9K\\x84`\\x96Q\\xD3\\xA8\\xDB\\xA8e\\xD4c"
            + "\\xD46\\xEA5\\xEA3\\xEA7\\xE7\\x00LI\\s\\xA0\\x0F\\x00\\x00",
        createTestBlockStr(GZ));
  }

  @Test
  public void testReaderV1() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        byte[] block = createTestV1Block(algo);
        Path path = new Path(TEST_UTIL.getDataTestDir(),
          "blocks_v1_"+ algo);
        LOG.info("Creating temporary file at " + path);
        FSDataOutputStream os = fs.create(path);
        int totalSize = 0;
        int numBlocks = 50;
        for (int i = 0; i < numBlocks; ++i) {
          os.write(block);
          totalSize += block.length;
        }
        os.close();

        FSDataInputStream is = fs.open(path);
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV1(is, algo,
            totalSize);
        HFileBlock b;
        int numBlocksRead = 0;
        long pos = 0;
        while (pos < totalSize) {
          b = hbr.readBlockData(pos, block.length, uncompressedSizeV1, pread);
          b.sanityCheck();
          pos += block.length;
          numBlocksRead++;
        }
        assertEquals(numBlocks, numBlocksRead);
        is.close();
      }
    }
  }

  @Test
  public void testReaderV2() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : new boolean[] { false, true }) {
        Path path = new Path(TEST_UTIL.getDataTestDir(), "blocks_v2_"
            + algo);
        FSDataOutputStream os = fs.create(path);
        HFileBlock.Writer hbw = new HFileBlock.Writer(algo);
        long totalSize = 0;
        for (int blockId = 0; blockId < 2; ++blockId) {
          DataOutputStream dos = hbw.startWriting(BlockType.DATA, false);
          for (int i = 0; i < 1234; ++i)
            dos.writeInt(i);
          hbw.writeHeaderAndData(os);
          totalSize += hbw.getOnDiskSizeWithHeader();
        }
        os.close();

        FSDataInputStream is = fs.open(path);
        HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
            totalSize);
        HFileBlock b = hbr.readBlockData(0, -1, -1, pread);
        is.close();

        b.sanityCheck();
        assertEquals(4936, b.getUncompressedSizeWithoutHeader());
        assertEquals(algo == GZ ? 2173 : 4936, b.getOnDiskSizeWithoutHeader());
        String blockStr = b.toString();

        if (algo == GZ) {
          is = fs.open(path);
          hbr = new HFileBlock.FSReaderV2(is, algo, totalSize);
          b = hbr.readBlockData(0, 2173 + HFileBlock.HEADER_SIZE, -1, pread);
          assertEquals(blockStr, b.toString());
          int wrongCompressedSize = 2172;
          try {
            b = hbr.readBlockData(0, wrongCompressedSize
                + HFileBlock.HEADER_SIZE, -1, pread);
            fail("Exception expected");
          } catch (IOException ex) {
            String expectedPrefix = "On-disk size without header provided is "
                + wrongCompressedSize + ", but block header contains "
                + b.getOnDiskSizeWithoutHeader() + ".";
            assertTrue("Invalid exception message: '" + ex.getMessage()
                + "'.\nMessage is expected to start with: '" + expectedPrefix
                + "'", ex.getMessage().startsWith(expectedPrefix));
          }
          is.close();
        }
      }
    }
  }

  @Test
  public void testPreviousOffset() throws IOException {
    for (Compression.Algorithm algo : COMPRESSION_ALGORITHMS) {
      for (boolean pread : BOOLEAN_VALUES) {
        for (boolean cacheOnWrite : BOOLEAN_VALUES) {
          Random rand = defaultRandom();
          LOG.info("Compression algorithm: " + algo + ", pread=" + pread);
          Path path = new Path(TEST_UTIL.getDataTestDir(), "prev_offset");
          List<Long> expectedOffsets = new ArrayList<Long>();
          List<Long> expectedPrevOffsets = new ArrayList<Long>();
          List<BlockType> expectedTypes = new ArrayList<BlockType>();
          List<ByteBuffer> expectedContents = cacheOnWrite
              ? new ArrayList<ByteBuffer>() : null;
          long totalSize = writeBlocks(rand, algo, path, expectedOffsets,
              expectedPrevOffsets, expectedTypes, expectedContents);

          FSDataInputStream is = fs.open(path);
          HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, algo,
              totalSize);
          long curOffset = 0;
          for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
            if (!pread) {
              assertEquals(is.getPos(), curOffset + (i == 0 ? 0 :
                  HFileBlock.HEADER_SIZE));
            }

            assertEquals(expectedOffsets.get(i).longValue(), curOffset);
            if (detailedLogging) {
              LOG.info("Reading block #" + i + " at offset " + curOffset);
            }
            HFileBlock b = hbr.readBlockData(curOffset, -1, -1, pread);
            if (detailedLogging) {
              LOG.info("Block #" + i + ": " + b);
            }
            assertEquals("Invalid block #" + i + "'s type:",
                expectedTypes.get(i), b.getBlockType());
            assertEquals("Invalid previous block offset for block " + i
                + " of " + "type " + b.getBlockType() + ":",
                (long) expectedPrevOffsets.get(i), b.getPrevBlockOffset());
            b.sanityCheck();
            assertEquals(curOffset, b.getOffset());

            // Now re-load this block knowing the on-disk size. This tests a
            // different branch in the loader.
            HFileBlock b2 = hbr.readBlockData(curOffset,
                b.getOnDiskSizeWithHeader(), -1, pread);
            b2.sanityCheck();

            assertEquals(b.getBlockType(), b2.getBlockType());
            assertEquals(b.getOnDiskSizeWithoutHeader(),
                b2.getOnDiskSizeWithoutHeader());
            assertEquals(b.getOnDiskSizeWithHeader(),
                b2.getOnDiskSizeWithHeader());
            assertEquals(b.getUncompressedSizeWithoutHeader(),
                b2.getUncompressedSizeWithoutHeader());
            assertEquals(b.getPrevBlockOffset(), b2.getPrevBlockOffset());
            assertEquals(curOffset, b2.getOffset());

            curOffset += b.getOnDiskSizeWithHeader();

            if (cacheOnWrite) {
              // In the cache-on-write mode we store uncompressed bytes so we
              // can compare them to what was read by the block reader.

              ByteBuffer bufRead = b.getBufferWithHeader();
              ByteBuffer bufExpected = expectedContents.get(i);
              boolean bytesAreCorrect = Bytes.compareTo(bufRead.array(),
                  bufRead.arrayOffset(), bufRead.limit(),
                  bufExpected.array(), bufExpected.arrayOffset(),
                  bufExpected.limit()) == 0;
              String wrongBytesMsg = "";

              if (!bytesAreCorrect) {
                // Optimization: only construct an error message in case we
                // will need it.
                wrongBytesMsg = "Expected bytes in block #" + i + " (algo="
                    + algo + ", pread=" + pread + "):\n";
                wrongBytesMsg += Bytes.toStringBinary(bufExpected.array(),
                    bufExpected.arrayOffset(), Math.min(32,
                        bufExpected.limit()))
                    + ", actual:\n"
                    + Bytes.toStringBinary(bufRead.array(),
                        bufRead.arrayOffset(), Math.min(32, bufRead.limit()));
              }

              assertTrue(wrongBytesMsg, bytesAreCorrect);
            }
          }

          assertEquals(curOffset, fs.getFileStatus(path).getLen());
          is.close();
        }
      }
    }
  }

  private Random defaultRandom() {
    return new Random(189237);
  }

  private class BlockReaderThread implements Callable<Boolean> {
    private final String clientId;
    private final HFileBlock.FSReader hbr;
    private final List<Long> offsets;
    private final List<BlockType> types;
    private final long fileSize;

    public BlockReaderThread(String clientId,
        HFileBlock.FSReader hbr, List<Long> offsets, List<BlockType> types,
        long fileSize) {
      this.clientId = clientId;
      this.offsets = offsets;
      this.hbr = hbr;
      this.types = types;
      this.fileSize = fileSize;
    }

    @Override
    public Boolean call() throws Exception {
      Random rand = new Random(clientId.hashCode());
      long endTime = System.currentTimeMillis() + 10000;
      int numBlocksRead = 0;
      int numPositionalRead = 0;
      int numWithOnDiskSize = 0;
      while (System.currentTimeMillis() < endTime) {
        int blockId = rand.nextInt(NUM_TEST_BLOCKS);
        long offset = offsets.get(blockId);
        boolean pread = rand.nextBoolean();
        boolean withOnDiskSize = rand.nextBoolean();
        long expectedSize =
          (blockId == NUM_TEST_BLOCKS - 1 ? fileSize
              : offsets.get(blockId + 1)) - offset;

        HFileBlock b;
        try {
          long onDiskSizeArg = withOnDiskSize ? expectedSize : -1;
          b = hbr.readBlockData(offset, onDiskSizeArg, -1, pread);
        } catch (IOException ex) {
          LOG.error("Error in client " + clientId + " trying to read block at "
              + offset + ", pread=" + pread + ", withOnDiskSize=" +
              withOnDiskSize, ex);
          return false;
        }

        assertEquals(types.get(blockId), b.getBlockType());
        assertEquals(expectedSize, b.getOnDiskSizeWithHeader());
        assertEquals(offset, b.getOffset());

        ++numBlocksRead;
        if (pread)
          ++numPositionalRead;
        if (withOnDiskSize)
          ++numWithOnDiskSize;
      }
      LOG.info("Client " + clientId + " successfully read " + numBlocksRead +
        " blocks (with pread: " + numPositionalRead + ", with onDiskSize " +
        "specified: " + numWithOnDiskSize + ")");

      return true;
    }

  }

  @Test
  public void testConcurrentReading() throws Exception {
    for (Compression.Algorithm compressAlgo : COMPRESSION_ALGORITHMS) {
      Path path =
          new Path(TEST_UTIL.getDataTestDir(), "concurrent_reading");
      Random rand = defaultRandom();
      List<Long> offsets = new ArrayList<Long>();
      List<BlockType> types = new ArrayList<BlockType>();
      writeBlocks(rand, compressAlgo, path, offsets, null, types, null);
      FSDataInputStream is = fs.open(path);
      long fileSize = fs.getFileStatus(path).getLen();
      HFileBlock.FSReader hbr = new HFileBlock.FSReaderV2(is, compressAlgo,
          fileSize);

      Executor exec = Executors.newFixedThreadPool(NUM_READER_THREADS);
      ExecutorCompletionService<Boolean> ecs =
          new ExecutorCompletionService<Boolean>(exec);

      for (int i = 0; i < NUM_READER_THREADS; ++i) {
        ecs.submit(new BlockReaderThread("reader_" + (char) ('A' + i), hbr,
            offsets, types, fileSize));
      }

      for (int i = 0; i < NUM_READER_THREADS; ++i) {
        Future<Boolean> result = ecs.take();
        assertTrue(result.get());
        if (detailedLogging) {
          LOG.info(String.valueOf(i + 1)
            + " reader threads finished successfully (algo=" + compressAlgo
            + ")");
        }
      }

      is.close();
    }
  }

  private long writeBlocks(Random rand, Compression.Algorithm compressAlgo,
      Path path, List<Long> expectedOffsets, List<Long> expectedPrevOffsets,
      List<BlockType> expectedTypes, List<ByteBuffer> expectedContents
  ) throws IOException {
    boolean cacheOnWrite = expectedContents != null;
    FSDataOutputStream os = fs.create(path);
    HFileBlock.Writer hbw = new HFileBlock.Writer(compressAlgo);
    Map<BlockType, Long> prevOffsetByType = new HashMap<BlockType, Long>();
    long totalSize = 0;
    for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
      int blockTypeOrdinal = rand.nextInt(BlockType.values().length);
      BlockType bt = BlockType.values()[blockTypeOrdinal];
      DataOutputStream dos = hbw.startWriting(bt, cacheOnWrite);
      for (int j = 0; j < rand.nextInt(500); ++j) {
        // This might compress well.
        dos.writeShort(i + 1);
        dos.writeInt(j + 1);
      }

      if (expectedOffsets != null)
        expectedOffsets.add(os.getPos());

      if (expectedPrevOffsets != null) {
        Long prevOffset = prevOffsetByType.get(bt);
        expectedPrevOffsets.add(prevOffset != null ? prevOffset : -1);
        prevOffsetByType.put(bt, os.getPos());
      }

      expectedTypes.add(bt);

      hbw.writeHeaderAndData(os);
      totalSize += hbw.getOnDiskSizeWithHeader();

      if (cacheOnWrite)
        expectedContents.add(hbw.getUncompressedBufferWithHeader());

      if (detailedLogging) {
        LOG.info("Writing block #" + i + " of type " + bt
            + ", uncompressed size " + hbw.getUncompressedSizeWithoutHeader()
            + " at offset " + os.getPos());
      }
    }
    os.close();
    LOG.info("Created a temporary file at " + path + ", "
        + fs.getFileStatus(path).getLen() + " byte, compression=" +
        compressAlgo);
    return totalSize;
  }

  @Test
  public void testBlockHeapSize() {
    // We have seen multiple possible values for this estimate of the heap size
    // of a ByteBuffer, presumably depending on the JDK version.
    assertTrue(HFileBlock.BYTE_BUFFER_HEAP_SIZE == 64 ||
               HFileBlock.BYTE_BUFFER_HEAP_SIZE == 80);

    for (int size : new int[] { 100, 256, 12345 }) {
      byte[] byteArr = new byte[HFileBlock.HEADER_SIZE + size];
      ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
      HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, buf,
          true, -1);
      long expected = ClassSize.align(ClassSize.estimateBase(HFileBlock.class,
          true)
          + ClassSize.estimateBase(buf.getClass(), true)
          + HFileBlock.HEADER_SIZE + size);
      assertEquals(expected, block.heapSize());
    }
  }

}
