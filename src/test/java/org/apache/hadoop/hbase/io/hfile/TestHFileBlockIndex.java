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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexChunk;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestHFileBlockIndex {

  @Parameters
  public static Collection<Object[]> compressionAlgorithms() {
    return HBaseTestingUtility.COMPRESSION_ALGORITHMS_PARAMETERIZED;
  }

  public TestHFileBlockIndex(Compression.Algorithm compr) {
    this.compr = compr;
  }

  private static final Log LOG = LogFactory.getLog(TestHFileBlockIndex.class);

  private static final int NUM_DATA_BLOCKS = 1000;
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final int SMALL_BLOCK_SIZE = 4096;
  private static final int NUM_KV = 10000;

  private static FileSystem fs;
  private Path path;
  private Random rand;
  private long rootIndexOffset;
  private int numRootEntries;
  private int numLevels;
  private static final List<byte[]> keys = new ArrayList<byte[]>();
  private final Compression.Algorithm compr;
  private byte[] firstKeyInFile;
  private Configuration conf;

  private static final int[] INDEX_CHUNK_SIZES = { 4096, 512, 384 };
  private static final int[] EXPECTED_NUM_LEVELS = { 2, 3, 4 };
  private static final int[] UNCOMPRESSED_INDEX_SIZES =
      { 19187, 21813, 23086 };

  static {
    assert INDEX_CHUNK_SIZES.length == EXPECTED_NUM_LEVELS.length;
    assert INDEX_CHUNK_SIZES.length == UNCOMPRESSED_INDEX_SIZES.length;
  }

  @Before
  public void setUp() throws IOException {
    keys.clear();
    rand = new Random(2389757);
    firstKeyInFile = null;
    conf = TEST_UTIL.getConfiguration();

    // This test requires at least HFile format version 2.
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);

    fs = FileSystem.get(conf);
  }

  @Test
  public void testBlockIndex() throws IOException {
    path = new Path(HBaseTestingUtility.getTestDir(), "block_index_" + compr);
    writeWholeIndex();
    readIndex();
  }

  /**
   * A wrapper around a block reader which only caches the results of the last
   * operation. Not thread-safe.
   */
  private static class BlockReaderWrapper implements HFileBlock.BasicReader {

    private HFileBlock.BasicReader realReader;
    private long prevOffset;
    private long prevOnDiskSize;
    private long prevUncompressedSize;
    private boolean prevPread;
    private HFileBlock prevBlock;

    public int hitCount = 0;
    public int missCount = 0;

    public BlockReaderWrapper(HFileBlock.BasicReader realReader) {
      this.realReader = realReader;
    }

    @Override
    public HFileBlock readBlockData(long offset, long onDiskSize,
        int uncompressedSize, boolean pread) throws IOException {
      if (offset == prevOffset && onDiskSize == prevOnDiskSize &&
          uncompressedSize == prevUncompressedSize && pread == prevPread) {
        hitCount += 1;
        return prevBlock;
      }

      missCount += 1;
      prevBlock = realReader.readBlockData(offset, onDiskSize,
          uncompressedSize, pread);
      prevOffset = offset;
      prevOnDiskSize = onDiskSize;
      prevUncompressedSize = uncompressedSize;
      prevPread = pread;

      return prevBlock;
    }
  }

  public void readIndex() throws IOException {
    long fileSize = fs.getFileStatus(path).getLen();
    LOG.info("Size of " + path + ": " + fileSize);

    FSDataInputStream istream = fs.open(path);
    HFileBlock.FSReader blockReader = new HFileBlock.FSReaderV2(istream,
        compr, fs.getFileStatus(path).getLen());

    BlockReaderWrapper brw = new BlockReaderWrapper(blockReader);
    HFileBlockIndex.BlockIndexReader indexReader =
        new HFileBlockIndex.BlockIndexReader(
            Bytes.BYTES_RAWCOMPARATOR, numLevels, brw);

    indexReader.readRootIndex(blockReader.blockRange(rootIndexOffset,
        fileSize).nextBlockAsStream(BlockType.ROOT_INDEX), numRootEntries);

    long prevOffset = -1;
    int i = 0;
    int expectedHitCount = 0;
    int expectedMissCount = 0;
    LOG.info("Total number of keys: " + keys.size());
    for (byte[] key : keys) {
      assertTrue(key != null);
      assertTrue(indexReader != null);
      HFileBlock b = indexReader.seekToDataBlock(key, 0, key.length, null);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(key, firstKeyInFile) < 0) {
        assertTrue(b == null);
        ++i;
        continue;
      }

      String keyStr = "key #" + i + ", " + Bytes.toStringBinary(key);

      assertTrue("seekToDataBlock failed for " + keyStr, b != null);

      if (prevOffset == b.getOffset()) {
        assertEquals(++expectedHitCount, brw.hitCount);
      } else {
        LOG.info("First key in a new block: " + keyStr + ", block offset: "
            + b.getOffset() + ")");
        assertTrue(b.getOffset() > prevOffset);
        assertEquals(++expectedMissCount, brw.missCount);
        prevOffset = b.getOffset();
      }
      ++i;
    }

    istream.close();
  }

  private void writeWholeIndex() throws IOException {
    assertEquals(0, keys.size());
    HFileBlock.Writer hbw = new HFileBlock.Writer(compr);
    FSDataOutputStream outputStream = fs.create(path);
    HFileBlockIndex.BlockIndexWriter biw =
        new HFileBlockIndex.BlockIndexWriter(hbw, null, null);

    for (int i = 0; i < NUM_DATA_BLOCKS; ++i) {
      hbw.startWriting(BlockType.DATA, false).write(
          String.valueOf(rand.nextInt(1000)).getBytes());
      long blockOffset = outputStream.getPos();
      hbw.writeHeaderAndData(outputStream);

      byte[] firstKey = null;
      for (int j = 0; j < 16; ++j) {
        byte[] k = TestHFileWriterV2.randomOrderedKey(rand, i * 16 + j);
        keys.add(k);
        if (j == 8)
          firstKey = k;
      }
      assertTrue(firstKey != null);
      if (firstKeyInFile == null)
        firstKeyInFile = firstKey;
      biw.addEntry(firstKey, blockOffset, hbw.getOnDiskSizeWithHeader());

      writeInlineBlocks(hbw, outputStream, biw, false);
    }
    writeInlineBlocks(hbw, outputStream, biw, true);
    rootIndexOffset = biw.writeIndexBlocks(outputStream);
    outputStream.close();

    numLevels = biw.getNumLevels();
    numRootEntries = biw.getNumRootEntries();

    LOG.info("Index written: numLevels=" + numLevels + ", numRootEntries=" +
        numRootEntries + ", rootIndexOffset=" + rootIndexOffset);
  }

  private void writeInlineBlocks(HFileBlock.Writer hbw,
      FSDataOutputStream outputStream, HFileBlockIndex.BlockIndexWriter biw,
      boolean isClosing) throws IOException {
    while (biw.shouldWriteBlock(isClosing)) {
      long offset = outputStream.getPos();
      biw.writeInlineBlock(hbw.startWriting(biw.getInlineBlockType(), false));
      hbw.writeHeaderAndData(outputStream);
      biw.blockWritten(offset, hbw.getOnDiskSizeWithHeader(),
          hbw.getUncompressedSizeWithoutHeader());
      LOG.info("Wrote an inline index block at " + offset + ", size " +
          hbw.getOnDiskSizeWithHeader());
    }
  }

  private static final long getDummyFileOffset(int i) {
    return i * 185 + 379;
  }

  private static final int getDummyOnDiskSize(int i) {
    return i * i * 37 + i * 19 + 13;
  }

  @Test
  public void testSecondaryIndexBinarySearch() throws IOException {
    int numTotalKeys = 99;
    assertTrue(numTotalKeys % 2 == 1); // Ensure no one made this even.

    // We only add odd-index keys into the array that we will binary-search.
    int numSearchedKeys = (numTotalKeys - 1) / 2;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    dos.writeInt(numSearchedKeys);
    int curAllEntriesSize = 0;
    int numEntriesAdded = 0;

    // Only odd-index elements of this array are used to keep the secondary
    // index entries of the corresponding keys.
    int secondaryIndexEntries[] = new int[numTotalKeys];

    for (int i = 0; i < numTotalKeys; ++i) {
      byte[] k = TestHFileWriterV2.randomOrderedKey(rand, i * 2);
      keys.add(k);
      String msgPrefix = "Key #" + i + " (" + Bytes.toStringBinary(k) + "): ";
      StringBuilder padding = new StringBuilder();
      while (msgPrefix.length() + padding.length() < 70)
        padding.append(' ');
      msgPrefix += padding;
      if (i % 2 == 1) {
        dos.writeInt(curAllEntriesSize);
        secondaryIndexEntries[i] = curAllEntriesSize;
        LOG.info(msgPrefix + "secondary index entry #" + ((i - 1) / 2) +
            ", offset " + curAllEntriesSize);
        curAllEntriesSize += k.length
            + HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
        ++numEntriesAdded;
      } else {
        secondaryIndexEntries[i] = -1;
        LOG.info(msgPrefix + "not in the searched array");
      }
    }

    // Make sure the keys are increasing.
    for (int i = 0; i < keys.size() - 1; ++i)
      assertTrue(Bytes.BYTES_RAWCOMPARATOR.compare(keys.get(i),
          keys.get(i + 1)) < 0);

    dos.writeInt(curAllEntriesSize);
    assertEquals(numSearchedKeys, numEntriesAdded);
    int secondaryIndexOffset = dos.size();
    assertEquals(Bytes.SIZEOF_INT * (numSearchedKeys + 2),
        secondaryIndexOffset);

    for (int i = 1; i <= numTotalKeys - 1; i += 2) {
      assertEquals(dos.size(),
          secondaryIndexOffset + secondaryIndexEntries[i]);
      long dummyFileOffset = getDummyFileOffset(i);
      int dummyOnDiskSize = getDummyOnDiskSize(i);
      LOG.debug("Storing file offset=" + dummyFileOffset + " and onDiskSize=" +
          dummyOnDiskSize + " at offset " + dos.size());
      dos.writeLong(dummyFileOffset);
      dos.writeInt(dummyOnDiskSize);
      LOG.debug("Stored key " + ((i - 1) / 2) +" at offset " + dos.size());
      dos.write(keys.get(i));
    }

    dos.writeInt(curAllEntriesSize);

    ByteBuffer nonRootIndex = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < numTotalKeys; ++i) {
      byte[] searchKey = keys.get(i);
      byte[] arrayHoldingKey = new byte[searchKey.length +
                                        searchKey.length / 2];

      // To make things a bit more interesting, store the key we are looking
      // for at a non-zero offset in a new array.
      System.arraycopy(searchKey, 0, arrayHoldingKey, searchKey.length / 2,
            searchKey.length);

      int searchResult = BlockIndexReader.binarySearchNonRootIndex(
          arrayHoldingKey, searchKey.length / 2, searchKey.length, nonRootIndex,
          Bytes.BYTES_RAWCOMPARATOR);
      String lookupFailureMsg = "Failed to look up key #" + i + " ("
          + Bytes.toStringBinary(searchKey) + ")";

      int expectedResult;
      int referenceItem;

      if (i % 2 == 1) {
        // This key is in the array we search as the element (i - 1) / 2. Make
        // sure we find it.
        expectedResult = (i - 1) / 2;
        referenceItem = i;
      } else {
        // This key is not in the array but between two elements on the array,
        // in the beginning, or in the end. The result should be the previous
        // key in the searched array, or -1 for i = 0.
        expectedResult = i / 2 - 1;
        referenceItem = i - 1;
      }

      assertEquals(lookupFailureMsg, expectedResult, searchResult);

      // Now test we can get the offset and the on-disk-size using a
      // higher-level API function.s
      boolean locateBlockResult =
        BlockIndexReader.locateNonRootIndexEntry(nonRootIndex, arrayHoldingKey,
            searchKey.length / 2, searchKey.length, Bytes.BYTES_RAWCOMPARATOR);

      if (i == 0) {
        assertFalse(locateBlockResult);
      } else {
        assertTrue(locateBlockResult);
        String errorMsg = "i=" + i + ", position=" + nonRootIndex.position();
        assertEquals(errorMsg, getDummyFileOffset(referenceItem),
            nonRootIndex.getLong());
        assertEquals(errorMsg, getDummyOnDiskSize(referenceItem),
            nonRootIndex.getInt());
      }
    }

  }

  @Test
  public void testBlockIndexChunk() throws IOException {
    BlockIndexChunk c = new BlockIndexChunk();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int N = 1000;
    int[] numSubEntriesAt = new int[N];
    int numSubEntries = 0;
    for (int i = 0; i < N; ++i) {
      baos.reset();
      DataOutputStream dos = new DataOutputStream(baos);
      c.writeNonRoot(dos);
      assertEquals(c.getNonRootSize(), dos.size());

      baos.reset();
      dos = new DataOutputStream(baos);
      c.writeRoot(dos);
      assertEquals(c.getRootSize(), dos.size());

      byte[] k = TestHFileWriterV2.randomOrderedKey(rand, i);
      numSubEntries += rand.nextInt(5) + 1;
      keys.add(k);
      c.add(k, getDummyFileOffset(i), getDummyOnDiskSize(i), numSubEntries);
    }

    // Test the ability to look up the entry that contains a particular
    // deeper-level index block's entry ("sub-entry"), assuming a global
    // 0-based ordering of sub-entries. This is needed for mid-key calculation.
    for (int i = 0; i < N; ++i) {
      for (int j = i == 0 ? 0 : numSubEntriesAt[i - 1];
           j < numSubEntriesAt[i];
           ++j) {
        assertEquals(i, c.getEntryBySubEntry(j));
      }
    }
  }

  /** Checks if the HeapSize calculator is within reason */
  @Test
  public void testHeapSizeForBlockIndex() throws IOException {
    Class<HFileBlockIndex.BlockIndexReader> cl =
        HFileBlockIndex.BlockIndexReader.class;
    long expected = ClassSize.estimateBase(cl, false);

    HFileBlockIndex.BlockIndexReader bi =
        new HFileBlockIndex.BlockIndexReader(Bytes.BYTES_RAWCOMPARATOR, 1);
    long actual = bi.heapSize();

    // Since the arrays in BlockIndex(byte [][] blockKeys, long [] blockOffsets,
    // int [] blockDataSizes) are all null they are not going to show up in the
    // HeapSize calculation, so need to remove those array costs from expected.
    expected -= ClassSize.align(3 * ClassSize.ARRAY);

    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
  }

  /**
   * Testing block index through the HFile writer/reader APIs. Allows to test
   * setting index block size through configuration, intermediate-level index
   * blocks, and caching index blocks on write.
   *
   * @throws IOException
   */
  @Test
  public void testHFileWriterAndReader() throws IOException {
    Path hfilePath = new Path(HBaseTestingUtility.getTestDir(),
        "hfile_for_block_index");
    BlockCache blockCache = StoreFile.getBlockCache(conf);

    for (int testI = 0; testI < INDEX_CHUNK_SIZES.length; ++testI) {
      int indexBlockSize = INDEX_CHUNK_SIZES[testI];
      int expectedNumLevels = EXPECTED_NUM_LEVELS[testI];
      LOG.info("Index block size: " + indexBlockSize + ", compression: "
          + compr);
      // Evict all blocks that were cached-on-write by the previous invocation.
      blockCache.evictBlocksByPrefix(hfilePath.getName()
          + HFile.CACHE_KEY_SEPARATOR);

      conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, indexBlockSize);
      Set<String> keyStrSet = new HashSet<String>();
      byte[][] keys = new byte[NUM_KV][];
      byte[][] values = new byte[NUM_KV][];

      // Write the HFile
      {
        HFile.Writer writer = HFile.getWriterFactory(conf).createWriter(fs,
            hfilePath, SMALL_BLOCK_SIZE, compr, KeyValue.KEY_COMPARATOR);
        Random rand = new Random(19231737);

        for (int i = 0; i < NUM_KV; ++i) {
          byte[] row = TestHFileWriterV2.randomOrderedKey(rand, i);

          // Key will be interpreted by KeyValue.KEY_COMPARATOR
          byte[] k = KeyValue.createFirstOnRow(row, 0, row.length, row, 0, 0,
              row, 0, 0).getKey();

          byte[] v = TestHFileWriterV2.randomValue(rand);
          writer.append(k, v);
          keys[i] = k;
          values[i] = v;
          keyStrSet.add(Bytes.toStringBinary(k));

          if (i > 0) {
            assertTrue(KeyValue.KEY_COMPARATOR.compare(keys[i - 1],
                keys[i]) < 0);
          }
        }

        writer.close();
      }

      // Read the HFile
      HFile.Reader reader = HFile.createReader(fs, hfilePath, blockCache,
          false, true);
      assertEquals(expectedNumLevels,
          reader.getTrailer().getNumDataIndexLevels());

      assertTrue(Bytes.equals(keys[0], reader.getFirstKey()));
      assertTrue(Bytes.equals(keys[NUM_KV - 1], reader.getLastKey()));
      LOG.info("Last key: " + Bytes.toStringBinary(keys[NUM_KV - 1]));

      for (boolean pread : new boolean[] { false, true }) {
        HFileScanner scanner = reader.getScanner(true, pread);
        for (int i = 0; i < NUM_KV; ++i) {
          checkSeekTo(keys, scanner, i);
          checkKeyValue("i=" + i, keys[i], values[i], scanner.getKey(),
              scanner.getValue());
        }
        assertTrue(scanner.seekTo());
        for (int i = NUM_KV - 1; i >= 0; --i) {
          checkSeekTo(keys, scanner, i);
          checkKeyValue("i=" + i, keys[i], values[i], scanner.getKey(),
              scanner.getValue());
        }
      }

      // Manually compute the mid-key and validate it.
      HFileReaderV2 reader2 = (HFileReaderV2) reader;
      HFileBlock.FSReader fsReader = reader2.getUncachedBlockReader();

      HFileBlock.BlockIterator iter = fsReader.blockRange(0,
          reader.getTrailer().getLoadOnOpenDataOffset());
      HFileBlock block;
      List<byte[]> blockKeys = new ArrayList<byte[]>();
      while ((block = iter.nextBlock()) != null) {
        if (block.getBlockType() != BlockType.LEAF_INDEX)
          return;
        ByteBuffer b = block.getBufferReadOnly();
        int n = b.getInt();
        // One int for the number of items, and n + 1 for the secondary index.
        int entriesOffset = Bytes.SIZEOF_INT * (n + 2);

        // Get all the keys from the leaf index block. S
        for (int i = 0; i < n; ++i) {
          int keyRelOffset = b.getInt(Bytes.SIZEOF_INT * (i + 1));
          int nextKeyRelOffset = b.getInt(Bytes.SIZEOF_INT * (i + 2));
          int keyLen = nextKeyRelOffset - keyRelOffset;
          int keyOffset = b.arrayOffset() + entriesOffset + keyRelOffset +
              HFileBlockIndex.SECONDARY_INDEX_ENTRY_OVERHEAD;
          byte[] blockKey = Arrays.copyOfRange(b.array(), keyOffset, keyOffset
              + keyLen);
          String blockKeyStr = Bytes.toString(blockKey);
          blockKeys.add(blockKey);

          // If the first key of the block is not among the keys written, we
          // are not parsing the non-root index block format correctly.
          assertTrue("Invalid block key from leaf-level block: " + blockKeyStr,
              keyStrSet.contains(blockKeyStr));
        }
      }

      // Validate the mid-key.
      assertEquals(
          Bytes.toStringBinary(blockKeys.get((blockKeys.size() - 1) / 2)),
          Bytes.toStringBinary(reader.midkey()));

      assertEquals(UNCOMPRESSED_INDEX_SIZES[testI],
          reader.getTrailer().getUncompressedDataIndexSize());

      reader.close();
    }
  }

  private void checkSeekTo(byte[][] keys, HFileScanner scanner, int i)
      throws IOException {
    assertEquals("Failed to seek to key #" + i + " ("
        + Bytes.toStringBinary(keys[i]) + ")", 0, scanner.seekTo(keys[i]));
  }

  private void assertArrayEqualsBuffer(String msgPrefix, byte[] arr,
      ByteBuffer buf) {
    assertEquals(msgPrefix + ": expected " + Bytes.toStringBinary(arr)
        + ", actual " + Bytes.toStringBinary(buf), 0, Bytes.compareTo(arr, 0,
        arr.length, buf.array(), buf.arrayOffset(), buf.limit()));
  }

  /** Check a key/value pair after it was read by the reader */
  private void checkKeyValue(String msgPrefix, byte[] expectedKey,
      byte[] expectedValue, ByteBuffer keyRead, ByteBuffer valueRead) {
    if (!msgPrefix.isEmpty())
      msgPrefix += ". ";

    assertArrayEqualsBuffer(msgPrefix + "Invalid key", expectedKey, keyRead);
    assertArrayEqualsBuffer(msgPrefix + "Invalid value", expectedValue,
        valueRead);
  }

}
