/**
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing writing a version 2 {@link HFile}. This is a low-level test written
 * during the development of {@link HFileWriterV2}.
 */
public class TestHFileWriterV2 {

  private static final Log LOG = LogFactory.getLog(TestHFileWriterV2.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testHFileFormatV2() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
        "testHFileFormatV2");

    final Compression.Algorithm COMPRESS_ALGO = Compression.Algorithm.GZ;
    HFileWriterV2 writer = new HFileWriterV2(conf, new CacheConfig(conf), fs,
        hfilePath, 4096, COMPRESS_ALGO, KeyValue.KEY_COMPARATOR);

    long totalKeyLength = 0;
    long totalValueLength = 0;

    Random rand = new Random(9713312); // Just a fixed seed.

    final int ENTRY_COUNT = 10000;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    for (int i = 0; i < ENTRY_COUNT; ++i) {
      byte[] keyBytes = randomOrderedKey(rand, i);

      // A random-length random value.
      byte[] valueBytes = randomValue(rand);
      writer.append(keyBytes, valueBytes);

      totalKeyLength += keyBytes.length;
      totalValueLength += valueBytes.length;

      keys.add(keyBytes);
      values.add(valueBytes);
    }

    // Add in an arbitrary order. They will be sorted lexicographically by
    // the key.
    writer.appendMetaBlock("CAPITAL_OF_USA", new Text("Washington, D.C."));
    writer.appendMetaBlock("CAPITAL_OF_RUSSIA", new Text("Moscow"));
    writer.appendMetaBlock("CAPITAL_OF_FRANCE", new Text("Paris"));

    writer.close();

    FSDataInputStream fsdis = fs.open(hfilePath);

    // A "manual" version of a new-format HFile reader. This unit test was
    // written before the V2 reader was fully implemented.

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer =
        FixedFileTrailer.readFromStream(fsdis, fileSize);

    assertEquals(2, trailer.getVersion());
    assertEquals(ENTRY_COUNT, trailer.getEntryCount());

    HFileBlock.FSReader blockReader =
        new HFileBlock.FSReaderV2(fsdis, COMPRESS_ALGO, fileSize);

    // Counters for the number of key/value pairs and the number of blocks
    int entriesRead = 0;
    int blocksRead = 0;

    // Scan blocks the way the reader would scan them
    fsdis.seek(0);
    long curBlockPos = 0;
    while (curBlockPos <= trailer.getLastDataBlockOffset()) {
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, -1, false);
      assertEquals(BlockType.DATA, block.getBlockType());
      ByteBuffer buf = block.getBufferWithoutHeader();
      while (buf.hasRemaining()) {
        int keyLen = buf.getInt();
        int valueLen = buf.getInt();

        byte[] key = new byte[keyLen];
        buf.get(key);

        byte[] value = new byte[valueLen];
        buf.get(value);

        // A brute-force check to see that all keys and values are correct.
        assertTrue(Bytes.compareTo(key, keys.get(entriesRead)) == 0);
        assertTrue(Bytes.compareTo(value, values.get(entriesRead)) == 0);

        ++entriesRead;
      }
      ++blocksRead;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }
    LOG.info("Finished reading: entries=" + entriesRead + ", blocksRead="
        + blocksRead);
    assertEquals(ENTRY_COUNT, entriesRead);

    // Meta blocks. We can scan until the load-on-open data offset (which is
    // the root block index offset in version 2) because we are not testing
    // intermediate-level index blocks here.

    int metaCounter = 0;
    while (fsdis.getPos() < trailer.getLoadOnOpenDataOffset()) {
      LOG.info("Current offset: " + fsdis.getPos() + ", scanning until " +
          trailer.getLoadOnOpenDataOffset());
      HFileBlock block = blockReader.readBlockData(curBlockPos, -1, -1, false);
      assertEquals(BlockType.META, block.getBlockType());
      Text t = new Text();
      block.readInto(t);
      Text expectedText =
          (metaCounter == 0 ? new Text("Paris") : metaCounter == 1 ? new Text(
              "Moscow") : new Text("Washington, D.C."));
      assertEquals(expectedText, t);
      LOG.info("Read meta block data: " + t);
      ++metaCounter;
      curBlockPos += block.getOnDiskSizeWithHeader();
    }

    fsdis.close();
  }

  // Static stuff used by various HFile v2 unit tests

  private static final String COLUMN_FAMILY_NAME = "_-myColumnFamily-_";
  private static final int MIN_ROW_OR_QUALIFIER_LENGTH = 64;
  private static final int MAX_ROW_OR_QUALIFIER_LENGTH = 128;

  /**
   * Generates a random key that is guaranteed to increase as the given index i
   * increases. The result consists of a prefix, which is a deterministic
   * increasing function of i, and a random suffix.
   *
   * @param rand
   *          random number generator to use
   * @param i
   * @return
   */
  public static byte[] randomOrderedKey(Random rand, int i) {
    StringBuilder k = new StringBuilder();

    // The fixed-length lexicographically increasing part of the key.
    for (int bitIndex = 31; bitIndex >= 0; --bitIndex) {
      if ((i & (1 << bitIndex)) == 0)
        k.append("a");
      else
        k.append("b");
    }

    // A random-length random suffix of the key.
    for (int j = 0; j < rand.nextInt(50); ++j)
      k.append(randomReadableChar(rand));

    byte[] keyBytes = k.toString().getBytes();
    return keyBytes;
  }

  public static byte[] randomValue(Random rand) {
    StringBuilder v = new StringBuilder();
    for (int j = 0; j < 1 + rand.nextInt(2000); ++j) {
      v.append((char) (32 + rand.nextInt(95)));
    }

    byte[] valueBytes = v.toString().getBytes();
    return valueBytes;
  }

  public static final char randomReadableChar(Random rand) {
    int i = rand.nextInt(26 * 2 + 10 + 1);
    if (i < 26)
      return (char) ('A' + i);
    i -= 26;

    if (i < 26)
      return (char) ('a' + i);
    i -= 26;

    if (i < 10)
      return (char) ('0' + i);
    i -= 10;

    assert i == 0;
    return '_';
  }

  public static byte[] randomRowOrQualifier(Random rand) {
    StringBuilder field = new StringBuilder();
    int fieldLen = MIN_ROW_OR_QUALIFIER_LENGTH
        + rand.nextInt(MAX_ROW_OR_QUALIFIER_LENGTH
            - MIN_ROW_OR_QUALIFIER_LENGTH + 1);
    for (int i = 0; i < fieldLen; ++i)
      field.append(randomReadableChar(rand));
    return field.toString().getBytes();
  }

  public static KeyValue randomKeyValue(Random rand) {
    return new KeyValue(randomRowOrQualifier(rand),
        COLUMN_FAMILY_NAME.getBytes(), randomRowOrQualifier(rand),
        randomValue(rand));
  }

}
