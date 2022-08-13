/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.bzip2.BZip2TextFileWriter;
import org.apache.hadoop.io.compress.bzip2.BZip2Utils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.io.compress.bzip2.BZip2TextFileWriter.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

public abstract class BaseTestLineRecordReaderBZip2 {

  // LF stands for line feed
  private static final byte[] LF = new byte[] {'\n'};
  // CR stands for cartridge return
  private static final byte[] CR = new byte[] {'\r'};
  private static final byte[] CR_LF = new byte[] {'\r', '\n'};

  private Configuration conf;
  private FileSystem fs;
  private Path tempFile;

  public Configuration getConf() {
    return conf;
  }

  public FileSystem getFs() {
    return fs;
  }

  public Path getTempFile() {
    return tempFile;
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();

    Path workDir = new Path(
        System.getProperty("test.build.data", "target"),
        "data/" + getClass().getSimpleName());

    fs = workDir.getFileSystem(conf);

    Path inputDir = new Path(workDir, "input");
    tempFile = new Path(inputDir, "test.txt.bz2");
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(tempFile, /* recursive */ false);
  }

  @Test
  public void firstBlockEndsWithLF() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void firstBlockEndsWithLFSecondBlockStartsWithLF() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, LF);
      // Write 254 empty rows terminating at LF, as those records will get
      // rolled into the first block record due to run-length encoding, the
      // 255th LF character will trigger a run to be written to the block. We
      // only need 254 LF characters since the last byte written by prior
      // writeManyRecords call is already a LF.
      writer.writeManyRecords(254, 254, LF);

      // This LF character should be the first byte of the second block, but
      // if splitting at blocks, the first split will read this record as the
      // additional record.
      writer.writeRecord(1, LF);

      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1255, 2});
  }

  @Test
  public void firstBlockEndsWithLFSecondBlockStartsWithCR() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, LF);
      writer.writeRecord(1, CR);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void firstBlockEndsWithCRLF() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, CR_LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void lastRecordContentSpanAcrossBlocks()
      throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, LF);
      writer.writeRecord(100, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void lastRecordOfBlockHasItsLFInNextBlock() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, LF);
      // The LF character is the first byte of the second block
      writer.writeRecord(51, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void lastRecordOfFirstBlockHasItsCRLFInSecondBlock() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, LF);
      // Both CR + LF characters are the first two bytes of second block
      writer.writeRecord(52, CR_LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void lastRecordOfFirstBlockHasItsCRLFPartlyInSecondBlock()
      throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, LF);
      // The CR character is the last byte of the first block and the LF is
      // the firs byte of the second block
      writer.writeRecord(51, CR_LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void lastByteInFirstBlockIsCRFirstByteInSecondBlockIsNotLF()
      throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, CR);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
      writer.writeRecord(10, LF);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void usingCRDelimiterWithSmallestBufferSize() throws Exception {
    // Forces calling LineReader#fillBuffer for ever byte read
    conf.set(IO_FILE_BUFFER_SIZE_KEY, "1");

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, CR);
      writer.writeRecord(100, CR);
      writer.writeRecord(10, CR);
      writer.writeRecord(10, CR);
      writer.writeRecord(10, CR);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void delimitedByCRSpanningThreeBlocks() throws Exception {
    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeRecord(3 * BLOCK_SIZE, CR);
      writer.writeRecord(3 * BLOCK_SIZE, CR);
      writer.writeRecord(3 * BLOCK_SIZE, CR);
    }
    assertRecordCountsPerSplit(tempFile,
        new long[] {1, 0, 1, 0, 0, 1, 0, 0, 0});
  }

  @Test
  public void customDelimiterLastThreeBytesInBlockAreDelimiter()
      throws Exception {
    byte[] delimiter = new byte[] {'e', 'n', 'd'};
    setDelimiter(delimiter);

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE, 1000, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void customDelimiterDelimiterSpansAcrossBlocks()
      throws Exception {
    byte[] delimiter = new byte[] {'e', 'n', 'd'};
    setDelimiter(delimiter);

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, delimiter);
      writer.writeRecord(52, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1001, 2});
  }

  @Test
  public void customDelimiterLastRecordDelimiterStartsAtNextBlockStart()
      throws Exception {
    byte[] delimiter = new byte[] {'e', 'n', 'd'};
    setDelimiter(delimiter);

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 50, 999, delimiter);
      writer.writeRecord(53, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  @Test
  public void customDelimiterLastBlockBytesShareCommonPrefixWithDelimiter()
      throws Exception {
    byte[] delimiter = new byte[] {'e', 'n', 'd'};
    setDelimiter(delimiter);

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.writeManyRecords(BLOCK_SIZE - 4, 999, delimiter);
      // The first 4 bytes, "an e", will be the last 4 bytes of the first block,
      // the last byte being 'e' which matches the first character of the
      // delimiter "end". The first byte of the next block also matches the
      // second byte of the delimiter "n"; however the next character "c" does
      // not match the last character of the delimiter. Thus an additional
      // record should not be read for the split that reads the first block.
      // The split that reads the second block will just discard
      // "nchanting tale coming to an end".
      writer.write("an enchanting tale coming to an end");
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
      writer.writeRecord(10, delimiter);
    }
    assertRecordCountsPerSplit(tempFile, new long[] {1000, 3});
  }

  protected abstract BaseLineRecordReaderHelper newReader(Path file);

  private void assertRecordCountsPerSplit(
      Path path, long[] countsIfSplitAtBlocks) throws IOException {
    RecordCountAssert countAssert =
        new RecordCountAssert(path, countsIfSplitAtBlocks);
    countAssert.assertSingleSplit();
    countAssert.assertSplittingAtBlocks();
    countAssert.assertSplittingJustAfterSecondBlockStarts();
  }

  private class RecordCountAssert {

    private final BaseLineRecordReaderHelper reader;
    private final long numBlocks;
    private final long[] countsIfSplitAtBlocks;
    private final long fileSize;
    private final long totalRecords;
    private final List<Long> nextBlockOffsets;

    RecordCountAssert(
        Path path, long[] countsIfSplitAtBlocks) throws IOException {
      this.reader = newReader(path);
      this.countsIfSplitAtBlocks = countsIfSplitAtBlocks;
      this.fileSize = getFileSize(path);
      this.totalRecords = Arrays.stream(countsIfSplitAtBlocks).sum();
      this.numBlocks = countsIfSplitAtBlocks.length;
      this.nextBlockOffsets = BZip2Utils.getNextBlockMarkerOffsets(path, conf);

      assertEquals(numBlocks, nextBlockOffsets.size() + 1);
    }

    private void assertSingleSplit() throws IOException {
      assertEquals(totalRecords, reader.countRecords(0, fileSize));
    }

    private void assertSplittingAtBlocks() throws IOException {
      for (int i = 0; i < numBlocks; i++) {
        long start = i == 0 ? 0 : nextBlockOffsets.get(i - 1);
        long end = i == numBlocks - 1 ? fileSize : nextBlockOffsets.get(i);
        long length = end - start;

        String message = "At i=" + i;
        long expectedCount = countsIfSplitAtBlocks[i];
        assertEquals(
            message, expectedCount, reader.countRecords(start, length));
      }
    }

    private void assertSplittingJustAfterSecondBlockStarts()
        throws IOException {
      if (numBlocks <= 1) {
        return;
      }
      long recordsInFirstTwoBlocks =
          countsIfSplitAtBlocks[0] + countsIfSplitAtBlocks[1];
      long remainingRecords = totalRecords - recordsInFirstTwoBlocks;

      long firstSplitSize = nextBlockOffsets.get(0) + 1;
      assertEquals(
          recordsInFirstTwoBlocks,
          reader.countRecords(0, firstSplitSize));
      assertEquals(
          remainingRecords,
          reader.countRecords(firstSplitSize, fileSize - firstSplitSize));
    }
  }

  private long getFileSize(Path path) throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  private void setDelimiter(byte[] delimiter) {
    conf.set("textinputformat.record.delimiter",
        new String(delimiter, StandardCharsets.UTF_8));
  }
}
