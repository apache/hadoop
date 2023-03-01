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
package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.primitives.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.io.compress.bzip2.BZip2TextFileWriter;
import org.apache.hadoop.io.compress.bzip2.BZip2Utils;

import static org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.BYBLOCK;
import static org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.CONTINUOUS;
import static org.apache.hadoop.io.compress.bzip2.BZip2TextFileWriter.BLOCK_SIZE;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class TestBZip2Codec {

  private static final long HEADER_LEN = 2;

  private Configuration conf;
  private FileSystem fs;
  private BZip2Codec codec;
  private Decompressor decompressor;
  private Path tempFile;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();

    Path workDir = new Path(System.getProperty("test.build.data", "target"),
        "data/" + getClass().getSimpleName());

    Path inputDir = new Path(workDir, "input");
    tempFile = new Path(inputDir, "test.txt.bz2");

    fs = workDir.getFileSystem(conf);

    codec = new BZip2Codec();
    codec.setConf(new Configuration(/* loadDefaults */ false));
    decompressor = CodecPool.getDecompressor(codec);
  }

  @After
  public void tearDown() throws Exception {
    CodecPool.returnDecompressor(decompressor);
    fs.delete(tempFile, /* recursive */ false);
  }

  @Test
  public void createInputStreamWithStartAndEnd() throws Exception {
    byte[] data1 = newAlternatingByteArray(BLOCK_SIZE, 'a', 'b');
    byte[] data2 = newAlternatingByteArray(BLOCK_SIZE, 'c', 'd');
    byte[] data3 = newAlternatingByteArray(BLOCK_SIZE, 'e', 'f');

    try (BZip2TextFileWriter writer = new BZip2TextFileWriter(tempFile, conf)) {
      writer.write(data1);
      writer.write(data2);
      writer.write(data3);
    }
    long fileSize = fs.getFileStatus(tempFile).getLen();

    List<Long> nextBlockOffsets = BZip2Utils.getNextBlockMarkerOffsets(tempFile, conf);
    long block2Start = nextBlockOffsets.get(0);
    long block3Start = nextBlockOffsets.get(1);

    try (SplitCompressionInputStream stream = newCompressionStream(tempFile, 0, fileSize,
        BYBLOCK)) {
      assertEquals(0, stream.getPos());
      assertCasesWhereReadDoesNotAdvanceStream(stream);
      assertReadingAtPositionZero(stream, data1);
      assertCasesWhereReadDoesNotAdvanceStream(stream);
      assertReadingPastEndOfBlock(stream, block2Start, data2);
      assertReadingPastEndOfBlock(stream, block3Start, data3);
      assertEquals(-1, stream.read());
    }

    try (SplitCompressionInputStream stream = newCompressionStream(tempFile, 1, fileSize - 1,
        BYBLOCK)) {
      assertEquals(block2Start, stream.getPos());
      assertCasesWhereReadDoesNotAdvanceStream(stream);
      assertReadingPastEndOfBlock(stream, block2Start, data2);
      assertCasesWhereReadDoesNotAdvanceStream(stream);
      assertReadingPastEndOfBlock(stream, block3Start, data3);
      assertEquals(-1, stream.read());
    }

    // With continuous mode, only starting at or after the stream header is
    // supported.
    byte[] allData = Bytes.concat(data1, data2, data3);
    assertReadingWithContinuousMode(tempFile, 0, fileSize, allData);
    assertReadingWithContinuousMode(tempFile, HEADER_LEN, fileSize - HEADER_LEN, allData);
  }

  private void assertReadingWithContinuousMode(Path file, long start, long length,
      byte[] expectedData) throws IOException {
    try (SplitCompressionInputStream stream = newCompressionStream(file, start, length,
        CONTINUOUS)) {
      assertEquals(HEADER_LEN, stream.getPos());

      assertRead(stream, expectedData);
      assertEquals(-1, stream.read());

      // When specifying CONTINUOUS read mode, the position ends up not being
      // updated at all.
      assertEquals(HEADER_LEN, stream.getPos());
    }
  }

  private SplitCompressionInputStream newCompressionStream(Path file, long start, long length,
      READ_MODE readMode) throws IOException {
    FSDataInputStream rawIn = fs.open(file);
    rawIn.seek(start);
    long end = start + length;
    return codec.createInputStream(rawIn, decompressor, start, end, readMode);
  }

  private static byte[] newAlternatingByteArray(int size, int... choices) {
    checkArgument(choices.length > 1);
    byte[] result = new byte[size];
    for (int i = 0; i < size; i++) {
      result[i] = (byte) choices[i % choices.length];
    }
    return result;
  }

  private static void assertCasesWhereReadDoesNotAdvanceStream(SplitCompressionInputStream in)
      throws IOException {
    long initialPos = in.getPos();

    assertEquals(0, in.read(new byte[0]));

    assertThatNullPointerException().isThrownBy(() -> in.read(null, 0, 1));
    assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(
        () -> in.read(new byte[5], -1, 2));
    assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(
        () -> in.read(new byte[5], 0, -1));
    assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(
        () -> in.read(new byte[5], 1, 5));

    assertEquals(initialPos, in.getPos());
  }

  private static void assertReadingAtPositionZero(SplitCompressionInputStream in,
      byte[] expectedData) throws IOException {
    byte[] buffer = new byte[expectedData.length];
    assertEquals(1, in.read(buffer, 0, 1));
    assertEquals(expectedData[0], buffer[0]);
    assertEquals(0, in.getPos());

    IOUtils.readFully(in, buffer, 1, expectedData.length - 1);
    assertArrayEquals(expectedData, buffer);
    assertEquals(0, in.getPos());
  }

  private static void assertReadingPastEndOfBlock(SplitCompressionInputStream in,
      long endOfBlockPos, byte[] expectedData) throws IOException {
    byte[] buffer = new byte[expectedData.length];
    assertEquals(1, in.read(buffer));
    assertEquals(expectedData[0], buffer[0]);
    assertEquals(endOfBlockPos + 1, in.getPos());

    IOUtils.readFully(in, buffer, 1, expectedData.length - 1);
    assertArrayEquals(expectedData, buffer);
    assertEquals(endOfBlockPos + 1, in.getPos());
  }

  private static void assertRead(InputStream in, byte[] expectedData) throws IOException {
    byte[] buffer = new byte[expectedData.length];
    IOUtils.readFully(in, buffer);
    assertArrayEquals(expectedData, buffer);
  }
}