/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test TFile loading resilience.
 */
public class TestTFileLoading {

  private static final String ROOT =
      GenericTestUtils.getTestDir().getAbsolutePath();

  private static final int BLOCK_SIZE = 512;

  private static final String COMPRESSION = Compression.Algorithm.NONE.getName();

  private static final String JCLASS_COMPARATOR =
      TFile.COMPARATOR_JCLASS + LongWritable.Comparator.class.getName();

  private FileSystem fs;

  private Path path;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    path = new Path(ROOT, "TestTFileLoading");
    fs = FileSystem.getLocal(conf);
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(path, true);
  }

  private Configuration enableJclass() {
    Configuration conf = new Configuration();
    conf.setBoolean(TFile.TFILE_COMPARATOR_JCLASS_ENABLED, true);
    return conf;
  }

  @Test
  public void testJClassComparatorRejectedByDefault() throws Exception {
    try (FSDataOutputStream out = fs.create(path)) {
      intercept(IllegalArgumentException.class, () ->
          new Writer(out, BLOCK_SIZE, COMPRESSION, JCLASS_COMPARATOR, new Configuration()));
    }
  }

  @Test
  public void testJClassComparatorMustBeRawComparator() throws Exception {
    Configuration conf = enableJclass();
    String notAComparator =
        TFile.COMPARATOR_JCLASS + Chunk.class.getName();
    try (FSDataOutputStream out = fs.create(path)) {
      intercept(IllegalArgumentException.class, () ->
          new Writer(out, BLOCK_SIZE, COMPRESSION, notAComparator, conf));
    }
  }

  /**
   * When jclass support is disabled, tfile readers fail to load files
   * containing them.
   */
  @Test
  public void testReaderRejectsJClassWhenDisabled() throws Exception {
    // write a valid, sorted file naming a jclass comparator.
    try (FSDataOutputStream out = fs.create(path);
         Writer writer = new Writer(out, BLOCK_SIZE, COMPRESSION, JCLASS_COMPARATOR,
             enableJclass())) {
      LongWritable key = new LongWritable(0);
      for (long i = 0; i < 4; ++i) {
        key.set(i);
        try (DataOutputStream dos = writer.prepareAppendKey(-1)) {
          key.write(dos);
        }
        try (DataOutputStream dos = writer.prepareAppendValue(-1)) {
          dos.write(("value-" + i).getBytes());
        }
      }
    }

    long len = fs.getFileStatus(path).getLen();
    // a reader that has not enabled the feature refuses the file.
    Configuration disabled = new Configuration();
    try (FSDataInputStream in = fs.open(path)) {
      intercept(IllegalArgumentException.class, () ->
          new Reader(in, len, disabled));
    }
    // with the feature enabled the same file opens.
    try (FSDataInputStream in = fs.open(path)) {
      Reader reader = new Reader(in, len, enableJclass());
      assertThat(reader.getComparator())
          .describedAs("comparator")
          .isNotNull();
      reader.close();
    }
  }

  /**
   * A bounded string read rejects an over-long length before allocating.
   */
  @Test
  public void testReadStringRejectsOversizedLength() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // claim a huge length but supply no payload.
    Utils.writeVInt(new DataOutputStream(bos), Integer.MAX_VALUE);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    intercept(EOFException.class, () -> Utils.readString(in, 1024));
  }

  /**
   * A string length less than minus 1 is rejected.
   */
  @Test
  public void testReadStringRejectsNegativeLength() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Utils.writeVInt(new DataOutputStream(bos), -2);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    intercept(NegativeArraySizeException.class, () -> Utils.readString(in));
  }

  /**
   * A length of -1 is null; this is the classic behavior.
   */
  @Test
  public void testReadStringMinus1MapsToNull() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Utils.writeVInt(new DataOutputStream(bos), -1);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    assertThat(Utils.readString(in)).isNull();
  }

  /**
   * Reject index entry records out of range.
   */
  @Test
  public void testIndexEntryKeyLengthBounded() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // a key length well beyond the 64KB maximum.
    Utils.writeVInt(new DataOutputStream(bos), 1 << 20);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
    intercept(IOException.class, () ->
        new TFile.TFileIndexEntry(in));
  }
}
