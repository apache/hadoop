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

package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSequenceFileAppend {

  private static Configuration conf;
  private static FileSystem fs;
  private static Path ROOT_PATH =
      new Path(GenericTestUtils.getTestDir().getAbsolutePath());

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.set("io.serializations",
        "org.apache.hadoop.io.serializer.JavaSerialization");
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.close();
  }

  @Test(timeout = 30000)
  public void testAppend() throws Exception {

    Path file = new Path(ROOT_PATH, "testseqappend.seq");
    fs.delete(file, true);

    Text key1 = new Text("Key1");
    Text value1 = new Text("Value1");
    Text value2 = new Text("Updated");

    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(key1, value1);
    Writer.Option metadataOption = Writer.metadata(metadata);

    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), metadataOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    metadata.set(key1, value2);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), metadataOption);

    // Verify the Meta data is not changed
    assertEquals(value1, writer.metadata.get(key1));

    writer.append(3L, "three");
    writer.append(4L, "four");

    writer.close();

    verifyAll4Values(file);

    // Verify the Meta data readable after append
    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(value1, reader.getMetadata().get(key1));
    reader.close();

    // Verify failure if the compression details are different
    try {
      Option wrongCompressOption = Writer.compression(CompressionType.RECORD,
          new GzipCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.BLOCK,
          new DefaultCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendRecordCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.RECORD,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(3L, "three");
    writer.append(4L, "four");
    writer.close();

    verifyAll4Values(file);

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendBlockCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.BLOCK,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(3L, "three");
    writer.append(4L, "four");
    writer.close();

    verifyAll4Values(file);

    // Verify failure if the compression details are different or not Provided
    try {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true));
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    // Verify failure if the compression details are different
    try {
      Option wrongCompressOption = Writer.compression(CompressionType.RECORD,
          new GzipCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    try {
      Option wrongCompressOption = Writer.compression(CompressionType.BLOCK,
          new DefaultCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendNoneCompression() throws Exception {
    Path file = new Path(ROOT_PATH, "testseqappendnonecompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.NONE);
    Writer writer =
        SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
            SequenceFile.Writer.keyClass(Long.class),
            SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(1L, "one");
    writer.append(2L, "two");
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(3L, "three");
    writer.append(4L, "four");
    writer.close();

    verifyAll4Values(file);

    // Verify failure if the compression details are different or not Provided
    try {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true));
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException iae) {
      // Expected exception. Ignore it
    }

    // Verify failure if the compression details are different
    try {
      Option wrongCompressOption =
          Writer.compression(CompressionType.RECORD, new GzipCodec());

      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(Long.class),
          SequenceFile.Writer.valueClass(String.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException iae) {
      // Expected exception. Ignore it
    }

    // Codec should be ignored
    Option noneWithCodec =
        Writer.compression(CompressionType.NONE, new DefaultCodec());

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), noneWithCodec);
    writer.close();
    fs.deleteOnExit(file);
  }

  @Test(timeout = 30000)
  public void testAppendSort() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendSort.seq");
    fs.delete(file, true);

    Path sortedFile = new Path(ROOT_PATH, "testseqappendSort.seq.sort");
    fs.delete(sortedFile, true);

    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new JavaSerializationComparator<Long>(), Long.class, String.class, conf);

    Option compressOption = Writer.compression(CompressionType.BLOCK,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class), compressOption);

    writer.append(2L, "two");
    writer.append(1L, "one");

    writer.close();

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(Long.class),
        SequenceFile.Writer.valueClass(String.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(4L, "four");
    writer.append(3L, "three");
    writer.close();

    // Sort file after append
    sorter.sort(file, sortedFile);
    verifyAll4Values(sortedFile);

    fs.deleteOnExit(file);
    fs.deleteOnExit(sortedFile);
  }

  private void verify2Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(1L, reader.next((Object) null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.next((Object) null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertNull(reader.next((Object) null));
    reader.close();
  }

  private void verifyAll4Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertEquals(1L, reader.next((Object) null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.next((Object) null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertEquals(3L, reader.next((Object) null));
    assertEquals("three", reader.getCurrentValue((Object) null));
    assertEquals(4L, reader.next((Object) null));
    assertEquals("four", reader.getCurrentValue((Object) null));
    assertNull(reader.next((Object) null));
    reader.close();
  }
}
