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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestSequenceFileAppend {

  private static Configuration conf;
  private static FileSystem fs;
  private static Path ROOT_PATH =
      new Path(GenericTestUtils.getTestDir().getAbsolutePath());

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new Configuration();
    conf.set("io.serializations",
        "org.apache.hadoop.io.serializer.WritableSerialization");
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    fs.close();
  }

  @Test
  @Timeout(value = 30)
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
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class), metadataOption);

    writer.append(new LongWritable(1L), new Text("one"));
    writer.append(new LongWritable(2L), new Text("two"));
    writer.close();

    verify2Values(file);

    metadata.set(key1, value2);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.appendIfExists(true), metadataOption);

    // Verify the Meta data is not changed
    assertEquals(value1, writer.metadata.get(key1));

    writer.append(new LongWritable(3L), new Text("three"));
    writer.append(new LongWritable(4L), new Text("four"));

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
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
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
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    fs.deleteOnExit(file);
  }

  @Test
  @Timeout(value = 30)
  public void testAppendRecordCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.RECORD,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class), compressOption);

    writer.append(new LongWritable(1L), new Text("one"));
    writer.append(new LongWritable(2L), new Text("two"));
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(new LongWritable(3L), new Text("three"));
    writer.append(new LongWritable(4L), new Text("four"));
    writer.close();

    verifyAll4Values(file);

    fs.deleteOnExit(file);
  }

  @Test
  @Timeout(value = 30)
  public void testAppendBlockCompression() throws Exception {
    GenericTestUtils.assumeInNativeProfile();

    Path file = new Path(ROOT_PATH, "testseqappendblockcompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.BLOCK,
        new GzipCodec());
    Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class), compressOption);

    writer.append(new LongWritable(1L), new Text("one"));
    writer.append(new LongWritable(2L), new Text("two"));
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(new LongWritable(3L), new Text("three"));
    writer.append(new LongWritable(4L), new Text("four"));
    writer.close();

    verifyAll4Values(file);

    // Verify failure if the compression details are different or not Provided
    try {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
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
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
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
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
          SequenceFile.Writer.appendIfExists(true), wrongCompressOption);
      writer.close();
      fail("Expected IllegalArgumentException for compression options");
    } catch (IllegalArgumentException IAE) {
      // Expected exception. Ignore it
    }

    fs.deleteOnExit(file);
  }

  @Test
  @Timeout(value = 30)
  public void testAppendNoneCompression() throws Exception {
    Path file = new Path(ROOT_PATH, "testseqappendnonecompr.seq");
    fs.delete(file, true);

    Option compressOption = Writer.compression(CompressionType.NONE);
    Writer writer =
        SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
            SequenceFile.Writer.keyClass(LongWritable.class),
            SequenceFile.Writer.valueClass(Text.class), compressOption);

    writer.append(new LongWritable(1L), new Text("one"));
    writer.append(new LongWritable(2L), new Text("two"));
    writer.close();

    verify2Values(file);

    writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.appendIfExists(true), compressOption);

    writer.append(new LongWritable(3L), new Text("three"));
    writer.append(new LongWritable(4L), new Text("four"));
    writer.close();

    verifyAll4Values(file);

    // Verify failure if the compression details are different or not Provided
    try {
      writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file),
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
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
          SequenceFile.Writer.keyClass(LongWritable.class),
          SequenceFile.Writer.valueClass(Text.class),
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
        SequenceFile.Writer.keyClass(LongWritable.class),
        SequenceFile.Writer.valueClass(Text.class),
        SequenceFile.Writer.appendIfExists(true), noneWithCodec);
    writer.close();
    fs.deleteOnExit(file);
  }

  private void verify2Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(1L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("one"));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(2L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("two"));
    assertThat(reader.next((Object) null)).isNull();
    reader.close();
  }

  private void verifyAll4Values(Path file) throws IOException {
    Reader reader = new Reader(conf, Reader.file(file));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(1L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("one"));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(2L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("two"));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(3L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("three"));
    assertThat(reader.next((Object) null)).isEqualTo(new LongWritable(4L));
    assertThat(reader.getCurrentValue((Object) null)).isEqualTo(new Text("four"));
    assertThat(reader.next((Object) null)).isNull();
    reader.close();
  }
}
