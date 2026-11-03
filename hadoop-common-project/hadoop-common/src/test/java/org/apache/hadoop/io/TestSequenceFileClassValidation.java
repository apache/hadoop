/*
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

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link SequenceFile.Reader} resolves the key, value and codec
 * class names from a file header without loading them eagerly, and rejects a
 * class that no configured serializer (or, for the codec, the codec type) can
 * handle.
 */
public class TestSequenceFileClassValidation extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestSequenceFileClassValidation.class);

  /** Name of {@link Sentinel}, referenced as a string so it is never loaded. */
  private static final String SENTINEL =
      TestSequenceFileClassValidation.class.getName() + "$Sentinel";

  private Configuration conf;
  private FileSystem fs;
  private File testDir;

  @BeforeEach
  public void setup() throws IOException {
    LoadFlag.reset();
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testDir = GenericTestUtils.getTestDir("TestSequenceFileClassValidation");
  }

  /**
   * Write a minimal SequenceFile header naming the given key, value and (when
   * compressed) codec classes; no records follow.
   */
  private Path writeHeader(String name, String keyClass, String valClass,
      boolean compressed, String codecClass) throws IOException {
    Path path = new Path(new File(testDir, name).toURI());
    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write(new byte[] {'S', 'E', 'Q', 6});  // magic + version
      Text.writeString(out, keyClass);
      Text.writeString(out, valClass);
      out.writeBoolean(compressed);              // compressed?
      out.writeBoolean(false);                   // block compressed?
      if (compressed) {
        Text.writeString(out, codecClass);
      }
      out.writeInt(0);                           // empty metadata
      out.write(new byte[16]);                   // sync marker
    }
    return path;
  }

  @Test
  public void testResolveDoesNotLoad() throws Exception {
    Class<?> clazz = WritableName.getClass(SENTINEL, conf);
    assertThat(clazz.getName()).isEqualTo(SENTINEL);
    assertSentinelNotLoaded();
  }

  @Test
  public void testResolveUnknownClassThrows() throws Exception {
    intercept(IOException.class,
        () -> WritableName.getClass("no.such.Class", conf));
  }

  @Test
  public void testReaderRejectsUnacceptedKeyClass() throws Exception {
    Path path = writeHeader("badkey.seq", SENTINEL, Text.class.getName(),
        false, null);
    intercept(IOException.class,
        () -> new SequenceFile.Reader(fs, path, conf).close());
    assertSentinelNotLoaded();
  }

  @Test
  public void testReaderRejectsUnacceptedValueClass() throws Exception {
    Path path = writeHeader("badvalue.seq", Text.class.getName(), SENTINEL,
        false, null);
    intercept(IOException.class,
        () -> new SequenceFile.Reader(fs, path, conf).close());
    assertSentinelNotLoaded();
  }

  @Test
  public void testReaderRejectsNonCodec() throws Exception {
    Path path = writeHeader("badcodec.seq", Text.class.getName(),
        Text.class.getName(), true, SENTINEL);
    assertSentinelNotLoaded();
    intercept(ClassCastException.class,
        () -> new SequenceFile.Reader(fs, path, conf).close());
    assertSentinelNotLoaded();
  }

  @Test
  public void testRoundTrip() throws Exception {
    Path path = new Path(new File(testDir, "roundtrip.seq").toURI());
    try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
        Text.class, IntWritable.class)) {
      writer.append(new Text("k"), new IntWritable(7));
    }
    assertRead(path);
  }

  @Test
  public void testRoundTripCompressed() throws Exception {
    Path path = new Path(new File(testDir, "roundtrip-codec.seq").toURI());
    try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
        Text.class, IntWritable.class, CompressionType.RECORD,
        new DefaultCodec())) {
      writer.append(new Text("k"), new IntWritable(7));
    }
    assertRead(path);
  }

  private void assertRead(Path path) throws IOException {
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
      Text key = new Text();
      IntWritable value = new IntWritable();
      assertThat(reader.next(key, value)).isTrue();
      assertThat(key).isEqualTo(new Text("k"));
      assertThat(value).isEqualTo(new IntWritable(7));
    }
  }


  /**
   * Assert the sentinel was not loaded.
   */
  private static void assertSentinelNotLoaded() {
    assertThat(LoadFlag.isLoaded())
        .describedAs("sentinel must not be loaded")
        .isFalse();
  }


  /**
   * Records whether {@link Sentinel} has been loaded. Kept separate from
   * {@link Sentinel} so that reading the flag does not load the sentinel.
   */
  public static final class LoadFlag {

    private static volatile boolean loaded = false;

    static boolean isLoaded() {
      return loaded;
    }

    static void set() {
      loaded = true;
    }

    static void reset() {
      LOG.info("Reset Sentinel");
      loaded = false;
    }
  }

  /** Neither a Writable nor a CompressionCodec; records that it was loaded. */
  @SuppressWarnings("unused")
  public static final class Sentinel {

    static {
      LOG.info("Initialized Sentinel");
      LoadFlag.set();
    }

    public Sentinel() {
    }
  }
}
