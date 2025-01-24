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

package org.apache.hadoop.fs.shell;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.test.GenericTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This class tests the logic for displaying the binary formats supported
 * by the Text command.
 */
public class TestTextCommand {
  private static final File TEST_ROOT_DIR =
      GenericTestUtils.getTestDir("testText");
  private static final String AVRO_FILENAME =
      new File(TEST_ROOT_DIR, "weather.avro").toURI().getPath();
  private static final String TEXT_FILENAME =
      new File(TEST_ROOT_DIR, "testtextfile.txt").toURI().getPath();
  private static final String SEQUENCE_FILENAME =
      new File(TEST_ROOT_DIR, "NonWritableSequenceFile").toURI().getPath();

  private static final String SEPARATOR = System.getProperty("line.separator");

  private static final String AVRO_EXPECTED_OUTPUT =
      "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}" + SEPARATOR
      + "{\"station\":\"011990-99999\",\"time\":-619506000000,\"temp\":22}" + SEPARATOR
      + "{\"station\":\"011990-99999\",\"time\":-619484400000,\"temp\":-11}" + SEPARATOR
      + "{\"station\":\"012650-99999\",\"time\":-655531200000,\"temp\":111}" + SEPARATOR
      + "{\"station\":\"012650-99999\",\"time\":-655509600000,\"temp\":78}" + SEPARATOR;

  private static final String SEQUENCE_FILE_EXPECTED_OUTPUT =
      "Key1\tValue1" + SEPARATOR + "Key2\tValue2" + SEPARATOR;

  @Rule
  public final Timeout testTimeout = new Timeout(30000);

  /**
   * Tests whether binary Avro data files are displayed correctly.
   */
  @Test
  public void testDisplayForAvroFiles() throws Exception {
    String output = readUsingTextCommand(AVRO_FILENAME,
                                         generateWeatherAvroBinaryData());
    Assertions.assertThat(output).describedAs("output").isEqualTo(AVRO_EXPECTED_OUTPUT);
  }

  @Test
  public void testDisplayForAvroFilesSmallMultiByteReads() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(IO_FILE_BUFFER_SIZE_KEY, 2);
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    String output = readUsingTextCommand(uri, conf);
    Assertions.assertThat(output).describedAs("output").isEqualTo(AVRO_EXPECTED_OUTPUT);
  }

  @Test
  public void testEmptyAvroFile() throws Exception {
    String output = readUsingTextCommand(AVRO_FILENAME,
                                         generateEmptyAvroBinaryData());
    Assertions.assertThat(output).describedAs("output").isEmpty();
  }

  @Test(expected = NullPointerException.class)
  public void testAvroFileInputStreamNullBuffer() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(null, 0, 10);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAvroFileInputStreamNegativePosition() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(new byte[10], -1, 10);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAvroFileInputStreamTooLong() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(new byte[10], 0, 11);
    }
  }

  @Test
  public void testAvroFileInputStreamZeroLengthRead() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is = getInputStream(uri, conf)) {
      Assertions.assertThat(is.read(new byte[10], 0, 0)).describedAs("bytes read").isEqualTo(0);
    }
  }

  @Test
  public void testAvroFileInputStreamConsistentEOF() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is = getInputStream(uri, conf)) {
      inputStreamToString(is);
      Assertions.assertThat(is.read()).describedAs("single byte EOF").isEqualTo(-1);
      Assertions.assertThat(is.read(new byte[10], 0, 10)).describedAs("multi byte EOF")
              .isEqualTo(-1);
    }
  }

  @Test
  public void testAvroFileInputStreamSingleAndMultiByteReads() throws Exception {
    createFile(AVRO_FILENAME, generateWeatherAvroBinaryData());
    URI uri = new URI(AVRO_FILENAME);
    Configuration conf = new Configuration();
    try (InputStream is1 = getInputStream(uri, conf);
        InputStream is2 = getInputStream(uri, conf)) {
      String multiByteReads = inputStreamToString(is1);
      String singleByteReads = inputStreamSingleByteReadsToString(is2);
      Assertions.assertThat(multiByteReads)
          .describedAs("same bytes read from multi and single byte reads")
          .isEqualTo(singleByteReads);
    }
  }

  /**
   * Tests that a zero-length file is displayed correctly.
   */
  @Test
  public void testEmptyTextFile() throws Exception {
    byte[] emptyContents = {};
    String output = readUsingTextCommand(TEXT_FILENAME, emptyContents);
    Assertions.assertThat(output).describedAs("output").isEmpty();
  }

  /**
   * Tests that a one-byte file is displayed correctly.
   */
  @Test
  public void testOneByteTextFile() throws Exception {
    byte[] oneByteContents = {'x'};
    String output = readUsingTextCommand(TEXT_FILENAME, oneByteContents);
    String expected = new String(oneByteContents, StandardCharsets.UTF_8);
    Assertions.assertThat(output).describedAs("output").isEqualTo(expected);
  }

  /**
   * Tests that a two-byte file is displayed correctly.
   */
  @Test
  public void testTwoByteTextFile() throws Exception {
    byte[] twoByteContents = {'x', 'y'};
    String output = readUsingTextCommand(TEXT_FILENAME, twoByteContents);
    String expected = new String(twoByteContents, StandardCharsets.UTF_8);
    Assertions.assertThat(output).describedAs("output").isEqualTo(expected);
  }

  @Test
  public void testDisplayForNonWritableSequenceFile() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    String output = readUsingTextCommand(uri, conf);
    Assertions.assertThat(output).describedAs("output").isEqualTo(SEQUENCE_FILE_EXPECTED_OUTPUT);
  }

  @Test
  public void testDisplayForSequenceFileSmallMultiByteReads() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(IO_FILE_BUFFER_SIZE_KEY, 2);
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    String output = readUsingTextCommand(uri, conf);
    Assertions.assertThat(output).describedAs("output").isEqualTo(SEQUENCE_FILE_EXPECTED_OUTPUT);
  }

  @Test
  public void testEmptySequenceFile() throws Exception {
    Configuration conf = new Configuration();
    createEmptySequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    String output = readUsingTextCommand(uri, conf);
    Assertions.assertThat(output).describedAs("output").isEmpty();
  }

  @Test(expected = NullPointerException.class)
  public void testSequenceFileInputStreamNullBuffer() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(null, 0, 10);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSequenceFileInputStreamNegativePosition() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(new byte[10], -1, 10);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSequenceFileInputStreamTooLong() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is = getInputStream(uri, conf)) {
      is.read(new byte[10], 0, 11);
    }
  }

  @Test
  public void testSequenceFileInputStreamZeroLengthRead() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is = getInputStream(uri, conf)) {
      Assertions.assertThat(is.read(new byte[10], 0, 0)).describedAs("bytes read").isEqualTo(0);
    }
  }

  @Test
  public void testSequenceFileInputStreamConsistentEOF() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is = getInputStream(uri, conf)) {
      inputStreamToString(is);
      Assertions.assertThat(is.read()).describedAs("single byte EOF").isEqualTo(-1);
      Assertions.assertThat(is.read(new byte[10], 0, 10)).describedAs("multi byte EOF")
              .isEqualTo(-1);
    }
  }

  @Test
  public void testSequenceFileInputStreamSingleAndMultiByteReads() throws Exception {
    Configuration conf = new Configuration();
    createNonWritableSequenceFile(SEQUENCE_FILENAME, conf);
    URI uri = new URI(SEQUENCE_FILENAME);
    try (InputStream is1 = getInputStream(uri, conf);
        InputStream is2 = getInputStream(uri, conf)) {
      String multiByteReads = inputStreamToString(is1);
      String singleByteReads = inputStreamSingleByteReadsToString(is2);
      Assertions.assertThat(multiByteReads)
          .describedAs("same bytes read from multi and single byte reads")
          .isEqualTo(singleByteReads);
    }
  }

  // Create a file on the local file system and read it using
  // the Display.Text class.
  private static String readUsingTextCommand(String fileName, byte[] fileContents)
          throws Exception {
    createFile(fileName, fileContents);

    Configuration conf = new Configuration();
    URI localPath = new URI(fileName);
    return readUsingTextCommand(localPath, conf);
  }
  // Read a file using Display.Text class.
  private static String readUsingTextCommand(URI uri, Configuration conf)
      throws Exception {
    InputStream stream = getInputStream(uri, conf);
    return inputStreamToString(stream);
  }

  private static String inputStreamToString(InputStream stream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer, StandardCharsets.UTF_8);
    return writer.toString();
  }

  private static String inputStreamSingleByteReadsToString(InputStream stream) throws IOException {
    StringWriter writer = new StringWriter();
    for (int b = stream.read(); b != -1; b = stream.read()) {
      writer.write(b);
    }
    return writer.toString();
  }

  private static void createFile(String fileName, byte[] contents) throws IOException {
    Files.createDirectories(TEST_ROOT_DIR.toPath());
    File file = new File(fileName);
    file.createNewFile();
    FileOutputStream stream = new FileOutputStream(file);
    stream.write(contents);
    stream.close();
  }

  private static byte[] generateWeatherAvroBinaryData() {
    // The contents of a simple binary Avro file with weather records.
    byte[] contents = {
        (byte) 0x4f, (byte) 0x62, (byte) 0x6a, (byte)  0x1,
        (byte)  0x4, (byte) 0x14, (byte) 0x61, (byte) 0x76,
        (byte) 0x72, (byte) 0x6f, (byte) 0x2e, (byte) 0x63,
        (byte) 0x6f, (byte) 0x64, (byte) 0x65, (byte) 0x63,
        (byte)  0x8, (byte) 0x6e, (byte) 0x75, (byte) 0x6c,
        (byte) 0x6c, (byte) 0x16, (byte) 0x61, (byte) 0x76,
        (byte) 0x72, (byte) 0x6f, (byte) 0x2e, (byte) 0x73,
        (byte) 0x63, (byte) 0x68, (byte) 0x65, (byte) 0x6d,
        (byte) 0x61, (byte) 0xf2, (byte)  0x2, (byte) 0x7b,
        (byte) 0x22, (byte) 0x74, (byte) 0x79, (byte) 0x70,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x72, (byte) 0x65, (byte) 0x63, (byte) 0x6f,
        (byte) 0x72, (byte) 0x64, (byte) 0x22, (byte) 0x2c,
        (byte) 0x22, (byte) 0x6e, (byte) 0x61, (byte) 0x6d,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x57, (byte) 0x65, (byte) 0x61, (byte) 0x74,
        (byte) 0x68, (byte) 0x65, (byte) 0x72, (byte) 0x22,
        (byte) 0x2c, (byte) 0x22, (byte) 0x6e, (byte) 0x61,
        (byte) 0x6d, (byte) 0x65, (byte) 0x73, (byte) 0x70,
        (byte) 0x61, (byte) 0x63, (byte) 0x65, (byte) 0x22,
        (byte) 0x3a, (byte) 0x22, (byte) 0x74, (byte) 0x65,
        (byte) 0x73, (byte) 0x74, (byte) 0x22, (byte) 0x2c,
        (byte) 0x22, (byte) 0x66, (byte) 0x69, (byte) 0x65,
        (byte) 0x6c, (byte) 0x64, (byte) 0x73, (byte) 0x22,
        (byte) 0x3a, (byte) 0x5b, (byte) 0x7b, (byte) 0x22,
        (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65,
        (byte) 0x22, (byte) 0x3a, (byte) 0x22, (byte) 0x73,
        (byte) 0x74, (byte) 0x61, (byte) 0x74, (byte) 0x69,
        (byte) 0x6f, (byte) 0x6e, (byte) 0x22, (byte) 0x2c,
        (byte) 0x22, (byte) 0x74, (byte) 0x79, (byte) 0x70,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x73, (byte) 0x74, (byte) 0x72, (byte) 0x69,
        (byte) 0x6e, (byte) 0x67, (byte) 0x22, (byte) 0x7d,
        (byte) 0x2c, (byte) 0x7b, (byte) 0x22, (byte) 0x6e,
        (byte) 0x61, (byte) 0x6d, (byte) 0x65, (byte) 0x22,
        (byte) 0x3a, (byte) 0x22, (byte) 0x74, (byte) 0x69,
        (byte) 0x6d, (byte) 0x65, (byte) 0x22, (byte) 0x2c,
        (byte) 0x22, (byte) 0x74, (byte) 0x79, (byte) 0x70,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x6c, (byte) 0x6f, (byte) 0x6e, (byte) 0x67,
        (byte) 0x22, (byte) 0x7d, (byte) 0x2c, (byte) 0x7b,
        (byte) 0x22, (byte) 0x6e, (byte) 0x61, (byte) 0x6d,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x74, (byte) 0x65, (byte) 0x6d, (byte) 0x70,
        (byte) 0x22, (byte) 0x2c, (byte) 0x22, (byte) 0x74,
        (byte) 0x79, (byte) 0x70, (byte) 0x65, (byte) 0x22,
        (byte) 0x3a, (byte) 0x22, (byte) 0x69, (byte) 0x6e,
        (byte) 0x74, (byte) 0x22, (byte) 0x7d, (byte) 0x5d,
        (byte) 0x2c, (byte) 0x22, (byte) 0x64, (byte) 0x6f,
        (byte) 0x63, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x41, (byte) 0x20, (byte) 0x77, (byte) 0x65,
        (byte) 0x61, (byte) 0x74, (byte) 0x68, (byte) 0x65,
        (byte) 0x72, (byte) 0x20, (byte) 0x72, (byte) 0x65,
        (byte) 0x61, (byte) 0x64, (byte) 0x69, (byte) 0x6e,
        (byte) 0x67, (byte) 0x2e, (byte) 0x22, (byte) 0x7d,
        (byte)  0x0, (byte) 0xb0, (byte) 0x81, (byte) 0xb3,
        (byte) 0xc4, (byte)  0xa, (byte)  0xc, (byte) 0xf6,
        (byte) 0x62, (byte) 0xfa, (byte) 0xc9, (byte) 0x38,
        (byte) 0xfd, (byte) 0x7e, (byte) 0x52, (byte)  0x0,
        (byte) 0xa7, (byte)  0xa, (byte) 0xcc, (byte)  0x1,
        (byte) 0x18, (byte) 0x30, (byte) 0x31, (byte) 0x31,
        (byte) 0x39, (byte) 0x39, (byte) 0x30, (byte) 0x2d,
        (byte) 0x39, (byte) 0x39, (byte) 0x39, (byte) 0x39,
        (byte) 0x39, (byte) 0xff, (byte) 0xa3, (byte) 0x90,
        (byte) 0xe8, (byte) 0x87, (byte) 0x24, (byte)  0x0,
        (byte) 0x18, (byte) 0x30, (byte) 0x31, (byte) 0x31,
        (byte) 0x39, (byte) 0x39, (byte) 0x30, (byte) 0x2d,
        (byte) 0x39, (byte) 0x39, (byte) 0x39, (byte) 0x39,
        (byte) 0x39, (byte) 0xff, (byte) 0x81, (byte) 0xfb,
        (byte) 0xd6, (byte) 0x87, (byte) 0x24, (byte) 0x2c,
        (byte) 0x18, (byte) 0x30, (byte) 0x31, (byte) 0x31,
        (byte) 0x39, (byte) 0x39, (byte) 0x30, (byte) 0x2d,
        (byte) 0x39, (byte) 0x39, (byte) 0x39, (byte) 0x39,
        (byte) 0x39, (byte) 0xff, (byte) 0xa5, (byte) 0xae,
        (byte) 0xc2, (byte) 0x87, (byte) 0x24, (byte) 0x15,
        (byte) 0x18, (byte) 0x30, (byte) 0x31, (byte) 0x32,
        (byte) 0x36, (byte) 0x35, (byte) 0x30, (byte) 0x2d,
        (byte) 0x39, (byte) 0x39, (byte) 0x39, (byte) 0x39,
        (byte) 0x39, (byte) 0xff, (byte) 0xb7, (byte) 0xa2,
        (byte) 0x8b, (byte) 0x94, (byte) 0x26, (byte) 0xde,
        (byte)  0x1, (byte) 0x18, (byte) 0x30, (byte) 0x31,
        (byte) 0x32, (byte) 0x36, (byte) 0x35, (byte) 0x30,
        (byte) 0x2d, (byte) 0x39, (byte) 0x39, (byte) 0x39,
        (byte) 0x39, (byte) 0x39, (byte) 0xff, (byte) 0xdb,
        (byte) 0xd5, (byte) 0xf6, (byte) 0x93, (byte) 0x26,
        (byte) 0x9c, (byte)  0x1, (byte) 0xb0, (byte) 0x81,
        (byte) 0xb3, (byte) 0xc4, (byte)  0xa, (byte)  0xc,
        (byte) 0xf6, (byte) 0x62, (byte) 0xfa, (byte) 0xc9,
        (byte) 0x38, (byte) 0xfd, (byte) 0x7e, (byte) 0x52,
        (byte)  0x0, (byte) 0xa7,
    };

    return contents;
  }

  private static byte[] generateEmptyAvroBinaryData() {
    // The binary contents of an empty Avro file (no records).
    byte[] contents = new byte[] {
        (byte) 0x4f, (byte) 0x62, (byte) 0x6a, (byte) 0x01,
        (byte) 0x04, (byte) 0x16, (byte) 0x61, (byte) 0x76,
        (byte) 0x72, (byte) 0x6f, (byte) 0x2e, (byte) 0x73,
        (byte) 0x63, (byte) 0x68, (byte) 0x65, (byte) 0x6d,
        (byte) 0x61, (byte) 0x92, (byte) 0x03, (byte) 0x7b,
        (byte) 0x22, (byte) 0x74, (byte) 0x79, (byte) 0x70,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x72, (byte) 0x65, (byte) 0x63, (byte) 0x6f,
        (byte) 0x72, (byte) 0x64, (byte) 0x22, (byte) 0x2c,
        (byte) 0x22, (byte) 0x6e, (byte) 0x61, (byte) 0x6d,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x55, (byte) 0x73, (byte) 0x65, (byte) 0x72,
        (byte) 0x22, (byte) 0x2c, (byte) 0x22, (byte) 0x6e,
        (byte) 0x61, (byte) 0x6d, (byte) 0x65, (byte) 0x73,
        (byte) 0x70, (byte) 0x61, (byte) 0x63, (byte) 0x65,
        (byte) 0x22, (byte) 0x3a, (byte) 0x22, (byte) 0x65,
        (byte) 0x78, (byte) 0x61, (byte) 0x6d, (byte) 0x70,
        (byte) 0x6c, (byte) 0x65, (byte) 0x2e, (byte) 0x61,
        (byte) 0x76, (byte) 0x72, (byte) 0x6f, (byte) 0x22,
        (byte) 0x2c, (byte) 0x22, (byte) 0x66, (byte) 0x69,
        (byte) 0x65, (byte) 0x6c, (byte) 0x64, (byte) 0x73,
        (byte) 0x22, (byte) 0x3a, (byte) 0x5b, (byte) 0x7b,
        (byte) 0x22, (byte) 0x6e, (byte) 0x61, (byte) 0x6d,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x6e, (byte) 0x61, (byte) 0x6d, (byte) 0x65,
        (byte) 0x22, (byte) 0x2c, (byte) 0x22, (byte) 0x74,
        (byte) 0x79, (byte) 0x70, (byte) 0x65, (byte) 0x22,
        (byte) 0x3a, (byte) 0x22, (byte) 0x73, (byte) 0x74,
        (byte) 0x72, (byte) 0x69, (byte) 0x6e, (byte) 0x67,
        (byte) 0x22, (byte) 0x7d, (byte) 0x2c, (byte) 0x7b,
        (byte) 0x22, (byte) 0x6e, (byte) 0x61, (byte) 0x6d,
        (byte) 0x65, (byte) 0x22, (byte) 0x3a, (byte) 0x22,
        (byte) 0x66, (byte) 0x61, (byte) 0x76, (byte) 0x6f,
        (byte) 0x72, (byte) 0x69, (byte) 0x74, (byte) 0x65,
        (byte) 0x5f, (byte) 0x6e, (byte) 0x75, (byte) 0x6d,
        (byte) 0x62, (byte) 0x65, (byte) 0x72, (byte) 0x22,
        (byte) 0x2c, (byte) 0x22, (byte) 0x74, (byte) 0x79,
        (byte) 0x70, (byte) 0x65, (byte) 0x22, (byte) 0x3a,
        (byte) 0x5b, (byte) 0x22, (byte) 0x69, (byte) 0x6e,
        (byte) 0x74, (byte) 0x22, (byte) 0x2c, (byte) 0x22,
        (byte) 0x6e, (byte) 0x75, (byte) 0x6c, (byte) 0x6c,
        (byte) 0x22, (byte) 0x5d, (byte) 0x7d, (byte) 0x2c,
        (byte) 0x7b, (byte) 0x22, (byte) 0x6e, (byte) 0x61,
        (byte) 0x6d, (byte) 0x65, (byte) 0x22, (byte) 0x3a,
        (byte) 0x22, (byte) 0x66, (byte) 0x61, (byte) 0x76,
        (byte) 0x6f, (byte) 0x72, (byte) 0x69, (byte) 0x74,
        (byte) 0x65, (byte) 0x5f, (byte) 0x63, (byte) 0x6f,
        (byte) 0x6c, (byte) 0x6f, (byte) 0x72, (byte) 0x22,
        (byte) 0x2c, (byte) 0x22, (byte) 0x74, (byte) 0x79,
        (byte) 0x70, (byte) 0x65, (byte) 0x22, (byte) 0x3a,
        (byte) 0x5b, (byte) 0x22, (byte) 0x73, (byte) 0x74,
        (byte) 0x72, (byte) 0x69, (byte) 0x6e, (byte) 0x67,
        (byte) 0x22, (byte) 0x2c, (byte) 0x22, (byte) 0x6e,
        (byte) 0x75, (byte) 0x6c, (byte) 0x6c, (byte) 0x22,
        (byte) 0x5d, (byte) 0x7d, (byte) 0x5d, (byte) 0x7d,
        (byte) 0x14, (byte) 0x61, (byte) 0x76, (byte) 0x72,
        (byte) 0x6f, (byte) 0x2e, (byte) 0x63, (byte) 0x6f,
        (byte) 0x64, (byte) 0x65, (byte) 0x63, (byte) 0x0e,
        (byte) 0x64, (byte) 0x65, (byte) 0x66, (byte) 0x6c,
        (byte) 0x61, (byte) 0x74, (byte) 0x65, (byte) 0x00,
        (byte) 0xed, (byte) 0xe0, (byte) 0xfa, (byte) 0x87,
        (byte) 0x3c, (byte) 0x86, (byte) 0xf5, (byte) 0x5f,
        (byte) 0x7d, (byte) 0x8d, (byte) 0x2f, (byte) 0xdb,
        (byte) 0xc7, (byte) 0xe3, (byte) 0x11, (byte) 0x39,
    };

    return contents;
  }

  private static void createEmptySequenceFile(String fileName, Configuration conf)
      throws IOException {
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    Path path = new Path(fileName);
    SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
        SequenceFile.Writer.keyClass(String.class), SequenceFile.Writer.valueClass(String.class));
    writer.close();
  }

  private static void createNonWritableSequenceFile(String fileName, Configuration conf)
      throws IOException {
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");
    Path path = new Path(fileName);
    try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(String.class),
        SequenceFile.Writer.valueClass(String.class))) {
      writer.append("Key1", "Value1");
      writer.append("Key2", "Value2");
    }
  }

  private static InputStream getInputStream(URI uri, Configuration conf) throws IOException {
    // Prepare and call the Text command's protected getInputStream method.
    PathData pathData = new PathData(uri, conf);
    Display.Text text = new Display.Text() {
      @Override
      public InputStream getInputStream(PathData item) throws IOException {
        return super.getInputStream(item);
      }
    };
    text.setConf(conf);
    return text.getInputStream(pathData);
  }
}
