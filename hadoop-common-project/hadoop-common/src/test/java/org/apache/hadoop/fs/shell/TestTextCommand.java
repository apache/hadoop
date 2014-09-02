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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * This class tests the logic for displaying the binary formats supported
 * by the Text command.
 */
public class TestTextCommand {
  private static final String TEST_ROOT_DIR =
    System.getProperty("test.build.data", "build/test/data/") + "/testText";
  private static final String AVRO_FILENAME =
    new Path(TEST_ROOT_DIR, "weather.avro").toUri().getPath();
  private static final String TEXT_FILENAME =
    new Path(TEST_ROOT_DIR, "testtextfile.txt").toUri().getPath();

  /**
   * Tests whether binary Avro data files are displayed correctly.
   */
  @Test (timeout = 30000)
  public void testDisplayForAvroFiles() throws Exception {
    String expectedOutput =
      "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}" +
      System.getProperty("line.separator") +
      "{\"station\":\"011990-99999\",\"time\":-619506000000,\"temp\":22}" +
      System.getProperty("line.separator") +
      "{\"station\":\"011990-99999\",\"time\":-619484400000,\"temp\":-11}" +
      System.getProperty("line.separator") +
      "{\"station\":\"012650-99999\",\"time\":-655531200000,\"temp\":111}" +
      System.getProperty("line.separator") +
      "{\"station\":\"012650-99999\",\"time\":-655509600000,\"temp\":78}" +
      System.getProperty("line.separator");

    String output = readUsingTextCommand(AVRO_FILENAME,
                                         generateWeatherAvroBinaryData());
    assertEquals(expectedOutput, output);
  }

  /**
   * Tests that a zero-length file is displayed correctly.
   */
  @Test (timeout = 30000)
  public void testEmptyTextFil() throws Exception {
    byte[] emptyContents = { };
    String output = readUsingTextCommand(TEXT_FILENAME, emptyContents);
    assertTrue("".equals(output));
  }

  /**
   * Tests that a one-byte file is displayed correctly.
   */
  @Test (timeout = 30000)
  public void testOneByteTextFil() throws Exception {
    byte[] oneByteContents = { 'x' };
    String output = readUsingTextCommand(TEXT_FILENAME, oneByteContents);
    assertTrue(new String(oneByteContents).equals(output));
  }

  /**
   * Tests that a one-byte file is displayed correctly.
   */
  @Test (timeout = 30000)
  public void testTwoByteTextFil() throws Exception {
    byte[] twoByteContents = { 'x', 'y' };
    String output = readUsingTextCommand(TEXT_FILENAME, twoByteContents);
    assertTrue(new String(twoByteContents).equals(output));
  }

  // Create a file on the local file system and read it using
  // the Display.Text class.
  private String readUsingTextCommand(String fileName, byte[] fileContents)
          throws Exception {
    createFile(fileName, fileContents);

    // Prepare and call the Text command's protected getInputStream method
    // using reflection.
    Configuration conf = new Configuration();
    URI localPath = new URI(fileName);
    PathData pathData = new PathData(localPath, conf);
    Display.Text text = new Display.Text() {
      @Override
      public InputStream getInputStream(PathData item) throws IOException {
        return super.getInputStream(item);
      }
    };
    text.setConf(conf);
    InputStream stream = (InputStream) text.getInputStream(pathData);
    return inputStreamToString(stream);
  }

  private String inputStreamToString(InputStream stream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer);
    return writer.toString();
  }

  private void createFile(String fileName, byte[] contents) throws IOException {
    (new File(TEST_ROOT_DIR)).mkdir();
    File file = new File(fileName);
    file.createNewFile();
    FileOutputStream stream = new FileOutputStream(file);
    stream.write(contents);
    stream.close();
  }

  private byte[] generateWeatherAvroBinaryData() {
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
}

