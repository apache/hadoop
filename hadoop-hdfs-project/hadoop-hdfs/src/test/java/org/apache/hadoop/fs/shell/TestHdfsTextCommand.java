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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This class tests the logic for displaying the binary formats supported
 * by the Text command.
 */
public class TestHdfsTextCommand {
  private static final String TEST_ROOT_DIR = "/build/test/data/testText";
  private static final Path AVRO_FILENAME = new Path(TEST_ROOT_DIR, "weather.avro");
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @Before
    public void setUp() throws IOException{
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
    public void tearDown() throws IOException{
    if(fs != null){
      fs.close();
    }
    if(cluster != null){
      cluster.shutdown();
    }
  }

  /**
   * Tests whether binary Avro data files are displayed correctly.
   */
  @Test
    public void testDisplayForAvroFiles() throws Exception {
    // Create a small Avro data file on the HDFS.
    createAvroFile(generateWeatherAvroBinaryData());

    // Prepare and call the Text command's protected getInputStream method
    // using reflection.
    Configuration conf = fs.getConf();
    PathData pathData = new PathData(AVRO_FILENAME.toString(), conf);
    Display.Text text = new Display.Text();
    text.setConf(conf);
    Method method = text.getClass().getDeclaredMethod(
                                                      "getInputStream", PathData.class);
    method.setAccessible(true);
    InputStream stream = (InputStream) method.invoke(text, pathData);
    String output = inputStreamToString(stream);

    // Check the output.
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

    assertEquals(expectedOutput, output);
  }

  private String inputStreamToString(InputStream stream) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer);
    return writer.toString();
  }

  private void createAvroFile(byte[] contents) throws IOException {
    FSDataOutputStream stream = fs.create(AVRO_FILENAME);
    stream.write(contents);
    stream.close();
    assertTrue(fs.exists(AVRO_FILENAME));
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
