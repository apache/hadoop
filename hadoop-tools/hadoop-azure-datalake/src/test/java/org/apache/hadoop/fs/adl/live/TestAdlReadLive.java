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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;

/**
 * Verify different data segment size read from the file to ensure the
 * integrity and order of the data over
 * BufferManger and BatchByteArrayInputStream implementation.
 */
public class TestAdlReadLive {
  private String expectedData = "1234567890abcdefghijklmnopqrstuvwxyz";

  @Before
  public void setup() throws Exception {
    org.junit.Assume
        .assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }

  private FileSystem getFileSystem() throws IOException, URISyntaxException {
    return AdlStorageConfiguration.createAdlStorageConnector();
  }

  private void setupFile(Path path) throws IOException, URISyntaxException {
    setupFile(path, expectedData);
  }

  private void setupFile(Path path, String data)
      throws IOException, URISyntaxException {
    expectedData = data;
    FileSystem fs = getFileSystem();
    fs.delete(path, true);
    FSDataOutputStream fdis = fs.create(path);
    fdis.writeBytes(expectedData);
    fdis.close();
    fs.listStatus(path.getParent());
    long actualLen = fs.getFileStatus(path).getLen();
    long expectedLen = expectedData.length();
    System.out.println(
        " Length of file : " + fs.getFileStatus(path).getLen() + " " + fs
            .getUri());
    Assert.assertEquals(expectedLen, actualLen);
  }

  @Test
  public void
      testOpenReadMoreThanAvailableBufferCrashFixIndexOutOfBoundsException()
      throws Throwable {
    Path path = new Path("/test1");
    FileSystem fs = getFileSystem();
    setupFile(path);

    if (fs.exists(path)) {
      Assert.assertTrue(fs.delete(path, true));
    }

    FSDataOutputStream outputStream = fs.create(path);
    final byte[] data = new byte[24 * 1024 * 1024];
    Random ran = new Random();
    ran.nextBytes(data);
    outputStream.write(data);

    FSDataInputStream bb = fs.open(path);
    byte[] expected = new byte[4 * 1024 * 1024];
    bb.read();
    bb.readFully(16711581, expected, 33,
        65640); // BugFix : Was causing crash IndexOutOfBoundsException
    bb.seek(16711581);
    bb.readFully(16711576, expected, 33, 65640);
    bb.readFully(16711578, expected, 33, 65640);
    bb.readFully(16711580, expected, 33, 65640);
    bb.readFully(16711576, expected, 0, expected.length);
    bb.seek(0);
    expected = new byte[134144];
    while (bb.read() != -1){
      continue;
    }
    bb.readFully(0, data, 0, data.length);
  }

  @Test
  public void readNullData() throws IOException, URISyntaxException {
    String data = "SPL   \u0001Lorg.apache.hadoop.examples.terasort"
        + ".TeraGen$RangeInputFormat$RangeInputSplit \u008DLK@Lorg.apache"
        + ".hadoop.examples.terasort"
        + ".TeraGen$RangeInputFormat$RangeInputSplit\u008DLK@\u008DLK@";
    Path path = new Path("/test4");
    FileSystem fs = this.getFileSystem();
    setupFile(path, data);
    FSDataInputStream bb = fs.open(path);
    int i = 0;
    String actualData = new String();
    System.out.println("Data Length :" + expectedData.length());
    byte[] arr = new byte[data.length()];
    bb.readFully(0, arr);
    actualData = new String(arr);
    System.out.println(" Data : " + actualData);
    Assert.assertEquals(actualData.length(), expectedData.length());

    arr = new byte[data.length() - 7];
    bb.readFully(7, arr);
    actualData = new String(arr);
    Assert.assertEquals(actualData.length(), expectedData.length() - 7);
    bb.close();
  }

  @Test
  public void readTest() throws IOException, URISyntaxException {
    Path path = new Path("/test4");
    FileSystem fs = this.getFileSystem();
    setupFile(path);
    FSDataInputStream bb = fs.open(path);
    int i = 0;
    String actualData = new String();
    while (true) {
      int c = bb.read();
      if (c < 0) {
        break;
      }
      actualData += (char) c;
    }

    byte[] b = new byte[100];
    System.out.println(bb.read(b, 9, 91));
    System.out.println(bb.read());
    System.out.println(bb.read());
    System.out.println(bb.read());
    System.out.println(bb.read());
    System.out.println(bb.read());
    System.out.println(bb.read());

    bb.close();
    Assert.assertEquals(actualData, expectedData);

    for (int j = 0; j < 100; ++j) {
      fs = this.getFileSystem();
      fs.exists(new Path("/test" + j));
    }
  }

  @Test
  public void readByteTest() throws IOException, URISyntaxException {
    Path path = new Path("/test3");
    FileSystem fs = this.getFileSystem();
    setupFile(path);
    FSDataInputStream bb = fs.open(path);
    int i = 0;
    byte[] data = new byte[expectedData.length()];
    int readByte = bb.read(data);
    bb.close();
    Assert.assertEquals(readByte, expectedData.length());
    Assert.assertEquals(new String(data), expectedData);
  }

  @Test
  public void readByteFullyTest() throws IOException, URISyntaxException {
    Path path = new Path("/test2");
    FileSystem fs = this.getFileSystem();
    setupFile(path);
    FSDataInputStream bb = fs.open(path);
    int i = 0;
    byte[] data = new byte[expectedData.length()];
    bb.readFully(data);
    bb.close();
    Assert.assertEquals(new String(data), expectedData);

    bb = fs.open(path);
    bb.readFully(data, 0, data.length);
    bb.close();
    Assert.assertEquals(new String(data), expectedData);
  }

  @Test
  public void readCombinationTest() throws IOException, URISyntaxException {
    Path path = new Path("/test1");
    FileSystem fs = this.getFileSystem();
    setupFile(path);
    FSDataInputStream bb = fs.open(path);
    int i = 0;
    byte[] data = new byte[5];
    int readByte = bb.read(data);
    Assert.assertEquals(new String(data), expectedData.substring(0, 5));

    bb.readFully(data, 0, data.length);
    Assert.assertEquals(new String(data), expectedData.substring(5, 10));
    bb.close();
    bb = fs.open(path);
    bb.readFully(5, data, 0, data.length);
    Assert.assertEquals(new String(data), expectedData.substring(5, 10));

    bb.read(data);
    Assert.assertEquals(new String(data), expectedData.substring(0, 5));
    bb.close();
    bb = fs.open(path);
    bb.read(new byte[100]);
    bb.close();
  }

  @Test
  public void readMultiSeekTest() throws IOException, URISyntaxException {
    final Path path = new Path(
        "/delete14/" + UUID.randomUUID().toString().replaceAll("-", ""));
    FileSystem fs = this.getFileSystem();

    final byte[] actualData = new byte[3267397];
    Random ran = new Random();
    ran.nextBytes(actualData);
    byte[] testData = null;

    fs.delete(path, true);
    FSDataOutputStream os = fs.create(path);
    os.write(actualData);
    os.close();

    FSDataInputStream bb = fs.open(path);
    byte[] data = new byte[16384];
    bb.readFully(3251013, data, 0, 16384);
    testData = new byte[16384];
    System.arraycopy(actualData, 3251013, testData, 0, 16384);
    Assert.assertArrayEquals(testData, data);

    data = new byte[1921];
    bb.readFully(3265476, data, 0, 1921);
    testData = new byte[1921];
    System.arraycopy(actualData, 3265476, testData, 0, 1921);
    Assert.assertArrayEquals(testData, data);

    data = new byte[3267394];
    bb.readFully(3, data, 0, 3267394);
    testData = new byte[3267394];
    System.arraycopy(actualData, 3, testData, 0, 3267394);
    Assert.assertArrayEquals(testData, data);

    data = new byte[3266943];
    bb.readFully(454, data, 0, 3266943);
    testData = new byte[3266943];
    System.arraycopy(actualData, 454, testData, 0, 3266943);
    Assert.assertArrayEquals(testData, data);

    data = new byte[3265320];
    bb.readFully(2077, data, 0, 3265320);
    testData = new byte[3265320];
    System.arraycopy(actualData, 2077, testData, 0, 3265320);
    Assert.assertArrayEquals(testData, data);

    bb.close();

    bb = fs.open(path);

    data = new byte[3263262];
    bb.readFully(4135, data, 0, 3263262);
    testData = new byte[3263262];
    System.arraycopy(actualData, 4135, testData, 0, 3263262);
    Assert.assertArrayEquals(testData, data);

    data = new byte[2992591];
    bb.readFully(274806, data, 0, 2992591);
    testData = new byte[2992591];
    System.arraycopy(actualData, 274806, testData, 0, 2992591);
    Assert.assertArrayEquals(testData, data);

    data = new byte[1985665];
    bb.readFully(1281732, data, 0, 1985665);
    testData = new byte[1985665];
    System.arraycopy(actualData, 1281732, testData, 0, 1985665);
    Assert.assertArrayEquals(testData, data);

    data = new byte[3267394];
    try {
      bb.readFully(2420207, data, 0, 3267394);
      Assert.fail("EOF expected");
    } catch (IOException e) {
    }

    bb.close();
  }

  @Test
  public void allASCIICharTest() throws IOException, URISyntaxException {
    final Path path = new Path(
        "/delete14/" + UUID.randomUUID().toString().replaceAll("-", ""));
    FileSystem fs = this.getFileSystem();
    final byte[] actualData = new byte[127];
    for (byte i = 0; i < 127; ++i) {
      actualData[i] = i;
    }

    fs.delete(path, true);
    FSDataOutputStream os = fs.create(path);
    os.write(actualData);
    os.close();

    FSDataInputStream bb = fs.open(path);
    byte[] data = new byte[127];

    bb.readFully(0, data, 0, data.length);
    bb.close();
    Assert.assertArrayEquals(data, actualData);

    bb = fs.open(path);
    int byteRead = 1;
    while (bb.read() != -1) {
      byteRead++;
    }

    bb.seek(0);
    byteRead = 1;
    while (bb.read() != -1) {
      byteRead++;
    }
    bb.close();
  }
}
