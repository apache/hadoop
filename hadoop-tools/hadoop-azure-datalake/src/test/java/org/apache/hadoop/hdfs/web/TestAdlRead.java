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

package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.TestADLResponseData;
import org.apache.hadoop.fs.common.AdlMockWebServer;
import org.apache.hadoop.fs.common.TestDataForRead;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * This class is responsible for stress positional reads vs number of network
 * calls required by to fetch the amount of data. Test does ensure the data
 * integrity and order of the data is maintained. This tests are meant to test
 * BufferManager.java and BatchByteArrayInputStream implementation.
 */
@RunWith(Parameterized.class)
public class TestAdlRead extends AdlMockWebServer {

  // Keeping timeout of 1 hour to ensure the test does complete and should
  // not terminate due to high backend latency.
  @Rule
  public Timeout globalTimeout = new Timeout(60 * 60000);
  private TestDataForRead testData;

  public TestAdlRead(TestDataForRead testData) {
    this.testData = testData;
    getConf().set("adl.feature.override.readahead.max.buffersize", "8192");
    getConf().set("adl.feature.override.readahead.max.concurrent.connection",
        "1");
  }

  @Parameterized.Parameters(name = "{index}")
  public static Collection testDataForReadOperation() {
    return Arrays.asList(new Object[][] {

        //--------------------------
        // Test Data
        //--------------------------
        {new TestDataForRead("Hello World".getBytes(), 3, 1000, true)},
        {new TestDataForRead(
            ("the problem you appear to be wrestling with is that this doesn't "
                + "display very well. ").getBytes(), 3, 1000, true)},
        {new TestDataForRead(
            ("Chinese Indonesians (Indonesian: Orang Tionghoa-Indonesia; "
                + "Chinese: "
                + "trad ???????, simp ???????, pin Y�nd�n�x?y� Hu�r�n), are "
                + "Indonesians descended from various Chinese ethnic groups, "
                + "particularly Han.").getBytes(), 3, 1000, true)},
        {new TestDataForRead(
            TestADLResponseData.getRandomByteArrayData(5 * 1024), 3, 1000,
            true)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(1 * 1024), 3, 50, true)},
        {new TestDataForRead(
            TestADLResponseData.getRandomByteArrayData(8 * 1024), 3, 10, true)},
        {new TestDataForRead(
            TestADLResponseData.getRandomByteArrayData(32 * 1024), 6, 10,
            false)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(48 * 1024), 8, 10, false)}});
  }

  @After
  @Before
  public void cleanReadBuffer() {
    BufferManager.getInstance().clear();
  }

  @Test
  public void testEntireBytes() throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    byte[] expectedData = new byte[testData.getActualData().length];
    Assert.assertEquals(in.read(expectedData), expectedData.length);
    Assert.assertArrayEquals(expectedData, testData.getActualData());
    in.close();
    if (testData.isCheckOfNoOfCalls()) {
      Assert.assertEquals(testData.getExpectedNoNetworkCall(),
          getMockServer().getRequestCount());
    }
  }

  @Test
  public void testSeekOperation() throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    Random random = new Random();
    for (int i = 0; i < 1000; ++i) {
      int position = random.nextInt(testData.getActualData().length);
      in.seek(position);
      Assert.assertEquals(in.getPos(), position);
      Assert.assertEquals(in.read(), testData.getActualData()[position] & 0xFF);
    }
    in.close();
    if (testData.isCheckOfNoOfCalls()) {
      Assert.assertEquals(testData.getExpectedNoNetworkCall(),
          getMockServer().getRequestCount());
    }
  }

  @Test
  public void testReadServerCalls() throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    byte[] expectedData = new byte[testData.getActualData().length];
    in.readFully(expectedData);
    Assert.assertArrayEquals(expectedData, testData.getActualData());
    Assert.assertEquals(testData.getExpectedNoNetworkCall(),
        getMockServer().getRequestCount());
    in.close();
  }

  @Test
  public void testReadFully() throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    byte[] expectedData = new byte[testData.getActualData().length];
    in.readFully(expectedData);
    Assert.assertArrayEquals(expectedData, testData.getActualData());

    in.readFully(0, expectedData);
    Assert.assertArrayEquals(expectedData, testData.getActualData());

    in.seek(0);
    in.readFully(expectedData, 0, expectedData.length);
    Assert.assertArrayEquals(expectedData, testData.getActualData());
    in.close();

    if (testData.isCheckOfNoOfCalls()) {
      Assert.assertEquals(testData.getExpectedNoNetworkCall(),
          getMockServer().getRequestCount());
    }
  }

  @Test
  public void testRandomPositionalReadUsingReadFully()
      throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    ByteArrayInputStream actualData = new ByteArrayInputStream(
        testData.getActualData());
    Random random = new Random();
    for (int i = 0; i < testData.getIntensityOfTest(); ++i) {
      int offset = random.nextInt(testData.getActualData().length);
      int length = testData.getActualData().length - offset;
      byte[] expectedData = new byte[length];
      byte[] actualDataSubset = new byte[length];
      actualData.reset();
      actualData.skip(offset);
      actualData.read(actualDataSubset, 0, length);

      in.readFully(offset, expectedData, 0, length);
      Assert.assertArrayEquals(expectedData, actualDataSubset);
    }

    for (int i = 0; i < testData.getIntensityOfTest(); ++i) {
      int offset = random.nextInt(testData.getActualData().length);
      int length = random.nextInt(testData.getActualData().length - offset);
      byte[] expectedData = new byte[length];
      byte[] actualDataSubset = new byte[length];
      actualData.reset();
      actualData.skip(offset);
      actualData.read(actualDataSubset, 0, length);

      in.readFully(offset, expectedData, 0, length);
      Assert.assertArrayEquals(expectedData, actualDataSubset);
    }

    in.close();
    if (testData.isCheckOfNoOfCalls()) {
      Assert.assertEquals(testData.getExpectedNoNetworkCall(),
          getMockServer().getRequestCount());
    }
  }
}
