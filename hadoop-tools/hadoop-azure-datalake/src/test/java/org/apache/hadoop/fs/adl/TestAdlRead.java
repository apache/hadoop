/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.common.Parallelized;
import org.apache.hadoop.fs.adl.common.TestDataForRead;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.hadoop.fs.adl.AdlConfKeys.READ_AHEAD_BUFFER_SIZE_KEY;

/**
 * This class is responsible for stress positional reads vs number of network
 * calls required by to fetch the amount of data. Test does ensure the data
 * integrity and order of the data is maintained.
 */
@RunWith(Parallelized.class)
public class TestAdlRead extends AdlMockWebServer {

  private TestDataForRead testData;

  public TestAdlRead(TestDataForRead testData) {
    Configuration configuration = new Configuration();
    configuration.setInt(READ_AHEAD_BUFFER_SIZE_KEY, 4 * 1024);
    setConf(configuration);
    this.testData = testData;
  }

  @Parameterized.Parameters(name = "{index}")
  public static Collection testDataForReadOperation() {
    return Arrays.asList(new Object[][] {

        //--------------------------
        // Test Data
        //--------------------------
        {new TestDataForRead("Hello World".getBytes(), 2, 1000, true)},
        {new TestDataForRead(
            ("the problem you appear to be wrestling with is that this doesn't "
                + "display very well. ").getBytes(), 2, 1000, true)},
        {new TestDataForRead(("您的數據是寶貴的資產，以您的組織，並有當前和未來價值。由於這個原因，"
            + "所有的數據應存儲以供將來分析。今天，這往往是不這樣做，" + "因為傳統的分析基礎架構的限制，"
            + "像模式的預定義，存儲大數據集和不同的數據筒倉的傳播的成本。"
            + "為了應對這一挑戰，數據湖面概念被引入作為一個企業級存儲庫來存儲所有"
            + "類型的在一個地方收集到的數據。對於運作和探索性分析的目的，所有類型的" + "數據可以定義需求或模式之前被存儲在數據湖。")
            .getBytes(), 2, 1000, true)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(4 * 1024), 2, 10, true)},
        {new TestDataForRead(TestADLResponseData.getRandomByteArrayData(100), 2,
            1000, true)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(1 * 1024), 2, 50, true)},
        {new TestDataForRead(
            TestADLResponseData.getRandomByteArrayData(8 * 1024), 3, 10,
            false)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(16 * 1024), 5, 10, false)},
        {new TestDataForRead(
            TestADLResponseData.getRandomByteArrayData(32 * 1024), 9, 10,
            false)}, {new TestDataForRead(
        TestADLResponseData.getRandomByteArrayData(64 * 1024), 17, 10,
        false)}});
  }

  @Test
  public void testEntireBytes() throws IOException, InterruptedException {
    getMockServer().setDispatcher(testData.getDispatcher());
    FSDataInputStream in = getMockAdlFileSystem().open(new Path("/test"));
    byte[] expectedData = new byte[testData.getActualData().length];
    int n = 0;
    int len = expectedData.length;
    int off = 0;
    while (n < len) {
      int count = in.read(expectedData, off + n, len - n);
      if (count < 0) {
        throw new EOFException();
      }
      n += count;
    }

    Assert.assertEquals(testData.getActualData().length, expectedData.length);
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

    in.readFully(0, expectedData, 0, expectedData.length);
    Assert.assertArrayEquals(expectedData, testData.getActualData());
    in.close();
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
  }
}
