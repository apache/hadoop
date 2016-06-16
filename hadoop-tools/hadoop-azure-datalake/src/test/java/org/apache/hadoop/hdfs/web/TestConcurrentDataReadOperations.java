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

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.TestADLResponseData;
import org.apache.hadoop.fs.common.AdlMockWebServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is responsible for testing multiple threads trying to access same
 * or multiple files from the offset. This tests are meant to test
 * BufferManager.java and BatchByteArrayInputStream implementation.
 */
@RunWith(Parameterized.class)
public class TestConcurrentDataReadOperations extends AdlMockWebServer {

  private static FSDataInputStream commonHandle = null;
  private static Object lock = new Object();
  private int concurrencyLevel;

  public TestConcurrentDataReadOperations(int concurrencyLevel) {
    this.concurrencyLevel = concurrencyLevel;
    getConf().set("adl.feature.override.readahead.max.buffersize", "102400");
    getConf().set("adl.feature.override.readahead.max.concurrent.connection",
        "1");
  }

  @Parameterized.Parameters(name = "{index}")
  public static Collection testDataNumberOfConcurrentRun() {
    return Arrays.asList(new Object[][] {{1}, {2}, {3}, {4}, {5}});
  }

  public static byte[] getRandomByteArrayData(int size) {
    byte[] b = new byte[size];
    Random rand = new Random();
    rand.nextBytes(b);
    return b;
  }

  private void setDispatcher(final ArrayList<CreateTestData> testData) {
    getMockServer().setDispatcher(new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest recordedRequest)
          throws InterruptedException {
        if (recordedRequest.getPath().equals("/refresh")) {
          return AdlMockWebServer.getTokenResponse();
        }

        CreateTestData currentRequest = null;
        for (CreateTestData local : testData) {
          if (recordedRequest.getPath().contains(local.path.toString())) {
            currentRequest = local;
            break;
          }
        }

        if (currentRequest == null) {
          new MockResponse().setBody("Request data not found")
              .setResponseCode(501);
        }

        if (recordedRequest.getRequestLine().contains("op=GETFILESTATUS")) {
          return new MockResponse().setResponseCode(200).setBody(
              TestADLResponseData
                  .getGetFileStatusJSONResponse(currentRequest.data.length));
        }

        if (recordedRequest.getRequestLine().contains("op=OPEN")) {
          String request = recordedRequest.getRequestLine();
          int offset = 0;
          int byteCount = 0;

          Pattern pattern = Pattern.compile("offset=([0-9]+)");
          Matcher matcher = pattern.matcher(request);
          if (matcher.find()) {
            System.out.println(matcher.group(1));
            offset = Integer.parseInt(matcher.group(1));
          }

          pattern = Pattern.compile("length=([0-9]+)");
          matcher = pattern.matcher(request);
          if (matcher.find()) {
            System.out.println(matcher.group(1));
            byteCount = Integer.parseInt(matcher.group(1));
          }

          Buffer buf = new Buffer();
          buf.write(currentRequest.data, offset, byteCount);
          return new MockResponse().setResponseCode(200)
              .setChunkedBody(buf, 4 * 1024 * 1024);
        }

        return new MockResponse().setBody("NOT SUPPORTED").setResponseCode(501);
      }
    });
  }

  @Before
  public void resetHandle() {
    commonHandle = null;
  }

  @Test
  public void testParallelReadOnDifferentStreams()
      throws IOException, InterruptedException, ExecutionException {

    ArrayList<CreateTestData> createTestData = new ArrayList<CreateTestData>();

    Random random = new Random();

    for (int i = 0; i < concurrencyLevel; i++) {
      CreateTestData testData = new CreateTestData();
      testData
          .set(new Path("/test/concurrentRead/" + UUID.randomUUID().toString()),
              getRandomByteArrayData(random.nextInt(1 * 1024 * 1024)));
      createTestData.add(testData);
    }

    setDispatcher(createTestData);

    ArrayList<ReadTestData> readTestData = new ArrayList<ReadTestData>();
    for (CreateTestData local : createTestData) {
      ReadTestData localReadData = new ReadTestData();
      localReadData.set(local.path, local.data, 0);
      readTestData.add(localReadData);
    }

    runReadTest(readTestData, false);
  }

  @Test
  public void testParallelReadOnSameStreams()
      throws IOException, InterruptedException, ExecutionException {
    ArrayList<CreateTestData> createTestData = new ArrayList<CreateTestData>();

    Random random = new Random();

    for (int i = 0; i < 1; i++) {
      CreateTestData testData = new CreateTestData();
      testData
          .set(new Path("/test/concurrentRead/" + UUID.randomUUID().toString()),
              getRandomByteArrayData(1024 * 1024));
      createTestData.add(testData);
    }

    setDispatcher(createTestData);

    ArrayList<ReadTestData> readTestData = new ArrayList<ReadTestData>();
    ByteArrayInputStream buffered = new ByteArrayInputStream(
        createTestData.get(0).data);

    ReadTestData readInitially = new ReadTestData();
    byte[] initialData = new byte[1024 * 1024];
    buffered.read(initialData);

    readInitially.set(createTestData.get(0).path, initialData, 0);
    readTestData.add(readInitially);
    runReadTest(readTestData, false);

    readTestData.clear();

    for (int i = 0; i < concurrencyLevel * 5; i++) {
      ReadTestData localReadData = new ReadTestData();
      int offset = random.nextInt((1024 * 1024)-1);
      int length = 1024 * 1024 - offset;
      byte[] expectedData = new byte[length];
      buffered.reset();
      buffered.skip(offset);
      buffered.read(expectedData);
      localReadData.set(createTestData.get(0).path, expectedData, offset);
      readTestData.add(localReadData);
    }

    runReadTest(readTestData, true);
  }

  void runReadTest(ArrayList<ReadTestData> testData, boolean useSameStream)
      throws InterruptedException, ExecutionException {

    ExecutorService executor = Executors.newFixedThreadPool(testData.size());
    Future[] subtasks = new Future[testData.size()];

    for (int i = 0; i < testData.size(); i++) {
      subtasks[i] = executor.submit(
          new ReadConcurrentRunnable(testData.get(i).data, testData.get(i).path,
              testData.get(i).offset, useSameStream));
    }

    executor.shutdown();

    // wait until all tasks are finished
    executor.awaitTermination(120, TimeUnit.SECONDS);

    for (int i = 0; i < testData.size(); ++i) {
      Assert.assertTrue((Boolean) subtasks[i].get());
    }
  }

  class ReadTestData {
    private Path path;
    private byte[] data;
    private int offset;

    public void set(Path filePath, byte[] dataToBeRead, int fromOffset) {
      this.path = filePath;
      this.data = dataToBeRead;
      this.offset = fromOffset;
    }
  }

  class CreateTestData {
    private Path path;
    private byte[] data;

    public void set(Path filePath, byte[] dataToBeWritten) {
      this.path = filePath;
      this.data = dataToBeWritten;
    }
  }

  class ReadConcurrentRunnable implements Callable<Boolean> {
    private Path path;
    private int offset;
    private byte[] expectedData;
    private boolean useSameStream;

    public ReadConcurrentRunnable(byte[] expectedData, Path path, int offset,
        boolean useSameStream) {
      this.path = path;
      this.offset = offset;
      this.expectedData = expectedData;
      this.useSameStream = useSameStream;
    }

    public Boolean call() throws IOException {
      try {
        FSDataInputStream in;
        if (useSameStream) {
          synchronized (lock) {
            if (commonHandle == null) {
              commonHandle = getMockAdlFileSystem().open(path);
            }
            in = commonHandle;
          }
        } else {
          in = getMockAdlFileSystem().open(path);
        }

        byte[] actualData = new byte[expectedData.length];
        in.readFully(offset, actualData);
        Assert.assertArrayEquals("Path :" + path.toString() + " did not match.",
            expectedData, actualData);
        if (!useSameStream) {
          in.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
      return true;
    }
  }
}
