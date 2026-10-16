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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.object.exceptions.ChecksumMismatchException;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestObjectMultiRangeInputStream extends ObjectStorageTestBase {
  private static ExecutorService threadPool;

  @BeforeAll
  public static void beforeClass() {
    threadPool = ThreadPools.newWorkerPool("TestObjectInputStream-pool");
  }

  @AfterAll
  public static void afterClass() {
    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  @Test
  public void testSequentialAndRandomRead() throws IOException {
    Path outPath = new Path(testDir(), "testSequentialAndRandomRead.txt");
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(5 << 20);
    getStorage().put(key, rawData);

    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(),
        ObjectUtils.pathToKey(outPath), rawData.length, Long.MAX_VALUE, content.checksum())) {
      // sequential read
      assertEquals(0, in.getPos());
      assertEquals(0, in.nextExpectPos());

      byte[] b = new byte[1024];
      int readCnt = in.read(b);
      assertEquals(readCnt, b.length);
      assertArrayEquals(Arrays.copyOfRange(rawData, 0, 1024), b);
      assertEquals(1024, in.getPos());
      assertEquals(1024, in.nextExpectPos());

      readCnt = in.read(b);
      assertEquals(readCnt, b.length);
      assertArrayEquals(Arrays.copyOfRange(rawData, 1024, 2048), b);
      assertEquals(2048, in.getPos());
      assertEquals(2048, in.nextExpectPos());

      // random read forward
      in.seek(4 << 20);
      assertEquals(4 << 20, in.getPos());
      assertEquals(2048, in.nextExpectPos());

      readCnt = in.read(b);
      assertEquals(readCnt, b.length);
      assertArrayEquals(Arrays.copyOfRange(rawData, 4 << 20, 1024 + (4 << 20)), b);
      assertEquals((4 << 20) + 1024, in.getPos());
      assertEquals((4 << 20) + 1024, in.nextExpectPos());

      // random read back
      in.seek(2 << 20);
      assertEquals(2 << 20, in.getPos());
      assertEquals((4 << 20) + 1024, in.nextExpectPos());

      readCnt = in.read(b);
      assertEquals(readCnt, b.length);
      assertArrayEquals(Arrays.copyOfRange(rawData, 2 << 20, 1024 + (2 << 20)), b);
      assertEquals((2 << 20) + 1024, in.getPos());
      assertEquals((2 << 20) + 1024, in.nextExpectPos());
    }
  }

  private InputStream getStream(String key) {
    return getStorage().get(key).stream();
  }

  @Test
  public void testReadSingleByte() throws IOException {
    int len = 10;
    Path outPath = new Path(testDir(), "testReadSingleByte.txt");
    byte[] data = TestUtility.rand(len);
    String key = ObjectUtils.pathToKey(outPath);
    byte[] checksum = getStorage().put(key, data);

    try (InputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(), key,
        data.length, Long.MAX_VALUE, checksum)) {
      for (int i = 0; i < data.length; i++) {
        assertTrue(in.read() >= 0);
      }
      assertEquals(-1, in.read());
    }
  }

  @Test
  public void testReadStreamButTheFileChangedDuringReading() throws IOException {
    int len = 2048;
    Path outPath = new Path(testDir(), "testReadStreamButTheFileChangedDuringReading.txt");
    byte[] data = TestUtility.rand(len);
    String key = ObjectUtils.pathToKey(outPath);
    byte[] checksum = getStorage().put(key, data);

    try (InputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(), key,
        data.length, 1024, checksum)) {
      byte[] read = new byte[1024];
      int n = in.read(read);
      assertEquals(1024, n);

      getStorage().put(key, TestUtility.rand(1024));
      assertThrows(ChecksumMismatchException.class, () -> in.read(read), "The file is staled");
    }
  }

  @Test
  public void testRead100M() throws IOException {
    testSequentialReadData(100 << 20, 6 << 20);
    testSequentialReadData(100 << 20, 5 << 20);
  }

  @Test
  public void testRead10M() throws IOException {
    testSequentialReadData(10 << 20, 4 << 20);
    testSequentialReadData(10 << 20, 5 << 20);
  }

  @Test
  public void testParallelRead10M() throws IOException, ExecutionException, InterruptedException {
    testParallelRandomRead(10 << 20, 4 << 20);
    testParallelRandomRead(10 << 20, 5 << 20);
  }

  @Test
  public void testRead100b() throws IOException {
    testSequentialReadData(100, 40);
    testSequentialReadData(100, 50);
    testSequentialReadData(100, 100);
    testSequentialReadData(100, 101);
  }

  private void testSequentialReadData(int dataSize, int partSize) throws IOException {
    Path outPath = new Path(testDir(), String.format("%d-%d.txt", dataSize, partSize));
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(dataSize);
    getStorage().put(key, rawData);

    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    int batchSize = (dataSize - 1) / partSize + 1;
    try (InputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(),
        ObjectUtils.pathToKey(outPath), rawData.length, Long.MAX_VALUE, content.checksum())) {
      for (int i = 0; i < batchSize; i++) {
        int start = i * partSize;
        int end = Math.min(dataSize, start + partSize);
        byte[] expectArr = Arrays.copyOfRange(rawData, start, end);

        byte[] b = new byte[end - start];
        int ret = in.read(b, 0, b.length);

        assertEquals(b.length, ret);
        assertArrayEquals(expectArr, b, String.format("the read bytes mismatched at batch: %d", i));
      }
      assertEquals(-1, in.read());
    }
  }

  private void testParallelRandomRead(int dataSize, int partSize)
      throws IOException, ExecutionException, InterruptedException {

    Path outPath = new Path(testDir(), String.format("%d-%d.txt", dataSize, partSize));
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(dataSize);
    getStorage().put(key, rawData);

    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    Random random = new Random();
    List<Future<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      int position = random.nextInt(rawData.length);
      tasks.add(threadPool.submit(
          () -> testReadDataFromSpecificPosition(rawData, outPath, position, partSize,
              content.checksum())));
    }

    for (Future<Boolean> task : tasks) {
      assertTrue(task.get());
    }
  }

  private boolean testReadDataFromSpecificPosition(
      final byte[] rawData,
      final Path objPath,
      final int startPosition,
      final int partSize,
      byte[] checksum) {
    int rawDataSize = rawData.length;
    int batchSize = (rawDataSize - startPosition - 1) / partSize + 1;
    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(),
        ObjectUtils.pathToKey(objPath), rawDataSize, Long.MAX_VALUE, checksum)) {
      in.seek(startPosition);

      for (int i = 0; i < batchSize; i++) {
        int start = startPosition + i * partSize;
        int end = Math.min(rawDataSize, start + partSize);
        byte[] expectArr = Arrays.copyOfRange(rawData, start, end);

        byte[] b = new byte[end - start];
        int ret = in.read(b, 0, b.length);

        assertEquals(b.length, ret);
        assertArrayEquals(expectArr, b, String.format("the read bytes mismatched at batch: %d", i));
      }
      assertEquals(-1, in.read());
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  @Test
  public void testParallelReadFromOneInputStream() throws IOException, ExecutionException,
      InterruptedException {
    testParallelReadFromOneInputStreamImpl(10 << 20, 512, 10);
    testParallelReadFromOneInputStreamImpl(10 << 20, 64, 100);
    testParallelReadFromOneInputStreamImpl(1 << 20, 2 << 20, 5);
  }

  public void testParallelReadFromOneInputStreamImpl(int dataSize, int batchSize, int parallel)
      throws IOException, ExecutionException, InterruptedException {

    Path outPath = new Path(testDir(),
        String.format("%d-%d-testParallelReadFromOneInputStreamImpl.txt", dataSize, batchSize));
    String key = ObjectUtils.pathToKey(outPath);
    byte[] rawData = TestUtility.rand(dataSize);
    getStorage().put(key, rawData);
    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    AtomicInteger sum = new AtomicInteger(0);
    CopyOnWriteArrayList<byte[]> readBytes = new CopyOnWriteArrayList();
    List<Future<?>> futures = new ArrayList<>();
    try (ObjectMultiRangeInputStream inputStream = new ObjectMultiRangeInputStream(threadPool,
        getStorage(), ObjectUtils.pathToKey(outPath), rawData.length, Long.MAX_VALUE,
        content.checksum())) {
      for (int i = 0; i < parallel; i++) {
        futures.add(threadPool.submit(() -> {
          byte[] data = new byte[batchSize];
          try {
            int count;
            while ((count = inputStream.read(data)) != -1) {
              sum.getAndAdd(count);
              readBytes.add(Arrays.copyOfRange(data, 0, count));
              data = new byte[batchSize];
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      for (Future<?> future : futures) {
        future.get();

      }
      assertEquals(rawData.length, sum.get());
    }

    byte[] actualBytes = new byte[rawData.length];
    int offset = 0;
    for (byte[] bytes : readBytes) {
      System.arraycopy(bytes, 0, actualBytes, offset, bytes.length);
      offset += bytes.length;
    }

    Arrays.sort(actualBytes);
    Arrays.sort(rawData);
    assertArrayEquals(rawData, actualBytes);
  }

  @Test
  public void testPositionalRead() throws IOException {
    Path outPath = new Path(testDir(), "testPositionalRead.txt");
    String key = ObjectUtils.pathToKey(outPath);
    int fileSize = 5 << 20;
    byte[] rawData = TestUtility.rand(fileSize);
    getStorage().put(key, rawData);
    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    Random rand = ThreadLocalRandom.current();

    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(),
        ObjectUtils.pathToKey(outPath), fileSize, Long.MAX_VALUE, content.checksum())) {
      for (int i = 0; i < 100; i++) {
        int pos = rand.nextInt(fileSize);
        int len = rand.nextInt(fileSize);

        int expectSize = Math.min(fileSize - pos, len);
        byte[] actual = new byte[expectSize];
        int actualLen = in.read(pos, actual, 0, expectSize);

        assertEquals(expectSize, actualLen);
        assertArrayEquals(Bytes.toBytes(rawData, pos, expectSize), actual);
      }
    }
  }

  @Test
  public void testReadAcrossRange() throws IOException {
    Path outPath = new Path(testDir(), "testReadAcrossRange.txt");
    String key = ObjectUtils.pathToKey(outPath);
    int fileSize = 1 << 10;
    byte[] rawData = TestUtility.rand(fileSize);
    getStorage().put(key, rawData);
    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(
        ThreadPools.defaultWorkerPool(), getStorage(), key, fileSize, 10, content.checksum())) {
      byte[] data = new byte[fileSize / 2];
      for (int i = 0; i < 2; i++) {
        assertEquals(data.length, in.read(data));
        assertEquals((i + 1) * data.length, in.getPos());
        assertArrayEquals(Bytes.toBytes(rawData, i * data.length, data.length), data);
      }
    }
  }

  @Test
  public void testStorageRange() throws IOException {
    Path outPath = new Path(testDir(), "testStorageRange.txt");
    String key = ObjectUtils.pathToKey(outPath);
    int fileSize = 5 << 20;
    byte[] rawData = TestUtility.rand(fileSize);
    getStorage().put(key, rawData);
    ObjectContent content = getStorage().get(key);
    assertArrayEquals(rawData, IOUtils.toByteArray(content.stream()));

    int oneMB = 1 << 20;
    long rangeOpenLen = oneMB;
    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(
        ThreadPools.defaultWorkerPool(), getStorage(), key, fileSize, rangeOpenLen,
        content.checksum())) {
      assertNull(in.stream());

      // Init range.
      in.read();
      assertEquals(Range.of(0, rangeOpenLen), in.stream().range());
      // Range doesn't change.
      in.read(new byte[(int) (rangeOpenLen - 1)], 0, (int) (rangeOpenLen - 1));
      assertEquals(Range.of(0, rangeOpenLen), in.stream().range());

      // Move to next range.
      in.read();
      assertEquals(Range.of(rangeOpenLen, rangeOpenLen), in.stream().range());

      // Seek and move.
      in.seek(rangeOpenLen * 3 + 10);
      in.read();
      assertEquals(Range.of(rangeOpenLen * 3, rangeOpenLen), in.stream().range());

      // Seek small and range doesn't change.
      in.seek(in.getPos() + 1);
      in.read();
      assertEquals(Range.of(rangeOpenLen * 3, rangeOpenLen), in.stream().range());

      // Seek big and range changes.
      in.seek(rangeOpenLen * 2);
      in.read(new byte[(int) (rangeOpenLen - 10)], 0, (int) (rangeOpenLen - 10));
      assertEquals(Range.of(rangeOpenLen * 2, rangeOpenLen), in.stream().range());
      // Old range has 10 bytes left. Seek 10 bytes then read 10 bytes. Old range can't read any
      // bytes, so range changes.
      assertEquals(rangeOpenLen * 3 - 10, in.getPos());
      in.seek(rangeOpenLen * 3);
      in.read(new byte[10], 0, 10);
      assertEquals(Range.of(rangeOpenLen * 3, rangeOpenLen), in.stream().range());

      // Read big buffer.
      in.seek(10);
      in.read(new byte[oneMB * 3], 0, oneMB * 3);
      assertEquals(oneMB * 3 + 10, in.getPos());
      assertEquals(Range.of(3 * rangeOpenLen, rangeOpenLen), in.stream().range());
    }

    try (ObjectMultiRangeInputStream in = new ObjectMultiRangeInputStream(threadPool, getStorage(),
        ObjectUtils.pathToKey(outPath), fileSize, Long.MAX_VALUE, content.checksum())) {
      assertNull(in.stream());

      // Init range.
      in.read();
      assertEquals(Range.of(0, fileSize), in.stream().range());

      // Range doesn't change.
      in.read(new byte[oneMB], 0, oneMB);
      assertEquals(Range.of(0, fileSize), in.stream().range());

      // Seek and move.
      long pos = oneMB * 3 + 10;
      in.seek(pos);
      in.read();
      assertEquals(Range.of(0, fileSize), in.stream().range());
    }
  }
}
