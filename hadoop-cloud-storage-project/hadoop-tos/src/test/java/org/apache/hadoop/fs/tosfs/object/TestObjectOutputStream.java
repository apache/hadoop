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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.staging.StagingPart;
import org.apache.hadoop.fs.tosfs.object.staging.State;
import org.apache.hadoop.fs.tosfs.util.FSUtils;
import org.apache.hadoop.fs.tosfs.util.TempFiles;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestObjectOutputStream extends ObjectStorageTestBase {

  private static ExecutorService threadPool;

  @BeforeAll
  public static void beforeClass() {
    threadPool = ThreadPools.newWorkerPool("TestObjectOutputStream-pool");
  }

  @AfterAll
  public static void afterClass() {
    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  @Test
  public void testMkStagingDir() throws ExecutionException, InterruptedException, IOException {
    try (TempFiles tmp = TempFiles.of()) {
      List<String> tmpDirs = Lists.newArrayList();
      for (int i = 0; i < 3; i++) {
        tmpDirs.add(tmp.newDir());
      }
      Configuration newConf = new Configuration(tosConf());
      newConf.set(ConfKeys.FS_MULTIPART_STAGING_DIR.key("filestore"), Joiner.on(",").join(tmpDirs));

      // Start multiple threads to open streams to create staging dir.
      List<Future<ObjectOutputStream>> futures = Collections.synchronizedList(new ArrayList<>());
      for (int i = 0; i < 10; i++) {
        futures.add(threadPool.submit(() ->
            new ObjectOutputStream(getStorage(), threadPool, newConf, path("none.txt"), true)));
      }
      for (Future<ObjectOutputStream> f : futures) {
        f.get().close();
      }
    }
  }

  @Test
  public void testWriteZeroByte() throws IOException {
    Path zeroByteTxt = path("zero-byte.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), zeroByteTxt, true);
    // write zero-byte and close.
    out.write(new byte[0], 0, 0);
    out.close();
    assertStagingPart(0, out.stagingParts());

    // Read and validate the dest object contents
    ObjectTestUtils.assertObject(zeroByteTxt, ObjectTestUtils.EMPTY_BYTES);
  }

  @Test
  public void testWriteZeroByteWithoutAllowPut() throws IOException {
    Path zeroByteTxt = path("zero-byte-without-allow-put.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), zeroByteTxt, false);
    // write zero-byte and close.
    out.close();
    assertStagingPart(0, out.stagingParts());

    // Read and validate the dest object content.
    ObjectTestUtils.assertObject(zeroByteTxt, ObjectTestUtils.EMPTY_BYTES);
  }

  @Test
  public void testDeleteStagingFileWhenUploadPartsOK() throws IOException {
    Path path = path("data.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), path, true);
    byte[] data = TestUtility.rand((int) (ConfKeys.FS_MULTIPART_SIZE_DEFAULT * 2));
    out.write(data);
    out.waitForPartsUpload();
    for (StagingPart part : out.stagingParts()) {
      assertEquals(State.CLEANED, part.state());
    }
    out.close();
    for (StagingPart part : out.stagingParts()) {
      assertEquals(State.CLEANED, part.state());
    }
  }

  @Test
  public void testDeleteStagingFileWithClose() throws IOException {
    Path path = path("data.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), path, true);
    byte[] data = TestUtility.rand((int) (ConfKeys.FS_MULTIPART_SIZE_DEFAULT * 2));
    out.write(data);
    out.close();
    for (StagingPart part : out.stagingParts()) {
      assertEquals(State.CLEANED, part.state());
    }
  }

  @Test
  public void testDeleteSimplePutStagingFile() throws IOException {
    Path smallTxt = path("small.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), smallTxt, true);
    byte[] data = TestUtility.rand(4 << 20);
    out.write(data);
    for (StagingPart part : out.stagingParts()) {
      assertTrue(part.size() > 0);
    }
    out.close();
    for (StagingPart part : out.stagingParts()) {
      assertEquals(State.CLEANED, part.state());
    }
  }

  @Test
  public void testSimplePut() throws IOException {
    Path smallTxt = path("small.txt");
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), smallTxt, true);
    byte[] data = TestUtility.rand(4 << 20);
    out.write(data);
    out.close();
    assertStagingPart(1, out.stagingParts());
    assertNull(out.upload(), "Should use the simple PUT to upload object for small file.");

    // Read and validate the dest object content.
    ObjectTestUtils.assertObject(smallTxt, data);
  }

  public void testWrite(int uploadPartSize, int len) throws IOException {
    Configuration newConf = new Configuration(tosConf());
    newConf.setLong(ConfKeys.FS_MULTIPART_SIZE.key(FSUtils.scheme(conf(), testDir().toUri())),
        uploadPartSize);

    Path outPath = path(len + ".txt");
    int partNum = (len - 1) / uploadPartSize + 1;

    byte[] data = TestUtility.rand(len);
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, newConf, outPath, true);
    try {
      out.write(data);
    } finally {
      out.close();
    }

    assertStagingPart(partNum, out.stagingParts());
    ObjectTestUtils.assertObject(outPath, data);

    // List multipart uploads
    int uploadsNum = 0;
    for (MultipartUpload ignored : getStorage().listUploads(out.destKey())) {
      uploadsNum += 1;
    }
    assertEquals(0L, uploadsNum);
  }

  @Test
  public void testParallelWriteOneOutPutStream() throws IOException, ExecutionException,
      InterruptedException {
    testParallelWriteOneOutPutStreamImpl(5 << 20, 10, 128);
    testParallelWriteOneOutPutStreamImpl(5 << 20, 10, 1 << 20);
    testParallelWriteOneOutPutStreamImpl(5 << 20, 10, 2 << 20);
    testParallelWriteOneOutPutStreamImpl(5 << 20, 10, 6 << 20);
  }

  public void testParallelWriteOneOutPutStreamImpl(int partSize, int epochs, int batchSize)
      throws IOException, ExecutionException, InterruptedException {
    Configuration newConf = new Configuration(tosConf());
    newConf.setLong(ConfKeys.FS_MULTIPART_SIZE.key(FSUtils.scheme(conf(), testDir().toUri())),
        partSize);

    String file =
        String.format("%d-%d-%d-testParallelWriteOneOutPutStream.txt", partSize, epochs, batchSize);
    Path outPath = path(file);
    try (ObjectOutputStream out = new ObjectOutputStream(getStorage(), threadPool, newConf, outPath,
        true)) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < epochs; i++) {
        final int index = i;
        futures.add(threadPool.submit(() -> {
          try {
            out.write(dataset(batchSize, index));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));
      }

      // wait for all tasks finished
      for (Future<?> future : futures) {
        future.get();
      }
    }

    try (InputStream inputStream = getStorage().get(ObjectUtils.pathToKey(outPath)).stream()) {
      List<byte[]> ret = new ArrayList<>();
      byte[] data = new byte[batchSize];
      while (inputStream.read(data) != -1) {
        ret.add(data);
        data = new byte[batchSize];
      }

      assertEquals(epochs, ret.size());
      List<byte[]> sortedRet = ret.stream()
          .sorted(Comparator.comparingInt(o -> o[0]))
          .collect(Collectors.toList());

      int j = 0;
      for (byte[] e : sortedRet) {
        assertArrayEquals(dataset(batchSize, j), e);
        j++;
      }
    }
  }

  public static byte[] dataset(int len, int base) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base);
    }
    return dataset;
  }

  @Test
  public void testWrite1MB() throws IOException {
    testWrite(5 << 20, 1 << 20);
    testWrite(8 << 20, 1 << 20);
    testWrite(16 << 20, 1 << 20);
  }

  @Test
  public void testWrite24MB() throws IOException {
    testWrite(5 << 20, 24 << 20);
    testWrite(8 << 20, 24 << 20);
    testWrite(16 << 20, 24 << 20);
  }

  @Test
  public void testWrite100MB() throws IOException {
    testWrite(5 << 20, 100 << 20);
    testWrite(8 << 20, 100 << 20);
    testWrite(16 << 20, 100 << 20);
  }

  private void testMultipartThreshold(int partSize, int multipartThreshold, int dataSize)
      throws IOException {
    Configuration newConf = new Configuration(tosConf());
    newConf.setLong(ConfKeys.FS_MULTIPART_SIZE.key(scheme()), partSize);
    newConf.setLong(ConfKeys.FS_MULTIPART_THRESHOLD.key(scheme()), multipartThreshold);
    Path outPath =
        path(String.format("threshold-%d-%d-%d.txt", partSize, multipartThreshold, dataSize));

    byte[] data = TestUtility.rand(dataSize);
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, newConf, outPath, true);
    try {
      // Verify for every 1MB data writing, unless reaching the threshold.
      int upperLimit = Math.min(multipartThreshold, dataSize);
      int curOff = 0;
      for (; curOff < upperLimit; curOff += (1 << 20)) {
        int end = Math.min(curOff + (1 << 20), upperLimit);
        out.write(Arrays.copyOfRange(data, curOff, end));

        List<MultipartUpload> uploads = Lists.newArrayList(getStorage().listUploads(out.destKey()));
        if (end < multipartThreshold) {
          assertEquals(0, uploads.size(),
              "Shouldn't has any uploads because it just use simple PUT");
        } else {
          assertEquals(1, uploads.size(), "Switch to use MPU.");
        }
        assertEquals((end - 1) / partSize + 1, out.stagingParts().size());
      }

      // Verify for every 1MB data writing, unless reaching the data size.
      for (; curOff < dataSize; curOff += (1 << 20)) {
        int end = Math.min(curOff + (1 << 20), dataSize);
        out.write(Arrays.copyOfRange(data, curOff, end));

        List<MultipartUpload> uploads = Lists.newArrayList(getStorage().listUploads(out.destKey()));
        assertEquals(1, uploads.size());
        assertEquals(out.destKey(), uploads.get(0).key());
        assertEquals((end - 1) / partSize + 1, out.stagingParts().size());
      }
    } finally {
      out.close();
    }

    assertStagingPart((dataSize - 1) / partSize + 1, out.stagingParts());
    ObjectTestUtils.assertObject(outPath, data);

    List<MultipartUpload> uploads = Lists.newArrayList(getStorage().listUploads(out.destKey()));
    assertEquals(0, uploads.size());
  }

  @Test
  public void testMultipartThreshold2MB() throws IOException {
    testMultipartThreshold(5 << 20, 2 << 20, 1 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, (2 << 20) - 1);
    testMultipartThreshold(5 << 20, 2 << 20, 2 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, 4 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, 5 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, (5 << 20) + 1);
    testMultipartThreshold(5 << 20, 2 << 20, 6 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, 10 << 20);
    testMultipartThreshold(5 << 20, 2 << 20, 20 << 20);
  }

  @Test
  public void testMultipartThreshold5MB() throws IOException {
    testMultipartThreshold(5 << 20, 5 << 20, 1 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 4 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 5 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 5 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 6 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 10 << 20);
    testMultipartThreshold(5 << 20, 5 << 20, 20 << 20);
  }

  @Test
  public void testMultipartThreshold10MB() throws IOException {
    testMultipartThreshold(5 << 20, 10 << 20, 1 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 10 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 11 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 15 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 20 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 40 << 20);
    testMultipartThreshold(5 << 20, 10 << 20, 30 << 20);
  }

  @Test
  public void testCloseStreamTwice() throws IOException {
    int len = 100;
    Path outPath = path(len + ".txt");
    int partNum = 1;

    byte[] data = TestUtility.rand(len);
    ObjectOutputStream out =
        new ObjectOutputStream(getStorage(), threadPool, tosConf(), outPath, true);
    try {
      out.write(data);
      out.close();
    } finally {
      out.close();
    }

    assertStagingPart(partNum, out.stagingParts());
    ObjectTestUtils.assertObject(outPath, data);
  }

  @Test
  public void testWriteClosedStream() throws IOException {
    byte[] data = TestUtility.rand(10);
    Path outPath = path("testWriteClosedStream.txt");
    try (ObjectOutputStream out = new ObjectOutputStream(getStorage(), threadPool, tosConf(),
        outPath, true)) {
      out.close();
      out.write(data);
    } catch (IllegalStateException e) {
      assertEquals("OutputStream is closed.", e.getMessage());
    }
  }

  private static void assertStagingPart(int expectedNum, List<StagingPart> parts) {
    assertEquals(expectedNum, parts.size());
    for (StagingPart part : parts) {
      assertTrue(part.size() > 0);
    }
  }

  private Path path(String name) {
    return new Path(testDir(), name);
  }
}
