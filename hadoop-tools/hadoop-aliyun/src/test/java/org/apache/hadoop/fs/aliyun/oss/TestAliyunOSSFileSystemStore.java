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

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.VoidResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.fs.aliyun.oss.Constants.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class TestAliyunOSSFileSystemStore {
  private AliyunOSSFileSystemStore store;
  private AliyunOSSFileSystem fs;

  private void initializeFileSystem() throws IOException {
    Configuration conf = new Configuration();
    assumeTrue(conf.get(Constants.ACCESS_KEY_ID) != null);
    assumeTrue(conf.get(Constants.ACCESS_KEY_SECRET) != null);
    assumeTrue(conf.get("test.fs.oss.name") != null);
    fs = new AliyunOSSFileSystem();
    fs.initialize(URI.create(conf.get("test.fs.oss.name")), conf);
    store = fs.getStore();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store == null) {
      return;
    }
    try {
      store.purge("test");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  protected void writeRenameReadCompare(Path path, long len)
      throws IOException, NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    OutputStream out = new BufferedOutputStream(
        new DigestOutputStream(fs.create(path, false), digest));
    for (long i = 0; i < len; i++) {
      out.write('Q');
    }
    out.flush();
    out.close();

    assertTrue(fs.exists(path), "Exists");

    ObjectMetadata srcMeta = fs.getStore().getObjectMetadata(
        path.toUri().getPath().substring(1));

    Path copyPath = path.suffix(".copy");
    fs.rename(path, copyPath);

    assertTrue(fs.exists(copyPath), "Copy exists");
    // file type should not change
    ObjectMetadata dstMeta = fs.getStore().getObjectMetadata(
        copyPath.toUri().getPath().substring(1));
    assertEquals(srcMeta.getObjectType(), dstMeta.getObjectType());
    // Download file from Aliyun OSS and compare the digest against the original
    MessageDigest digest2 = MessageDigest.getInstance("MD5");
    InputStream in = new BufferedInputStream(
        new DigestInputStream(fs.open(copyPath), digest2));
    long copyLen = 0;
    while (in.read() != -1) {
      copyLen++;
    }
    in.close();

    assertEquals(len, copyLen, "Copy length matches original");
    assertArrayEquals(digest.digest(), digest2.digest(), "Digests match");
  }

  @Test
  public void testSmallUpload() throws IOException, NoSuchAlgorithmException {
    initializeFileSystem();
    // Regular upload, regular copy
    writeRenameReadCompare(new Path("/test/small"), 16384);
  }

  @Test
  public void testLargeUpload()
      throws IOException, NoSuchAlgorithmException {
    initializeFileSystem();
    // Multipart upload, shallow copy
    writeRenameReadCompare(new Path("/test/xlarge"),
        Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT + 1);
  }

  @Test
  public void testDeleteObjects() throws IOException, NoSuchAlgorithmException {
    initializeFileSystem();
    // generate test files
    final int files = 10;
    final long size = 5 * 1024 * 1024;
    final String prefix = "dir";
    for (int i = 0; i < files; i++) {
      Path path = new Path(String.format("/%s/testFile-%d.txt", prefix, i));
      ContractTestUtils.generateTestFile(this.fs, path, size, 256, 255);
    }
    OSSListRequest listRequest =
        store.createListObjectsRequest(prefix, MAX_PAGING_KEYS_DEFAULT, null, null, true);
    List<String> keysToDelete = new ArrayList<>();
    OSSListResult objects = store.listObjects(listRequest);
    assertEquals(files, objects.getObjectSummaries().size());

    // test delete files
    for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
      keysToDelete.add(objectSummary.getKey());
    }
    store.deleteObjects(keysToDelete);
    objects = store.listObjects(listRequest);
    assertEquals(0, objects.getObjectSummaries().size());
  }

  private static class RecordingStore extends AliyunOSSFileSystemStore
      implements AutoCloseable {
    private final List<Object> responses = new ArrayList<>();
    private final List<CompleteMultipartUploadRequest> requests =
        new ArrayList<>();
    private final List<Long> delays = new ArrayList<>();
    private int nextResponse;
    private boolean interrupt;

    @Override
    CompleteMultipartUploadResult submitCompleteMultipartUpload(
        CompleteMultipartUploadRequest request) {
      requests.add(request);
      Object response = responses.get(nextResponse++);
      if (response instanceof RuntimeException) {
        throw (RuntimeException) response;
      }
      return (CompleteMultipartUploadResult) response;
    }

    @Override
    void sleepBeforeCompleteRetry(long delayMillis)
        throws InterruptedException {
      delays.add(delayMillis);
      if (interrupt) {
        throw new InterruptedException("test interruption");
      }
    }
  }

  private static class CopyClient extends OSSClient {
    private int copiedParts;
    private int aborts;

    CopyClient() {
      super("http://localhost", "test-id", "test-secret");
    }

    @Override
    public CopyObjectResult copyObject(String srcBucket, String srcKey,
        String dstBucket, String dstKey) {
      throw new ClientException("Use multipart copy");
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(
        InitiateMultipartUploadRequest request) {
      InitiateMultipartUploadResult result =
          new InitiateMultipartUploadResult();
      result.setUploadId("copy-upload-id");
      return result;
    }

    @Override
    public UploadPartCopyResult uploadPartCopy(UploadPartCopyRequest request) {
      copiedParts++;
      UploadPartCopyResult result = new UploadPartCopyResult();
      result.setPartNumber(request.getPartNumber());
      result.setETag("copy-etag");
      return result;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(
        CompleteMultipartUploadRequest request) {
      throw new AssertionError("Multipart copy bypassed completion retries");
    }

    @Override
    public VoidResult abortMultipartUpload(AbortMultipartUploadRequest request) {
      aborts++;
      return new VoidResult();
    }
  }

  private static RecordingStore configuredStore(Configuration conf)
      throws IOException {
    RecordingStore retryStore = new RecordingStore();
    conf.set(ENDPOINT_KEY, "http://localhost");
    conf.set(ACCESS_KEY_ID, "test-id");
    conf.set(ACCESS_KEY_SECRET, "test-secret");
    retryStore.initialize(URI.create("oss://bucket"), conf, "test-user",
        new FileSystem.Statistics("oss"));
    return retryStore;
  }

  private static RecordingStore configuredStore(int retryLimit,
      String retryInterval) throws IOException {
    Configuration conf = new Configuration(false);
    conf.setInt(MULTIPART_COMPLETE_RETRY_LIMIT_KEY, retryLimit);
    conf.set(MULTIPART_COMPLETE_RETRY_INTERVAL_KEY, retryInterval);
    return configuredStore(conf);
  }

  private static OSSException ossError(String code) {
    return new OSSException("OSS rejected completion", code,
        "request", "host", null, null, null);
  }

  private static CompleteMultipartUploadResult complete(RecordingStore retryStore) {
    List<PartETag> parts = new ArrayList<>(Arrays.asList(
        new PartETag(2, "second"), new PartETag(1, "first")));
    return retryStore.completeMultipartUpload("key", "upload-id", parts);
  }

  @Test
  public void testCompleteMultipartUploadRetries() throws IOException {
    for (int retryLimit : new int[] {0, 1, 3, 10}) {
      for (boolean succeeds : new boolean[] {true, false}) {
        // Leave the default configuration unset to verify initialize() uses it.
        try (RecordingStore retryStore = retryLimit == 3
            ? configuredStore(new Configuration(false))
            : configuredStore(retryLimit, retryLimit == 1 ? "1ms" : "30s")) {
          OSSException throttled = ossError("QpsLimitExceeded");
          CompleteMultipartUploadResult result =
              new CompleteMultipartUploadResult();
          for (int i = 0; i < retryLimit; i++) {
            retryStore.responses.add(throttled);
          }
          retryStore.responses.add(succeeds ? result : throttled);
          if (succeeds) {
            assertSame(result, complete(retryStore));
          } else {
            assertSame(throttled, assertThrows(OSSException.class,
                () -> complete(retryStore)));
          }
          assertEquals(retryLimit + 1, retryStore.requests.size());
          assertEquals(retryLimit, retryStore.delays.size());
          CompleteMultipartUploadRequest request = retryStore.requests.get(0);
          retryStore.requests.forEach(attempt -> assertSame(request, attempt));
          assertEquals("upload-id", request.getUploadId());
          assertEquals(1, request.getPartETags().get(0).getPartNumber());
          assertEquals("first", request.getPartETags().get(0).getETag());
          assertEquals(2, request.getPartETags().get(1).getPartNumber());
          assertEquals("second", request.getPartETags().get(1).getETag());
          if (retryLimit == 3) {
            long[] minimum = {500, 1000, 2000};
            long[] maximum = {1500, 3000, 6000};
            for (int i = 0; i < retryLimit; i++) {
              assertTrue(retryStore.delays.get(i) >= minimum[i]
                  && retryStore.delays.get(i) < maximum[i]);
            }
          } else if (retryLimit == 1) {
            assertTrue(retryStore.delays.get(0) >= 1
                && retryStore.delays.get(0) < 3);
          } else if (retryLimit == 10) {
            // Cap the actual delay, including jitter, even with a larger base.
            retryStore.delays.forEach(delay ->
                assertEquals(10_000L, delay.longValue()));
          }
        }
      }
    }
  }

  @Test
  public void testCompleteMultipartUploadStopsOnFailure() throws IOException {
    for (RuntimeException failure : Arrays.asList(ossError("InvalidPart"),
        ossError("NoSuchUpload"), ossError("AccessDenied"),
        new ClientException("response lost"))) {
      for (int throttles : new int[] {0, 1}) {
        try (RecordingStore retryStore = configuredStore(new Configuration(false))) {
          if (throttles > 0) {
            retryStore.responses.add(ossError("QpsLimitExceeded"));
          }
          retryStore.responses.add(failure);
          assertSame(failure, assertThrows(failure.getClass(),
              () -> complete(retryStore)));
          assertEquals(throttles + 1, retryStore.requests.size());
          assertEquals(throttles, retryStore.delays.size());
        }
      }
    }
    try (RecordingStore retryStore = configuredStore(new Configuration(false))) {
      retryStore.responses.add(ossError("QpsLimitExceeded"));
      retryStore.interrupt = true;
      ClientException failure = assertThrows(ClientException.class,
          () -> complete(retryStore));
      assertTrue(failure.getCause() instanceof InterruptedException);
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, retryStore.requests.size());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testInvalidCompletionRetryConfiguration() {
    for (int retryLimit : new int[] {-1, 11}) {
      IllegalArgumentException failure = assertThrows(
          IllegalArgumentException.class,
          () -> configuredStore(retryLimit, "500ms"));
      assertTrue(failure.getMessage().contains(MULTIPART_COMPLETE_RETRY_LIMIT_KEY));
    }
    for (String interval : Arrays.asList("0ms", "-1ms",
        Long.MAX_VALUE + "ms")) {
      IllegalArgumentException failure = assertThrows(
          IllegalArgumentException.class, () -> configuredStore(3, interval));
      assertTrue(failure.getMessage().contains(MULTIPART_COMPLETE_RETRY_INTERVAL_KEY));
    }
  }

  @Test
  public void testMultipartCopyUsesCompletionRetries() throws Exception {
    for (boolean succeeds : new boolean[] {true, false}) {
      try (RecordingStore retryStore = configuredStore(1, "500ms")) {
        CopyClient client = new CopyClient();
        retryStore.close();
        Field clientField = AliyunOSSFileSystemStore.class
            .getDeclaredField("ossClient");
        clientField.setAccessible(true);
        clientField.set(retryStore, client);
        retryStore.responses.add(ossError("QpsLimitExceeded"));
        retryStore.responses.add(succeeds ? new CompleteMultipartUploadResult()
            : ossError("QpsLimitExceeded"));

        assertEquals(succeeds, retryStore.copyFile("src",
            MULTIPART_UPLOAD_PART_SIZE_DEFAULT + 1, "dst"));
        assertEquals(2, client.copiedParts);
        assertEquals(succeeds ? 0 : 1, client.aborts);
        assertEquals(2, retryStore.requests.size());
        assertEquals(1, retryStore.delays.size());
        assertSame(retryStore.requests.get(0), retryStore.requests.get(1));
        assertEquals("dst", retryStore.requests.get(0).getKey());
        assertEquals("copy-upload-id", retryStore.requests.get(0).getUploadId());
        assertEquals(2, retryStore.requests.get(0).getPartETags().size());
        assertEquals("copy-etag",
            retryStore.requests.get(0).getPartETags().get(0).getETag());
      }
    }
  }
}
