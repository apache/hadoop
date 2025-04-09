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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.Md5Utils;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PART_UPLOAD_TIMEOUT;
import static org.apache.hadoop.fs.s3a.impl.PutObjectOptions.defaultOptions;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that the request factory creates requests; factory
 * is is built with different options on different test cases.
 * Everything goes through {@link AWSRequestAnalyzer} to
 * verify it handles every example, and logged so that a manual
 * review of the output can show it is valid.
 */
public class TestRequestFactory extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRequestFactory.class);

  private final AWSRequestAnalyzer analyzer = new AWSRequestAnalyzer();

  /**
   * Count of requests analyzed via the {@link #a(AwsRequest.Builder)}
   * call.
   */
  private int requestsAnalyzed;

  /**
   * No preparer; encryption is set.
   */
  @Test
  public void testRequestFactoryWithEncryption() throws Throwable {
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withEncryptionSecrets(
            new EncryptionSecrets(S3AEncryptionMethods.SSE_KMS,
                "kms:key", ""))
        .build();
    createFactoryObjects(factory);
  }

  /**
   * Verify ACLs are passed from the factory to the requests.
   */
  @Test
  public void testRequestFactoryWithCannedACL() throws Throwable {
    String acl = "bucket-owner-full-control";
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withCannedACL(acl)
        .build();
    String path = "path";
    String path2 = "path2";
    HeadObjectResponse md = HeadObjectResponse.builder().contentLength(128L).build();

    Assertions.assertThat(factory.newPutObjectRequestBuilder(path,
                defaultOptions(), 128, false)
            .build()
            .acl()
            .toString())
        .describedAs("ACL of PUT")
        .isEqualTo(acl);
    Assertions.assertThat(factory.newCopyObjectRequestBuilder(path, path2, md)
            .build()
            .acl()
            .toString())
        .describedAs("ACL of COPY")
        .isEqualTo(acl);
    Assertions.assertThat(factory.newMultipartUploadRequestBuilder(path, null)
            .build()
            .acl()
            .toString())
        .describedAs("ACL of MPU")
        .isEqualTo(acl);
  }

  /**
   * Now add a processor and verify that it was invoked for
   * exactly as many requests as were analyzed.
   */
  @Test
  public void testRequestFactoryWithProcessor() throws Throwable {
    CountRequests countRequests = new CountRequests();
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withRequestPreparer(countRequests)
        .build();

    createFactoryObjects(factory);
    assertThat(countRequests.counter.get())
        .describedAs("request preparation count")
        .isEqualTo(requestsAnalyzed);
  }

  private final class CountRequests
      implements RequestFactoryImpl.PrepareRequest {

    private final AtomicLong counter = new AtomicLong();

    @Override
    public void prepareRequest(final SdkRequest.Builder t) {
      counter.addAndGet(1);
    }
  }

  /**
   * Analyze the request, log the output, return the info.
   * @param builder request builder.
   * @return value
   */
  private AWSRequestAnalyzer.RequestInfo a(AwsRequest.Builder builder) {
    AWSRequestAnalyzer.RequestInfo info = analyzer.analyze(builder.build());
    LOG.info("{}", info);
    requestsAnalyzed++;
    return info;
  }

  /**
   * Create objects through the factory.
   * @param factory factory
   */
  private void createFactoryObjects(RequestFactory factory) throws IOException {
    String path = "path";
    String path2 = "path2";
    String id = "1";
    a(factory.newAbortMultipartUploadRequestBuilder(path, id));
    a(factory.newCompleteMultipartUploadRequestBuilder(path, id,
        new ArrayList<>(), new PutObjectOptions(true,
            "some class",
            Collections.emptyMap(),
            EnumSet.noneOf(WriteObjectFlags.class),
            "")));
    a(factory.newCopyObjectRequestBuilder(path, path2,
        HeadObjectResponse.builder().build()));
    a(factory.newDeleteObjectRequestBuilder(path));
    a(factory.newBulkDeleteRequestBuilder(new ArrayList<>()));
    a(factory.newDirectoryMarkerRequest(path));
    a(factory.newGetObjectRequestBuilder(path));
    a(factory.newHeadObjectRequestBuilder(path));
    a(factory.newListMultipartUploadsRequestBuilder(path));
    a(factory.newListObjectsV1RequestBuilder(path, "/", 1));
    a(factory.newListObjectsV2RequestBuilder(path, "/", 1));
    a(factory.newMultipartUploadRequestBuilder(path, null));
    a(factory.newPutObjectRequestBuilder(path,
        PutObjectOptions.keepingDirs(), -1, true));
    a(factory.newPutObjectRequestBuilder(path,
        PutObjectOptions.deletingDirs(), 1024, false));
  }

  /**
   * Multiparts are special so test on their own.
   */
  @Test
  public void testMultipartUploadRequest() throws Throwable {
    CountRequests countRequests = new CountRequests();

    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withRequestPreparer(countRequests)
        .withMultipartPartCountLimit(2)
        .build();

    String path = "path";
    String id = "1";

    a(factory.newUploadPartRequestBuilder(path, id, 1, false, 0));
    a(factory.newUploadPartRequestBuilder(path, id, 2, false,
        128_000_000));
    // partNumber is past the limit
    intercept(PathIOException.class, () ->
        factory.newUploadPartRequestBuilder(path, id, 3, true,
            128_000_000));

    assertThat(countRequests.counter.get())
        .describedAs("request preparation count")
        .isEqualTo(requestsAnalyzed);
  }

  /**
   * Assertion for Request timeouts.
   * @param duration expected duration.
   * @param request request.
   */
  private void assertApiTimeouts(Duration duration, S3Request request) {
    Assertions.assertThat(request.overrideConfiguration())
        .describedAs("request %s", request)
        .isNotEmpty();
    final AwsRequestOverrideConfiguration override =
        request.overrideConfiguration().get();
    Assertions.assertThat(override.apiCallAttemptTimeout())
        .describedAs("apiCallAttemptTimeout")
        .hasValue(duration);
    Assertions.assertThat(override.apiCallTimeout())
        .describedAs("apiCallTimeout")
        .hasValue(duration);
  }

  /**
   * If not overridden timeouts are set to the default part upload timeout.
   */
  @Test
  public void testDefaultUploadTimeouts() throws Throwable {

    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withMultipartPartCountLimit(2)
        .build();
    final UploadPartRequest upload =
        factory.newUploadPartRequestBuilder("path", "id", 2,
            true, 128_000_000)
            .build();
    assertApiTimeouts(DEFAULT_PART_UPLOAD_TIMEOUT, upload);
  }

  /**
   * Verify that when upload request timeouts are set,
   * they are passed down.
   */
  @Test
  public void testUploadTimeouts() throws Throwable {
    Duration partDuration = Duration.ofDays(1);
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withPartUploadTimeout(partDuration)
        .build();

    String path = "path";

    // A simple PUT
    final PutObjectRequest put = factory.newPutObjectRequestBuilder(path,
        PutObjectOptions.defaultOptions(), 1024, false).build();
    assertApiTimeouts(partDuration, put);

    // multipart part
    final UploadPartRequest upload = factory.newUploadPartRequestBuilder(path,
        "1", 3, false, 128_000_000)
        .build();
    assertApiTimeouts(partDuration, upload);

  }

  @Test
  public void testRequestFactoryWithChecksumAlgorithmCRC32() throws IOException {
    testRequestFactoryWithChecksumAlgorithm(ChecksumAlgorithm.CRC32);
  }

  @Test
  public void testRequestFactoryWithChecksumAlgorithmCRC32C() throws IOException {
    testRequestFactoryWithChecksumAlgorithm(ChecksumAlgorithm.CRC32_C);
  }

  @Test
  public void testRequestFactoryWithChecksumAlgorithmSHA1() throws IOException {
    testRequestFactoryWithChecksumAlgorithm(ChecksumAlgorithm.SHA1);
  }

  @Test
  public void testRequestFactoryWithChecksumAlgorithmSHA256() throws IOException {
    testRequestFactoryWithChecksumAlgorithm(ChecksumAlgorithm.SHA256);
  }

  private void testRequestFactoryWithChecksumAlgorithm(ChecksumAlgorithm checksumAlgorithm)
      throws IOException {
    String path = "path";
    String path2 = "path2";
    HeadObjectResponse md = HeadObjectResponse.builder().contentLength(128L).build();

    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withChecksumAlgorithm(checksumAlgorithm)
        .build();
    createFactoryObjects(factory);

    final CopyObjectRequest copyObjectRequest = factory.newCopyObjectRequestBuilder(path,
            path2, md).build();
    Assertions.assertThat(copyObjectRequest.checksumAlgorithm()).isEqualTo(checksumAlgorithm);

    final PutObjectRequest putObjectRequest = factory.newPutObjectRequestBuilder(path,
        PutObjectOptions.keepingDirs(), 1024, false).build();
    Assertions.assertThat(putObjectRequest.checksumAlgorithm()).isEqualTo(checksumAlgorithm);

    final CreateMultipartUploadRequest multipartUploadRequest =
        factory.newMultipartUploadRequestBuilder(path, null).build();
    Assertions.assertThat(multipartUploadRequest.checksumAlgorithm()).isEqualTo(checksumAlgorithm);

    final UploadPartRequest uploadPartRequest = factory.newUploadPartRequestBuilder(path,
        "id", 2, true, 128_000_000).build();
    Assertions.assertThat(uploadPartRequest.checksumAlgorithm()).isEqualTo(checksumAlgorithm);
  }

  @Test
  public void testCompleteMultipartUploadRequestWithChecksumAlgorithmAndSSEC() throws IOException {
    final byte[] encryptionKey = "encryptionKey".getBytes(StandardCharsets.UTF_8);
    final String encryptionKeyBase64 = Base64.getEncoder()
        .encodeToString(encryptionKey);
    final String encryptionKeyMd5 = Md5Utils.md5AsBase64(encryptionKey);
    final EncryptionSecrets encryptionSecrets = new EncryptionSecrets(S3AEncryptionMethods.SSE_C,
        encryptionKeyBase64, null);
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withChecksumAlgorithm(ChecksumAlgorithm.CRC32_C)
        .withEncryptionSecrets(encryptionSecrets)
        .build();
    createFactoryObjects(factory);

    PutObjectOptions putObjectOptions = new PutObjectOptions(true,
            null,
            null,
            EnumSet.noneOf(WriteObjectFlags.class),
            null);

    final CompleteMultipartUploadRequest request =
        factory.newCompleteMultipartUploadRequestBuilder("path", "1", new ArrayList<>(), putObjectOptions)
            .build();
    Assertions.assertThat(request.sseCustomerAlgorithm())
        .isEqualTo(ServerSideEncryption.AES256.name());
    Assertions.assertThat(request.sseCustomerKey()).isEqualTo(encryptionKeyBase64);
    Assertions.assertThat(request.sseCustomerKeyMD5()).isEqualTo(encryptionKeyMd5);
  }
}
