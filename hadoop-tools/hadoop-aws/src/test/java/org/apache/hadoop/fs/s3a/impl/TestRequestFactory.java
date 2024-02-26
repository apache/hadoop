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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AWSRequestAnalyzer;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.test.AbstractHadoopTestBase;

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
                "kms:key"))
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

    Assertions.assertThat(factory.newPutObjectRequestBuilder(path, null, 128, false)
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
        new ArrayList<>()));
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

    a(factory.newUploadPartRequestBuilder(path, id, 1, 0));
    a(factory.newUploadPartRequestBuilder(path, id, 2, 128_000_000));
    // partNumber is past the limit
    intercept(PathIOException.class, () ->
        factory.newUploadPartRequestBuilder(path, id, 3, 128_000_000));

    assertThat(countRequests.counter.get())
        .describedAs("request preparation count")
        .isEqualTo(requestsAnalyzed);
  }

}
