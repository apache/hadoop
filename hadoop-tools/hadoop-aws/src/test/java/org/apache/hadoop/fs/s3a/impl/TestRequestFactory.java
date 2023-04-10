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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Count of requests analyzed via the {@link #a(AmazonWebServiceRequest)}
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
    CannedAccessControlList acl = CannedAccessControlList.BucketOwnerFullControl;
    RequestFactory factory = RequestFactoryImpl.builder()
        .withBucket("bucket")
        .withCannedACL(acl)
        .build();
    String path = "path";
    String path2 = "path2";
    ObjectMetadata md = factory.newObjectMetadata(128);
    Assertions.assertThat(
            factory.newPutObjectRequest(path, md,
                    null, new ByteArrayInputStream(new byte[0]))
                .getCannedAcl())
        .describedAs("ACL of PUT")
        .isEqualTo(acl);
    Assertions.assertThat(factory.newCopyObjectRequest(path, path2, md)
            .getCannedAccessControlList())
        .describedAs("ACL of COPY")
        .isEqualTo(acl);
    Assertions.assertThat(factory.newMultipartUploadRequest(path,
                null)
            .getCannedACL())
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
    public <T extends AmazonWebServiceRequest> T prepareRequest(final T t) {
      counter.addAndGet(1);
      return t;
    }
  }

  /**
   * Analyze the request, log the output, return the info.
   * @param request request.
   * @param <T> type of request.
   * @return value
   */
  private <T extends AmazonWebServiceRequest> AWSRequestAnalyzer.RequestInfo
      a(T request) {
    AWSRequestAnalyzer.RequestInfo info = analyzer.analyze(request);
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
    ObjectMetadata md = factory.newObjectMetadata(128);
    a(factory.newAbortMultipartUploadRequest(path, id));
    a(factory.newCompleteMultipartUploadRequest(path, id,
        new ArrayList<>()));
    a(factory.newCopyObjectRequest(path, path2, md));
    a(factory.newDeleteObjectRequest(path));
    a(factory.newBulkDeleteRequest(new ArrayList<>()));
    a(factory.newDirectoryMarkerRequest(path));
    a(factory.newGetObjectRequest(path));
    a(factory.newGetObjectMetadataRequest(path));
    a(factory.newListMultipartUploadsRequest(path));
    a(factory.newListObjectsV1Request(path, "/", 1));
    a(factory.newListNextBatchOfObjectsRequest(new ObjectListing()));
    a(factory.newListObjectsV2Request(path, "/", 1));
    a(factory.newMultipartUploadRequest(path, null));
    File srcfile = new File("/tmp/a");
    a(factory.newPutObjectRequest(path,
        factory.newObjectMetadata(-1), null, srcfile));
    ByteArrayInputStream stream = new ByteArrayInputStream(new byte[0]);
    a(factory.newPutObjectRequest(path, md, null, stream));
    a(factory.newSelectRequest(path));
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
        .build();

    String path = "path";
    String path2 = "path2";
    String id = "1";
    File srcfile = File.createTempFile("file", "");
    try {
      ByteArrayInputStream stream = new ByteArrayInputStream(new byte[0]);

      a(factory.newUploadPartRequest(path, id, 1, 0, stream, null, 0));
      a(factory.newUploadPartRequest(path, id, 2, 128_000_000,
          null, srcfile, 0));
      // offset is past the EOF
      intercept(IllegalArgumentException.class, () ->
          factory.newUploadPartRequest(path, id, 3, 128_000_000,
              null, srcfile, 128));
    } finally {
      srcfile.delete();
    }
    assertThat(countRequests.counter.get())
        .describedAs("request preparation count")
        .isEqualTo(requestsAnalyzed);
  }

}
