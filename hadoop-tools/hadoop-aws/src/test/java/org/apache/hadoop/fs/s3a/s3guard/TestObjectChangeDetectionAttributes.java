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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3AMockTest;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_SERVER;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE_ETAG;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE_VERSION_ID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Unit tests to ensure object eTag and versionId are captured on S3 PUT and
 * used on GET.
 * Further (integration) testing is performed in
 * {@link org.apache.hadoop.fs.s3a.ITestS3ARemoteFileChanged}.
 */
@RunWith(Parameterized.class)
public class TestObjectChangeDetectionAttributes extends AbstractS3AMockTest {
  private final String changeDetectionSource;

  public TestObjectChangeDetectionAttributes(String changeDetectionSource) {
    this.changeDetectionSource = changeDetectionSource;
  }

  @Parameterized.Parameters(name = "change={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {CHANGE_DETECT_SOURCE_ETAG},
        {CHANGE_DETECT_SOURCE_VERSION_ID}
    });
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setClass(Constants.S3_METADATA_STORE_IMPL,
        LocalMetadataStore.class, MetadataStore.class);
    conf.set(CHANGE_DETECT_SOURCE, changeDetectionSource);
    conf.set(CHANGE_DETECT_MODE, CHANGE_DETECT_MODE_SERVER);
    return conf;
  }

  /**
   * Tests a file uploaded with a single PUT to ensure eTag is captured and used
   * on file read.
   */
  @Test
  public void testCreateAndReadFileSinglePart() throws Exception {
    String bucket = "s3a://mock-bucket/";
    String file = "single-part-file";
    Path path = new Path(bucket, file);
    byte[] content = "content".getBytes();
    String eTag = "abc";
    String versionId = "def";

    putObject(file, path, content, eTag, versionId);

    // make sure the eTag and versionId were put into the metadataStore
    assertVersionAttributes(path, eTag, versionId);

    // Ensure underlying S3 getObject call uses the stored eTag or versionId
    // when reading data back.  If it doesn't, the read won't work and the
    // assert will fail.
    assertContent(file, path, content, eTag, versionId);

    // test overwrite
    byte[] newConent = "newcontent".getBytes();
    String newETag = "newETag";
    String newVersionId = "newVersionId";

    putObject(file, path, newConent, newETag, newVersionId);
    assertVersionAttributes(path, newETag, newVersionId);
    assertContent(file, path, newConent, newETag, newVersionId);
  }

  /**
   * Tests a file uploaded with multi-part upload to ensure eTag is captured
   * and used on file read.
   */
  @Test
  public void testCreateAndReadFileMultiPart() throws Exception {
    String bucket = "s3a://mock-bucket/";
    String file = "multi-part-file";
    Path path = new Path(bucket, file);
    byte[] content = new byte[Constants.MULTIPART_MIN_SIZE + 1];
    String eTag = "abc";
    String versionId = "def";

    multipartUpload(file, path, content, eTag, versionId);

    // make sure the eTag and versionId were put into the metadataStore
    assertVersionAttributes(path, eTag, versionId);

    // Ensure underlying S3 getObject call uses the stored eTag or versionId
    // when reading data back.  If it doesn't, the read won't work and the
    // assert will fail.
    assertContent(file, path, content, eTag, versionId);

    // test overwrite
    byte[] newContent = new byte[Constants.MULTIPART_MIN_SIZE + 1];
    Arrays.fill(newContent, (byte) 1);
    String newETag = "newETag";
    String newVersionId = "newVersionId";

    multipartUpload(file, path, newContent, newETag, newVersionId);
    assertVersionAttributes(path, newETag, newVersionId);
    assertContent(file, path, newContent, newETag, newVersionId);
  }

  private void putObject(String file, Path path, byte[] content,
      String eTag, String versionId) throws IOException {
    PutObjectResult putObjectResult = new PutObjectResult();
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(content.length);
    putObjectResult.setMetadata(objectMetadata);
    putObjectResult.setETag(eTag);
    putObjectResult.setVersionId(versionId);

    when(s3.getObjectMetadata(any(GetObjectMetadataRequest.class)))
        .thenThrow(NOT_FOUND);
    when(s3.putObject(argThat(correctPutObjectRequest(file))))
        .thenReturn(putObjectResult);
    ListObjectsV2Result emptyListing = new ListObjectsV2Result();
    when(s3.listObjectsV2(argThat(correctListObjectsRequest(file + "/"))))
        .thenReturn(emptyListing);

    FSDataOutputStream outputStream = fs.create(path);
    outputStream.write(content);
    outputStream.close();
  }

  private void multipartUpload(String file, Path path, byte[] content,
      String eTag, String versionId) throws IOException {
    CompleteMultipartUploadResult uploadResult =
        new CompleteMultipartUploadResult();
    uploadResult.setVersionId(versionId);

    when(s3.getObjectMetadata(any(GetObjectMetadataRequest.class)))
        .thenThrow(NOT_FOUND);

    InitiateMultipartUploadResult initiateMultipartUploadResult =
        new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("uploadId");
    when(s3.initiateMultipartUpload(
        argThat(correctInitiateMultipartUploadRequest(file))))
        .thenReturn(initiateMultipartUploadResult);

    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setETag("partETag");
    when(s3.uploadPart(argThat(correctUploadPartRequest(file))))
        .thenReturn(uploadPartResult);

    CompleteMultipartUploadResult multipartUploadResult =
        new CompleteMultipartUploadResult();
    multipartUploadResult.setETag(eTag);
    multipartUploadResult.setVersionId(versionId);
    when(s3.completeMultipartUpload(
        argThat(correctMultipartUploadRequest(file))))
        .thenReturn(multipartUploadResult);

    ListObjectsV2Result emptyListing = new ListObjectsV2Result();
    when(s3.listObjectsV2(argThat(correctListObjectsRequest(file + "/"))))
        .thenReturn(emptyListing);

    FSDataOutputStream outputStream = fs.create(path);
    outputStream.write(content);
    outputStream.close();
  }

  private void assertContent(String file, Path path, byte[] content,
      String eTag, String versionId) throws IOException {
    S3Object s3Object = new S3Object();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setHeader(Headers.S3_VERSION_ID, versionId);
    metadata.setHeader(Headers.ETAG, eTag);
    s3Object.setObjectMetadata(metadata);
    s3Object.setObjectContent(new ByteArrayInputStream(content));
    when(s3.getObject(argThat(correctGetObjectRequest(file, eTag, versionId))))
        .thenReturn(s3Object);
    FSDataInputStream inputStream = fs.open(path);
    byte[] readContent = IOUtils.toByteArray(inputStream);
    assertArrayEquals(content, readContent);
  }

  private void assertVersionAttributes(Path path, String eTag, String versionId)
      throws IOException {
    MetadataStore metadataStore = fs.getMetadataStore();
    PathMetadata pathMetadata = metadataStore.get(path);
    assertNotNull(pathMetadata);
    S3AFileStatus fileStatus = pathMetadata.getFileStatus();
    assertEquals(eTag, fileStatus.getETag());
    assertEquals(versionId, fileStatus.getVersionId());
  }

  private Matcher<GetObjectRequest> correctGetObjectRequest(final String key,
      final String eTag, final String versionId) {
    return new BaseMatcher<GetObjectRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof GetObjectRequest) {
          GetObjectRequest getObjectRequest = (GetObjectRequest) item;
          if (getObjectRequest.getKey().equals(key)) {
            if (changeDetectionSource.equals(
                CHANGE_DETECT_SOURCE_ETAG)) {
              return getObjectRequest.getMatchingETagConstraints()
                  .contains(eTag);
            } else if (changeDetectionSource.equals(
                CHANGE_DETECT_SOURCE_VERSION_ID)) {
              return getObjectRequest.getVersionId().equals(versionId);
            }
          }
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key and "
            + changeDetectionSource
            + " matches");
      }
    };
  }

  private Matcher<UploadPartRequest> correctUploadPartRequest(
      final String key) {
    return new BaseMatcher<UploadPartRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof UploadPartRequest) {
          UploadPartRequest request = (UploadPartRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<InitiateMultipartUploadRequest>
      correctInitiateMultipartUploadRequest(final String key) {
    return new BaseMatcher<InitiateMultipartUploadRequest>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }

      @Override
      public boolean matches(Object item) {
        if (item instanceof InitiateMultipartUploadRequest) {
          InitiateMultipartUploadRequest request =
              (InitiateMultipartUploadRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }
    };
  }

  private Matcher<CompleteMultipartUploadRequest>
      correctMultipartUploadRequest(final String key) {
    return new BaseMatcher<CompleteMultipartUploadRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof CompleteMultipartUploadRequest) {
          CompleteMultipartUploadRequest request =
              (CompleteMultipartUploadRequest) item;
          return request.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<ListObjectsV2Request> correctListObjectsRequest(
      final String key) {
    return new BaseMatcher<ListObjectsV2Request>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof ListObjectsV2Request) {
          ListObjectsV2Request listObjectsRequest =
              (ListObjectsV2Request) item;
          return listObjectsRequest.getPrefix().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }

  private Matcher<PutObjectRequest> correctPutObjectRequest(
      final String key) {
    return new BaseMatcher<PutObjectRequest>() {
      @Override
      public boolean matches(Object item) {
        if (item instanceof PutObjectRequest) {
          PutObjectRequest putObjectRequest = (PutObjectRequest) item;
          return putObjectRequest.getKey().equals(key);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("key matches");
      }
    };
  }
}
