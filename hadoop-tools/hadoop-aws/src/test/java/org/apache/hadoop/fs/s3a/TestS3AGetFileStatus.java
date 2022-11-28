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

package org.apache.hadoop.fs.s3a;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;
import org.mockito.ArgumentMatcher;


/**
 * S3A tests for getFileStatus using mock S3 client.
 */
public class TestS3AGetFileStatus extends AbstractS3AMockTest {

  @Test
  public void testFile() throws Exception {
    Path path = new Path("/file");
    String key = path.toUri().getPath().substring(1);
    HeadObjectResponse objectMetadata =
        HeadObjectResponse.builder().contentLength(1L).lastModified(new Date(2L).toInstant())
            .build();
    when(s3.headObject(argThat(correctGetMetadataRequest(BUCKET, key)))).thenReturn(objectMetadata);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isFile());
    assertEquals(objectMetadata.contentLength().longValue(), stat.getLen());
    assertEquals(Date.from(objectMetadata.lastModified()).getTime(), stat.getModificationTime());
    ContractTestUtils.assertNotErasureCoded(fs, path);
    assertTrue(path + " should have erasure coding unset in " +
            "FileStatus#toString(): " + stat,
        stat.toString().contains("isErasureCoded=false"));
  }

  @Test
  public void testFakeDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.headObject(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    String keyDir = key + "/";
    List<S3Object> s3Objects = new ArrayList<>(1);
    s3Objects.add(S3Object.builder().key(keyDir).size(0L).build());
    ListObjectsV2Response listObjectsV2Response =
        ListObjectsV2Response.builder().contents(s3Objects).build();
    when(s3.listObjectsV2(argThat(
        matchListV2Request(BUCKET, keyDir))
    )).thenReturn(listObjectsV2Response);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
  }

  @Test
  public void testImplicitDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.headObject(argThat(correctGetMetadataRequest(BUCKET,  key))))
      .thenThrow(NOT_FOUND);
    when(s3.headObject(argThat(
      correctGetMetadataRequest(BUCKET, key + "/"))
    )).thenThrow(NOT_FOUND);
    setupListMocks(Collections.singletonList(CommonPrefix.builder().prefix("dir/").build()),
        Collections.emptyList());
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
    ContractTestUtils.assertNotErasureCoded(fs, path);
    assertTrue(path + " should have erasure coding unset in " +
            "FileStatus#toString(): " + stat,
        stat.toString().contains("isErasureCoded=false"));
  }

  @Test
  public void testRoot() throws Exception {
    Path path = new Path("/");
    String key = path.toUri().getPath().substring(1);
    when(s3.headObject(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    when(s3.headObject(argThat(
      correctGetMetadataRequest(BUCKET, key + "/")
    ))).thenThrow(NOT_FOUND);
    setupListMocks(Collections.emptyList(), Collections.emptyList());
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
    assertTrue(stat.getPath().isRoot());
  }

  @Test
  public void testNotFound() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.headObject(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    when(s3.headObject(argThat(
      correctGetMetadataRequest(BUCKET, key + "/")
    ))).thenThrow(NOT_FOUND);
    setupListMocks(Collections.emptyList(), Collections.emptyList());
    exception.expect(FileNotFoundException.class);
    fs.getFileStatus(path);
  }

  private void setupListMocks(List<CommonPrefix> prefixes,
      List<S3Object> s3Objects) {
    // V1 list API mock
    ListObjectsResponse v1Response = ListObjectsResponse.builder()
        .commonPrefixes(prefixes)
        .contents(s3Objects)
        .build();
    when(s3.listObjects(any(ListObjectsRequest.class))).thenReturn(v1Response);

    // V2 list API mock
    ListObjectsV2Response v2Result = ListObjectsV2Response.builder()
        .commonPrefixes(prefixes)
        .contents(s3Objects)
        .build();
    when(s3.listObjectsV2(
        any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class))).thenReturn(
        v2Result);
  }

  private ArgumentMatcher<HeadObjectRequest> correctGetMetadataRequest(
      String bucket, String key) {
    return request -> request != null
        && request.bucket().equals(bucket)
        && request.key().equals(key);
  }

  private ArgumentMatcher<ListObjectsV2Request> matchListV2Request(
      String bucket, String key) {
    return (ListObjectsV2Request request) -> {
      return request != null
          && request.bucket().equals(bucket)
          && request.prefix().equals(key);
    };
  }

}
