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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

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
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(1L);
    meta.setLastModified(new Date(2L));
    when(s3.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenReturn(meta);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isFile());
    assertEquals(meta.getContentLength(), stat.getLen());
    assertEquals(meta.getLastModified().getTime(), stat.getModificationTime());
    ContractTestUtils.assertNotErasureCoded(fs, path);
    assertTrue(path + " should have erasure coding unset in " +
            "FileStatus#toString(): " + stat,
        stat.toString().contains("isErasureCoded=false"));
  }

  @Test
  public void testFakeDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(0L);
    when(s3.getObjectMetadata(argThat(
        correctGetMetadataRequest(BUCKET, key + "/"))
    )).thenReturn(meta);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
  }

  @Test
  public void testImplicitDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET,  key))))
      .thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(argThat(
      correctGetMetadataRequest(BUCKET, key + "/"))
    )).thenThrow(NOT_FOUND);
    setupListMocks(Collections.singletonList("dir/"), Collections.emptyList());
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
    when(s3.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(argThat(
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
    when(s3.getObjectMetadata(argThat(correctGetMetadataRequest(BUCKET, key))))
      .thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(argThat(
      correctGetMetadataRequest(BUCKET, key + "/")
    ))).thenThrow(NOT_FOUND);
    setupListMocks(Collections.emptyList(), Collections.emptyList());
    exception.expect(FileNotFoundException.class);
    fs.getFileStatus(path);
  }

  private void setupListMocks(List<String> prefixes,
      List<S3ObjectSummary> summaries) {

    // V1 list API mock
    ObjectListing objects = mock(ObjectListing.class);
    when(objects.getCommonPrefixes()).thenReturn(prefixes);
    when(objects.getObjectSummaries()).thenReturn(summaries);
    when(s3.listObjects(any(ListObjectsRequest.class))).thenReturn(objects);

    // V2 list API mock
    ListObjectsV2Result v2Result = mock(ListObjectsV2Result.class);
    when(v2Result.getCommonPrefixes()).thenReturn(prefixes);
    when(v2Result.getObjectSummaries()).thenReturn(summaries);
    when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(v2Result);
  }

  private ArgumentMatcher<GetObjectMetadataRequest> correctGetMetadataRequest(
      String bucket, String key) {
    return request -> request != null
        && request.getBucketName().equals(bucket)
        && request.getKey().equals(key);
  }
}
