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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Date;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

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
    when(s3.getObjectMetadata(BUCKET, key)).thenReturn(meta);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isFile());
    assertEquals(meta.getContentLength(), stat.getLen());
    assertEquals(meta.getLastModified().getTime(), stat.getModificationTime());
  }

  @Test
  public void testFakeDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.getObjectMetadata(BUCKET, key)).thenThrow(NOT_FOUND);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(0L);
    when(s3.getObjectMetadata(BUCKET, key + "/")).thenReturn(meta);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
  }

  @Test
  public void testImplicitDirectory() throws Exception {
    Path path = new Path("/dir");
    String key = path.toUri().getPath().substring(1);
    when(s3.getObjectMetadata(BUCKET, key)).thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(BUCKET, key + "/")).thenThrow(NOT_FOUND);
    ObjectListing objects = mock(ObjectListing.class);
    when(objects.getCommonPrefixes()).thenReturn(
        Collections.singletonList("dir/"));
    when(objects.getObjectSummaries()).thenReturn(
        Collections.<S3ObjectSummary>emptyList());
    when(s3.listObjects(any(ListObjectsRequest.class))).thenReturn(objects);
    FileStatus stat = fs.getFileStatus(path);
    assertNotNull(stat);
    assertEquals(fs.makeQualified(path), stat.getPath());
    assertTrue(stat.isDirectory());
  }

  @Test
  public void testRoot() throws Exception {
    Path path = new Path("/");
    String key = path.toUri().getPath().substring(1);
    when(s3.getObjectMetadata(BUCKET, key)).thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(BUCKET, key + "/")).thenThrow(NOT_FOUND);
    ObjectListing objects = mock(ObjectListing.class);
    when(objects.getCommonPrefixes()).thenReturn(
        Collections.<String>emptyList());
    when(objects.getObjectSummaries()).thenReturn(
        Collections.<S3ObjectSummary>emptyList());
    when(s3.listObjects(any(ListObjectsRequest.class))).thenReturn(objects);
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
    when(s3.getObjectMetadata(BUCKET, key)).thenThrow(NOT_FOUND);
    when(s3.getObjectMetadata(BUCKET, key + "/")).thenThrow(NOT_FOUND);
    ObjectListing objects = mock(ObjectListing.class);
    when(objects.getCommonPrefixes()).thenReturn(
        Collections.<String>emptyList());
    when(objects.getObjectSummaries()).thenReturn(
        Collections.<S3ObjectSummary>emptyList());
    when(s3.listObjects(any(ListObjectsRequest.class))).thenReturn(objects);
    exception.expect(FileNotFoundException.class);
    fs.getFileStatus(path);
  }
}
