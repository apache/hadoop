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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_METADATASTORE_METADATA_TTL;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_METADATA_TTL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link S3Guard} utility class.
 */
public class TestS3Guard extends Assert {

  /**
   * Basic test to ensure results from S3 and MetadataStore are merged
   * correctly.
   */
  @Test
  public void testDirListingUnion() throws Exception {
    MetadataStore ms = new LocalMetadataStore();

    Path dirPath = new Path("s3a://bucket/dir");

    // Two files in metadata store listing
    PathMetadata m1 = makePathMeta("s3a://bucket/dir/ms-file1", false);
    PathMetadata m2 = makePathMeta("s3a://bucket/dir/ms-file2", false);
    DirListingMetadata dirMeta = new DirListingMetadata(dirPath,
        Arrays.asList(m1, m2), false);

    // Two other files in s3
    List<S3AFileStatus> s3Listing = Arrays.asList(
        makeFileStatus("s3a://bucket/dir/s3-file3", false),
        makeFileStatus("s3a://bucket/dir/s3-file4", false)
    );

    ITtlTimeProvider timeProvider = new S3Guard.TtlTimeProvider(
        DEFAULT_METADATASTORE_METADATA_TTL);
    FileStatus[] result = S3Guard.dirListingUnion(ms, dirPath, s3Listing,
        dirMeta, false, timeProvider);

    assertEquals("listing length", 4, result.length);
    assertContainsPath(result, "s3a://bucket/dir/ms-file1");
    assertContainsPath(result, "s3a://bucket/dir/ms-file2");
    assertContainsPath(result, "s3a://bucket/dir/s3-file3");
    assertContainsPath(result, "s3a://bucket/dir/s3-file4");
  }

  @Test
  public void testPutWithTtlDirListingMeta() throws Exception {
    // arrange
    DirListingMetadata dlm = new DirListingMetadata(new Path("/"), null,
        false);
    MetadataStore ms = spy(MetadataStore.class);
    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(100L);

    // act
    S3Guard.putWithTtl(ms, dlm, timeProvider, null);

    // assert
    assertEquals("last update in " + dlm, 100L, dlm.getLastUpdated());
    verify(timeProvider, times(1)).getNow();
    verify(ms, times(1)).put(dlm, null);
  }

  @Test
  public void testPutWithTtlFileMeta() throws Exception {
    // arrange
    S3AFileStatus fileStatus = mock(S3AFileStatus.class);
    when(fileStatus.getPath()).thenReturn(new Path("/"));
    PathMetadata pm = new PathMetadata(fileStatus);
    MetadataStore ms = spy(MetadataStore.class);
    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(100L);

    // act
    S3Guard.putWithTtl(ms, pm, timeProvider, null);

    // assert
    assertEquals("last update in " + pm, 100L, pm.getLastUpdated());
    verify(timeProvider, times(1)).getNow();
    verify(ms, times(1)).put(pm, null);
  }

  @Test
  public void testPutWithTtlCollection() throws Exception {
    // arrange
    S3AFileStatus fileStatus = mock(S3AFileStatus.class);
    when(fileStatus.getPath()).thenReturn(new Path("/"));
    Collection<PathMetadata> pmCollection = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      pmCollection.add(new PathMetadata(fileStatus));
    }
    MetadataStore ms = spy(MetadataStore.class);
    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(100L);

    // act
    S3Guard.putWithTtl(ms, pmCollection, timeProvider, null);

    // assert
    pmCollection.forEach(
        pm -> assertEquals(100L, pm.getLastUpdated())
    );
    verify(timeProvider, times(1)).getNow();
    verify(ms, times(1)).put(pmCollection, null);
  }

  @Test
  public void testGetWithTtlExpired() throws Exception {
    // arrange
    S3AFileStatus fileStatus = mock(S3AFileStatus.class);
    Path path = new Path("/file");
    when(fileStatus.getPath()).thenReturn(path);
    PathMetadata pm = new PathMetadata(fileStatus);
    pm.setLastUpdated(100L);

    MetadataStore ms = mock(MetadataStore.class);
    when(ms.get(path, false)).thenReturn(pm);

    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(101L);
    when(timeProvider.getMetadataTtl()).thenReturn(1L);

    // act
    final PathMetadata pmExpired = S3Guard.getWithTtl(ms, path, timeProvider,
        false);

    // assert
    assertNull(pmExpired);
  }

  @Test
  public void testGetWithTtlNotExpired() throws Exception {
    // arrange
    S3AFileStatus fileStatus = mock(S3AFileStatus.class);
    Path path = new Path("/file");
    when(fileStatus.getPath()).thenReturn(path);
    PathMetadata pm = new PathMetadata(fileStatus);
    pm.setLastUpdated(100L);

    MetadataStore ms = mock(MetadataStore.class);
    when(ms.get(path, false)).thenReturn(pm);

    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(101L);
    when(timeProvider.getMetadataTtl()).thenReturn(2L);

    // act
    final PathMetadata pmNotExpired =
        S3Guard.getWithTtl(ms, path, timeProvider, false);

    // assert
    assertNotNull(pmNotExpired);
  }

  @Test
  public void testGetWithZeroLastUpdatedNotExpired() throws Exception {
    // arrange
    S3AFileStatus fileStatus = mock(S3AFileStatus.class);
    Path path = new Path("/file");
    when(fileStatus.getPath()).thenReturn(path);
    PathMetadata pm = new PathMetadata(fileStatus);
    // we set 0 this time as the last updated: can happen eg. when we use an
    // old dynamo table
    pm.setLastUpdated(0L);

    MetadataStore ms = mock(MetadataStore.class);
    when(ms.get(path, false)).thenReturn(pm);

    ITtlTimeProvider timeProvider =
        mock(ITtlTimeProvider.class);
    when(timeProvider.getNow()).thenReturn(101L);
    when(timeProvider.getMetadataTtl()).thenReturn(2L);

    // act
    final PathMetadata pmExpired = S3Guard.getWithTtl(ms, path, timeProvider,
        false);

    // assert
    assertNotNull(pmExpired);
  }


  /**
   * Makes sure that all uses of TTL timeouts use a consistent time unit.
   * @throws Throwable failure
   */
  @Test
  public void testTTLConstruction() throws Throwable {
    // first one
    ITtlTimeProvider timeProviderExplicit = new S3Guard.TtlTimeProvider(
        DEFAULT_METADATASTORE_METADATA_TTL);

    // mirror the FS construction,
    // from a config guaranteed to be empty (i.e. the code defval)
    Configuration conf = new Configuration(false);
    long millitime = conf.getTimeDuration(METADATASTORE_METADATA_TTL,
        DEFAULT_METADATASTORE_METADATA_TTL, TimeUnit.MILLISECONDS);
    assertEquals(15 * 60_000, millitime);
    S3Guard.TtlTimeProvider fsConstruction = new S3Guard.TtlTimeProvider(
        millitime);
    assertEquals("explicit vs fs construction", timeProviderExplicit,
        fsConstruction);
    assertEquals("first and second constructor", timeProviderExplicit,
        new S3Guard.TtlTimeProvider(conf));
    // set the conf to a time without unit
    conf.setLong(METADATASTORE_METADATA_TTL,
        DEFAULT_METADATASTORE_METADATA_TTL);
    assertEquals("first and second time set through long", timeProviderExplicit,
        new S3Guard.TtlTimeProvider(conf));
    double timeInSeconds = DEFAULT_METADATASTORE_METADATA_TTL / 1000;
    double timeInMinutes = timeInSeconds / 60;
    String timeStr = String.format("%dm", (int) timeInMinutes);
    assertEquals(":wrong time in minutes from " + timeInMinutes,
        "15m", timeStr);
    conf.set(METADATASTORE_METADATA_TTL, timeStr);
    assertEquals("Time in millis as string from "
            + conf.get(METADATASTORE_METADATA_TTL),
        timeProviderExplicit,
        new S3Guard.TtlTimeProvider(conf));
  }

  void assertContainsPath(FileStatus[] statuses, String pathStr) {
    assertTrue("listing doesn't contain " + pathStr,
        containsPath(statuses, pathStr));
  }

  boolean containsPath(FileStatus[] statuses, String pathStr) {
    for (FileStatus s : statuses) {
      if (s.getPath().toString().equals(pathStr)) {
        return true;
      }
    }
    return false;
  }

  private PathMetadata makePathMeta(String pathStr, boolean isDir) {
    return new PathMetadata(makeFileStatus(pathStr, isDir));
  }

  private S3AFileStatus makeFileStatus(String pathStr, boolean isDir) {
    Path p = new Path(pathStr);
    S3AFileStatus fileStatus;
    if (isDir) {
      fileStatus = new S3AFileStatus(Tristate.UNKNOWN, p, null);
    } else {
      fileStatus = new S3AFileStatus(
          100, System.currentTimeMillis(), p, 1, null, null, null);
    }
    return fileStatus;
  }
}
