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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_METADATASTORE_METADATA_TTL;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_METADATA_TTL;
import static org.apache.hadoop.fs.s3a.Listing.toProvidedFileStatusIterator;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.dirMetaToStatuses;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link S3Guard} utility class.
 */
public class TestS3Guard extends Assert {

  public static final String MS_FILE_1 = "s3a://bucket/dir/ms-file1";

  public static final String MS_FILE_2 = "s3a://bucket/dir/ms-file2";

  public static final String S3_FILE_3 = "s3a://bucket/dir/s3-file3";

  public static final String S3_DIR_4 = "s3a://bucket/dir/s3-dir4";

  public static final Path DIR_PATH = new Path("s3a://bucket/dir");

  private MetadataStore ms;

  private ITtlTimeProvider timeProvider;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration(false);
    ms = new LocalMetadataStore();
    ms.initialize(conf, new S3Guard.TtlTimeProvider(conf));
    timeProvider = new S3Guard.TtlTimeProvider(
        DEFAULT_METADATASTORE_METADATA_TTL);
  }

  @After
  public void tearDown() throws Exception {
    if (ms != null) {
       ms.destroy();
    }
  }

  /**
   * Basic test to ensure results from S3 and MetadataStore are merged
   * correctly.
   */
  @Test
  public void testDirListingUnionNonauth() throws Exception {

    // Two files in metadata store listing
    PathMetadata m1 = makePathMeta(MS_FILE_1, false);
    PathMetadata m2 = makePathMeta(MS_FILE_2, false);
    DirListingMetadata dirMeta = new DirListingMetadata(DIR_PATH,
        Arrays.asList(m1, m2), false);

    // Two other entries in s3
    final S3AFileStatus s1Status = makeFileStatus(S3_FILE_3, false);
    final S3AFileStatus s2Status = makeFileStatus(S3_DIR_4, true);
    List<S3AFileStatus> s3Listing = Arrays.asList(
        s1Status,
        s2Status);
    RemoteIterator<S3AFileStatus> storeItr = toProvidedFileStatusIterator(
            s3Listing.toArray(new S3AFileStatus[0]));
    RemoteIterator<S3AFileStatus> resultItr = S3Guard.dirListingUnion(
            ms, DIR_PATH, storeItr, dirMeta, false,
            timeProvider, s3AFileStatuses ->
                    toProvidedFileStatusIterator(dirMetaToStatuses(dirMeta)));
    S3AFileStatus[] result = S3AUtils.iteratorToStatuses(
            resultItr, new HashSet<>());

    assertEquals("listing length", 4, result.length);
    assertContainsPaths(result, MS_FILE_1, MS_FILE_2, S3_FILE_3, S3_DIR_4);

    // check the MS doesn't contain the s3 entries as nonauth
    // unions should block them
    assertNoRecord(ms, S3_FILE_3);
    assertNoRecord(ms, S3_DIR_4);

    // for entries which do exist, when updated in S3, the metastore is updated
    S3AFileStatus f1Status2 = new S3AFileStatus(
        200, System.currentTimeMillis(), new Path(MS_FILE_1),
        1, null, "tag2", "ver2");
    S3AFileStatus[] f1Statuses = new S3AFileStatus[1];
    f1Statuses[0] = f1Status2;
    RemoteIterator<S3AFileStatus> itr = toProvidedFileStatusIterator(
            f1Statuses);
    FileStatus[] result2 = S3AUtils.iteratorToStatuses(
            S3Guard.dirListingUnion(
                    ms, DIR_PATH, itr, dirMeta,
                    false, timeProvider,
              s3AFileStatuses ->
                      toProvidedFileStatusIterator(
                              dirMetaToStatuses(dirMeta))),
            new HashSet<>());
    // the listing returns the new status
    Assertions.assertThat(find(result2, MS_FILE_1))
        .describedAs("Entry in listing results for %s", MS_FILE_1)
        .isSameAs(f1Status2);
    // as does a query of the MS
    final PathMetadata updatedMD = verifyRecord(ms, MS_FILE_1);
    Assertions.assertThat(updatedMD.getFileStatus())
        .describedAs("Entry in metastore for %s: %s", MS_FILE_1, updatedMD)
        .isEqualTo(f1Status2);
  }

  /**
   * Auth mode unions are different.
   */
  @Test
  public void testDirListingUnionAuth() throws Exception {

    // Two files in metadata store listing
    PathMetadata m1 = makePathMeta(MS_FILE_1, false);
    PathMetadata m2 = makePathMeta(MS_FILE_2, false);
    DirListingMetadata dirMeta = new DirListingMetadata(DIR_PATH,
        Arrays.asList(m1, m2), true);

    // Two other entries in s3
    S3AFileStatus s1Status = makeFileStatus(S3_FILE_3, false);
    S3AFileStatus s2Status = makeFileStatus(S3_DIR_4, true);
    List<S3AFileStatus> s3Listing = Arrays.asList(
        s1Status,
        s2Status);

    ITtlTimeProvider timeProvider = new S3Guard.TtlTimeProvider(
        DEFAULT_METADATASTORE_METADATA_TTL);

    RemoteIterator<S3AFileStatus> storeItr = toProvidedFileStatusIterator(
            s3Listing.toArray(new S3AFileStatus[0]));
    RemoteIterator<S3AFileStatus> resultItr = S3Guard
            .dirListingUnion(ms, DIR_PATH, storeItr, dirMeta,
                    true, timeProvider,
              s3AFileStatuses ->
                      toProvidedFileStatusIterator(
                              dirMetaToStatuses(dirMeta)));

    S3AFileStatus[] result = S3AUtils.iteratorToStatuses(
            resultItr, new HashSet<>());
    assertEquals("listing length", 4, result.length);
    assertContainsPaths(result, MS_FILE_1, MS_FILE_2, S3_FILE_3, S3_DIR_4);

    // now verify an auth scan added the records
    PathMetadata file3Meta = verifyRecord(ms, S3_FILE_3);
    PathMetadata dir4Meta = verifyRecord(ms, S3_DIR_4);

    // we can't check auth flag handling because local FS doesn't have one
    // so do just check the dir status still all good.
    Assertions.assertThat(dir4Meta)
        .describedAs("Metastore entry for dir %s", dir4Meta)
        .matches(m -> m.getFileStatus().isDirectory());

    DirListingMetadata dirMeta2 = new DirListingMetadata(DIR_PATH,
        Arrays.asList(m1, m2, file3Meta, dir4Meta), true);
    // now s1 status is updated on S3
    S3AFileStatus s1Status2 = new S3AFileStatus(
        200, System.currentTimeMillis(), new Path(S3_FILE_3),
        1, null, "tag2", "ver2");
    S3AFileStatus[] f1Statuses = new S3AFileStatus[1];
    f1Statuses[0] = s1Status2;
    RemoteIterator<S3AFileStatus> itr =
            toProvidedFileStatusIterator(f1Statuses);
    FileStatus[] result2 = S3AUtils.iteratorToStatuses(
            S3Guard.dirListingUnion(ms, DIR_PATH, itr, dirMeta,
                    true, timeProvider,
              s3AFileStatuses ->
                      toProvidedFileStatusIterator(
                              dirMetaToStatuses(dirMeta))),
            new HashSet<>());

    // but the result of the listing contains the old entry
    // because auth mode doesn't pick up changes in S3 which
    // didn't go through s3guard
    Assertions.assertThat(find(result2, S3_FILE_3))
        .describedAs("Entry in listing results for %s", S3_FILE_3)
        .isSameAs(file3Meta.getFileStatus());
  }

  /**
   * Assert there is no record in the store.
   * @param ms metastore
   * @param path path
   * @throws IOException IOError
   */
  private void assertNoRecord(MetadataStore ms, String path)
      throws IOException {
    Assertions.assertThat(lookup(ms, path))
        .describedAs("Metastore entry for %s", path)
        .isNull();
  }

  /**
   * Assert there is arecord in the store, then return it.
   * @param ms metastore
   * @param path path
   * @return the record.
   * @throws IOException IO Error
   */
  private PathMetadata verifyRecord(MetadataStore ms, String path)
     throws IOException {
   final PathMetadata md = lookup(ms, path);
   Assertions.assertThat(md)
       .describedAs("Metastore entry for %s", path)
       .isNotNull();
   return md;
  }

  /**
   * Look up a record.
   * @param ms store
   * @param path path
   * @return the record or null
   * @throws IOException IO Error
   */
  private PathMetadata lookup(final MetadataStore ms, final String path)
      throws IOException {
    return ms.get(new Path(path));
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
    S3Guard.putWithTtl(ms, dlm, Collections.emptyList(), timeProvider, null);

    // assert
    assertEquals("last update in " + dlm, 100L, dlm.getLastUpdated());
    verify(timeProvider, times(1)).getNow();
    verify(ms, times(1)).put(dlm, Collections.emptyList(), null);
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
        false, false);

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
        S3Guard.getWithTtl(ms, path, timeProvider, false, false);

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
        false, false);

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

  @Test
  public void testLogS3GuardDisabled() throws Exception {
    final Logger localLogger = LoggerFactory.getLogger(
        TestS3Guard.class.toString() + UUID.randomUUID());
    S3Guard.logS3GuardDisabled(localLogger,
        S3Guard.DisabledWarnLevel.SILENT.toString(), "bucket");
    S3Guard.logS3GuardDisabled(localLogger,
        S3Guard.DisabledWarnLevel.INFORM.toString(), "bucket");
    S3Guard.logS3GuardDisabled(localLogger,
        S3Guard.DisabledWarnLevel.WARN.toString(), "bucket");

    // Test that lowercase setting is accepted
    S3Guard.logS3GuardDisabled(localLogger,
        S3Guard.DisabledWarnLevel.WARN.toString()
            .toLowerCase(Locale.US), "bucket");

    ExitUtil.ExitException ex = LambdaTestUtils.intercept(
        ExitUtil.ExitException.class,
        String.format(S3Guard.DISABLED_LOG_MSG, "bucket"),
        () -> S3Guard.logS3GuardDisabled(
            localLogger, S3Guard.DisabledWarnLevel.FAIL.toString(), "bucket"));
    if (ex.getExitCode() != LauncherExitCodes.EXIT_BAD_CONFIGURATION) {
      throw ex;
    }
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        S3Guard.UNKNOWN_WARN_LEVEL,
        () -> S3Guard.logS3GuardDisabled(
            localLogger, "FOO_BAR_LEVEL", "bucket"));
  }

  void assertContainsPaths(FileStatus[] statuses, String...pathStr) {
    for (String s :pathStr) {
      assertContainsPath(statuses, s);
    }
  }

  void assertContainsPath(FileStatus[] statuses, String pathStr) {
    find(statuses, pathStr);
  }

  /**
   * Look up an entry or raise an assertion
   * @param statuses list of statuses
   * @param pathStr path to search
   * @return the entry if found
   */
  private FileStatus find(FileStatus[] statuses, String pathStr) {
    for (FileStatus s : statuses) {
      if (s.getPath().toString().equals(pathStr)) {
        return s;
      }
    }
    // no match, fail meaningfully
    Assertions.assertThat(statuses)
        .anyMatch(s -> s.getPath().toString().equals(pathStr));
    return null;
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
