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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.isMetadataStoreAuthoritative;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * These tests are testing the S3Guard TTL (time to live) features.
 */
@RunWith(Parameterized.class)
public class ITestS3GuardTtl extends AbstractS3ATestBase {

  private final boolean authoritative;

  /**
   * Test array for parameterized test runs.
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters(name = "auth={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true}, {false}
    });
  }

  /**
   * By changing the method name, the thread name is changed and
   * so you can see in the logs which mode is being tested.
   * @return a string to use for the thread namer.
   */
  @Override
  protected String getMethodName() {
    return super.getMethodName() +
        (authoritative ? "-auth" : "-nonauth");
  }

  public ITestS3GuardTtl(boolean authoritative) {
    this.authoritative = authoritative;
  }

  /**
   * Patch the configuration - this test needs disabled filesystem caching.
   * These tests modify the fs instance that would cause flaky tests.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(configuration);
    configuration =
        S3ATestUtils.prepareTestConfiguration(configuration);
    configuration.setBoolean(METADATASTORE_AUTHORITATIVE, authoritative);
    return configuration;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(getFileSystem().hasMetadataStore());
  }

  @Test
  public void testDirectoryListingAuthoritativeTtl() throws Exception {
    LOG.info("Authoritative mode: {}", authoritative);

    final S3AFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.hasMetadataStore());
    final MetadataStore ms = fs.getMetadataStore();

    Assume.assumeTrue("MetadataStore should be capable for authoritative "
            + "storage of directories to run this test.",
        metadataStorePersistsAuthoritativeBit(ms));

    Assume.assumeTrue("MetadataStore should be authoritative for this test",
        isMetadataStoreAuthoritative(getFileSystem().getConf()));

    ITtlTimeProvider mockTimeProvider =
        mock(ITtlTimeProvider.class);
    ITtlTimeProvider restoreTimeProvider = fs.getTtlTimeProvider();
    fs.setTtlTimeProvider(mockTimeProvider);
    when(mockTimeProvider.getNow()).thenReturn(100L);
    when(mockTimeProvider.getMetadataTtl()).thenReturn(1L);

    Path dir = path("ttl/");
    Path file = path("ttl/afile");

    try {
      fs.mkdirs(dir);
      touch(fs, file);

      // get an authoritative listing in ms
      fs.listStatus(dir);

      // check if authoritative
      DirListingMetadata dirListing =
          S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider, authoritative);
      assertTrue("Listing should be authoritative.",
          dirListing.isAuthoritative());
      // change the time, and assume it's not authoritative anymore
      // if the metadatastore is not authoritative.
      when(mockTimeProvider.getNow()).thenReturn(102L);
      dirListing = S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider,
          authoritative);
      if (authoritative) {
        assertTrue("Listing should be authoritative.",
            dirListing.isAuthoritative());
      } else {
        assertFalse("Listing should not be authoritative.",
            dirListing.isAuthoritative());
      }

      // get an authoritative listing in ms again - retain test
      fs.listStatus(dir);
      // check if authoritative
      dirListing = S3Guard.listChildrenWithTtl(ms, dir, mockTimeProvider,
          authoritative);
      assertTrue("Listing shoud be authoritative after listStatus.",
          dirListing.isAuthoritative());
    } finally {
      fs.delete(dir, true);
      fs.setTtlTimeProvider(restoreTimeProvider);
    }
  }

  @Test
  public void testFileMetadataExpiresTtl() throws Exception {
    LOG.info("Authoritative mode: {}", authoritative);

    Path fileExpire1 = path("expirettl-" + UUID.randomUUID());
    Path fileExpire2 = path("expirettl-" + UUID.randomUUID());
    Path fileRetain = path("expirettl-" + UUID.randomUUID());

    final S3AFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.hasMetadataStore());
    final MetadataStore ms = fs.getMetadataStore();

    ITtlTimeProvider mockTimeProvider = mock(ITtlTimeProvider.class);
    ITtlTimeProvider originalTimeProvider = fs.getTtlTimeProvider();

    try {
      fs.setTtlTimeProvider(mockTimeProvider);
      when(mockTimeProvider.getMetadataTtl()).thenReturn(5L);

      // set the time, so the fileExpire1 will expire
      when(mockTimeProvider.getNow()).thenReturn(100L);
      touch(fs, fileExpire1);
      // set the time, so fileExpire2 will expire
      when(mockTimeProvider.getNow()).thenReturn(101L);
      touch(fs, fileExpire2);
      // set the time, so fileRetain won't expire
      when(mockTimeProvider.getNow()).thenReturn(109L);
      touch(fs, fileRetain);
      final FileStatus origFileRetainStatus = fs.getFileStatus(fileRetain);
      // change time, so the first two file metadata is expired
      when(mockTimeProvider.getNow()).thenReturn(110L);

      // metadata is expired so this should refresh the metadata with
      // last_updated to the getNow() if the store is not authoritative
      final FileStatus fileExpire1Status = fs.getFileStatus(fileExpire1);
      assertNotNull(fileExpire1Status);
      if (authoritative) {
        assertEquals(100L, ms.get(fileExpire1).getLastUpdated());
      } else {
        assertEquals(110L, ms.get(fileExpire1).getLastUpdated());
      }

      // metadata is expired so this should refresh the metadata with
      // last_updated to the getNow() if the store is not authoritative
      final FileStatus fileExpire2Status = fs.getFileStatus(fileExpire2);
      assertNotNull(fileExpire2Status);
      if (authoritative) {
        assertEquals(101L, ms.get(fileExpire2).getLastUpdated());
      } else {
        assertEquals(110L, ms.get(fileExpire2).getLastUpdated());
      }

      final FileStatus fileRetainStatus = fs.getFileStatus(fileRetain);
      assertEquals("Modification time of these files should be equal.",
          origFileRetainStatus.getModificationTime(),
          fileRetainStatus.getModificationTime());
      assertNotNull(fileRetainStatus);
      assertEquals(109L, ms.get(fileRetain).getLastUpdated());
    } finally {
      fs.delete(fileExpire1, true);
      fs.delete(fileExpire2, true);
      fs.delete(fileRetain, true);
      fs.setTtlTimeProvider(originalTimeProvider);
    }
  }

  /**
   * create(tombstone file) must succeed irrespective of overwrite flag.
   */
  @Test
  public void testCreateOnTombstonedFileSucceeds() throws Exception {
    LOG.info("Authoritative mode: {}", authoritative);
    final S3AFileSystem fs = getFileSystem();

    String fileToTry = methodName + UUID.randomUUID().toString();

    final Path filePath = path(fileToTry);

    // Create a directory with
    ITtlTimeProvider mockTimeProvider = mock(ITtlTimeProvider.class);
    ITtlTimeProvider originalTimeProvider = fs.getTtlTimeProvider();

    try {
      fs.setTtlTimeProvider(mockTimeProvider);
      when(mockTimeProvider.getNow()).thenReturn(100L);
      when(mockTimeProvider.getMetadataTtl()).thenReturn(5L);

      // CREATE A FILE
      touch(fs, filePath);

      // DELETE THE FILE - TOMBSTONE
      fs.delete(filePath, true);

      // CREATE THE SAME FILE WITHOUT ERROR DESPITE THE TOMBSTONE
      touch(fs, filePath);

    } finally {
      fs.delete(filePath, true);
      fs.setTtlTimeProvider(originalTimeProvider);
    }
  }

  /**
   * create("parent has tombstone") must always succeed (We dont check the
   * parent), but after the file has been written, all entries up the tree
   * must be valid. That is: the putAncestor code will correct everything
   */
  @Test
  public void testCreateParentHasTombstone() throws Exception {
    LOG.info("Authoritative mode: {}", authoritative);
    final S3AFileSystem fs = getFileSystem();

    String dirToDelete = methodName + UUID.randomUUID().toString();
    String fileToTry = dirToDelete + "/theFileToTry";

    final Path dirPath = path(dirToDelete);
    final Path filePath = path(fileToTry);

    // Create a directory with
    ITtlTimeProvider mockTimeProvider = mock(ITtlTimeProvider.class);
    ITtlTimeProvider originalTimeProvider = fs.getTtlTimeProvider();

    try {
      fs.setTtlTimeProvider(mockTimeProvider);
      when(mockTimeProvider.getNow()).thenReturn(100L);
      when(mockTimeProvider.getMetadataTtl()).thenReturn(5L);

      // CREATE DIRECTORY
      fs.mkdirs(dirPath);

      // DELETE DIRECTORY
      fs.delete(dirPath, true);

      // WRITE TO DELETED DIRECTORY - SUCCESS
      touch(fs, filePath);

      // SET TIME SO METADATA EXPIRES
      when(mockTimeProvider.getNow()).thenReturn(110L);

      // WRITE TO DELETED DIRECTORY - SUCCESS
      touch(fs, filePath);

    } finally {
      fs.delete(filePath, true);
      fs.delete(dirPath, true);
      fs.setTtlTimeProvider(originalTimeProvider);
    }
  }

  /**
   * Test that listing of metadatas is filtered from expired items.
   */
  @Test
  public void testListingFilteredExpiredItems() throws Exception {
    LOG.info("Authoritative mode: {}", authoritative);
    final S3AFileSystem fs = getFileSystem();

    long oldTime = 100L;
    long newTime = 110L;
    long ttl = 9L;
    final String basedir = "testListingFilteredExpiredItems";
    final Path tombstonedPath = path(basedir + "/tombstonedPath");
    final Path baseDirPath = path(basedir);
    final List<Path> filesToCreate = new ArrayList<>();
    final MetadataStore ms = fs.getMetadataStore();

    for (int i = 0; i < 10; i++) {
      filesToCreate.add(path(basedir + "/file" + i));
    }

    ITtlTimeProvider mockTimeProvider = mock(ITtlTimeProvider.class);
    ITtlTimeProvider originalTimeProvider = fs.getTtlTimeProvider();

    try {
      fs.setTtlTimeProvider(mockTimeProvider);
      when(mockTimeProvider.getMetadataTtl()).thenReturn(ttl);

      // add and delete entry with the oldtime
      when(mockTimeProvider.getNow()).thenReturn(oldTime);
      touch(fs, tombstonedPath);
      fs.delete(tombstonedPath, false);

      // create items with newTime
      when(mockTimeProvider.getNow()).thenReturn(newTime);
      for (Path path : filesToCreate) {
        touch(fs, path);
      }

      // listing will contain the tombstone with oldtime
      when(mockTimeProvider.getNow()).thenReturn(oldTime);
      final DirListingMetadata fullDLM = getDirListingMetadata(ms, baseDirPath);
      List<Path> containedPaths = fullDLM.getListing().stream()
          .map(pm -> pm.getFileStatus().getPath())
          .collect(Collectors.toList());
      Assertions.assertThat(containedPaths)
          .describedAs("Full listing of path %s", baseDirPath)
          .hasSize(11)
          .contains(tombstonedPath);

      // listing will be filtered if the store is not authoritative,
      // and won't contain the tombstone with oldtime
      when(mockTimeProvider.getNow()).thenReturn(newTime);
      final DirListingMetadata filteredDLM = S3Guard.listChildrenWithTtl(
          ms, baseDirPath, mockTimeProvider, authoritative);
      containedPaths = filteredDLM.getListing().stream()
          .map(pm -> pm.getFileStatus().getPath())
          .collect(Collectors.toList());
      if (authoritative) {
        Assertions.assertThat(containedPaths)
            .describedAs("Full listing of path %s", baseDirPath)
            .hasSize(11)
            .contains(tombstonedPath);
      } else {
        Assertions.assertThat(containedPaths)
            .describedAs("Full listing of path %s", baseDirPath)
            .hasSize(10)
            .doesNotContain(tombstonedPath);
      }
    } finally {
      fs.delete(baseDirPath, true);
      fs.setTtlTimeProvider(originalTimeProvider);
    }
  }

  protected DirListingMetadata getDirListingMetadata(final MetadataStore ms,
      final Path baseDirPath) throws IOException {
    final DirListingMetadata fullDLM = ms.listChildren(baseDirPath);
    Assertions.assertThat(fullDLM)
        .describedAs("Metastrore directory listing of %s",
            baseDirPath)
        .isNotNull();
    return fullDLM;
  }

}
