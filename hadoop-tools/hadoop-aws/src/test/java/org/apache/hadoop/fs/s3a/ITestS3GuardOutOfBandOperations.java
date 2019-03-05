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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;

import static org.junit.Assume.assumeTrue;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * <pre>
 * This integration test is for documenting and defining how S3Guard should
 * behave in case of out-of-band (OOB) operations.
 *
 * The behaviour is the following in case of S3AFileSystem.getFileStatus:
 * A client with S3Guard
 * B client without S3Guard (Directly to S3)
 *
 * * OOB OVERWRITE, authoritative mode:
 * ** A client creates F1 file
 * ** B client overwrites F1 file with F2 (Same, or different file size)
 * ** A client's getFileStatus returns F1 metadata
 *
 * * OOB OVERWRITE, NOT authoritative mode:
 * ** A client creates F1 file
 * ** B client overwrites F1 file with F2 (Same, or different file size)
 * ** A client's getFileStatus returns F2 metadata. In not authoritative
 * mode we check S3 for the file. If the modification time of the file in S3
 * is greater than in S3Guard, we can safely return the S3 file metadata and
 * update the cache.
 *
 * * OOB DELETE, authoritative mode:
 * ** A client creates F file
 * ** B client deletes F file
 * ** A client's getFileStatus returns that the file is still there
 *
 * * OOB DELETE, NOT authoritative mode:
 * ** A client creates F file
 * ** B client deletes F file
 * ** A client's getFileStatus returns that the file is still there
 *
 * As you can see, authoritative and NOT authoritative mode behaves the same
 * at OOB DELETE case.
 *
 * The behaviour is the following in case of S3AFileSystem.listStatus:
 * * File status in metadata store gets updated during the listing (in
 * S3Guard.dirListingUnion) the same way as in getFileStatus.
 * </pre>
 */
@RunWith(Parameterized.class)
public class ITestS3GuardOutOfBandOperations extends AbstractS3ATestBase {

  private S3AFileSystem guardedFs;
  private S3AFileSystem rawFS;

  private MetadataStore realMs;

  /**
   * Is the "real" FS Authoritative.
   */
  private final boolean authoritative;

  /**
   * Test array for parameterized test runs.
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true}, {false}
    });
  }

  public ITestS3GuardOutOfBandOperations(final boolean authoritative) {
    this.authoritative = authoritative;
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // These test will fail if no ms
    assumeTrue("FS needs to have a metadatastore.",
        fs.hasMetadataStore());
    assumeTrue("Metadatastore should persist authoritative bit",
        metadataStorePersistsAuthoritativeBit(fs.getMetadataStore()));

    realMs = fs.getMetadataStore();
    // now we create a new FS with the auth parameter
    guardedFs = createGuardedFS(authoritative);
    // create raw fs without s3guard
    rawFS = createUnguardedFS();
    assertFalse("Raw FS still has S3Guard " + rawFS,
        rawFS.hasMetadataStore());
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.cleanupWithLogger(LOG, rawFS, guardedFs);
  }

  /**
   * Create a new FS which is the same config as the test FS, except
   * that it is guarded with the specific authoritative mode.
   * @param authoritativeMode mode of the new FS's metastore
   * @return the new FS
   */
  private S3AFileSystem createGuardedFS(boolean authoritativeMode)
      throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    S3ATestUtils.removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE);
    config.setBoolean(METADATASTORE_AUTHORITATIVE, authoritativeMode);
    final S3AFileSystem gFs = createFS(uri, config);
    // set back the same metadata store instance
    gFs.setMetadataStore(realMs);
    return gFs;
  }

  /**
   * Create a test filesystem which is always unguarded.
   * @return the new FS
   */
  private S3AFileSystem createUnguardedFS() throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    S3ATestUtils.removeBaseAndBucketOverrides(uri.getHost(), config,
        S3_METADATA_STORE_IMPL);
    S3ATestUtils.removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE);
    return createFS(uri, config);
  }

  /**
   * Create and init a filesystem.
   * @param uri FSU URI
   * @param config config.
   * @return new instance
   * @throws IOException failure
   */
  private S3AFileSystem createFS(final URI uri, final Configuration config)
      throws IOException {
    S3AFileSystem fs2 = new S3AFileSystem();
    fs2.initialize(uri, config);
    return fs2;
  }

  @Test
  public void testSameLengthOverwrite() throws Exception {
    String firstText = "hello, world!";
    String secondText = "HELLO, WORLD!";
    overwriteFile(firstText, secondText);
  }

  @Test
  public void testLongerLengthOverwrite() throws Exception {
    String firstText = "Hello, World!";
    String secondText = firstText + " " + firstText;
    overwriteFile(firstText, secondText);
  }

  @Test
  public void testOutOfBandDeletes() throws Exception {
    Path testFileName = path("/OutOfBandDelete-" + UUID.randomUUID());
    outOfBandDeletes(testFileName, true);
  }

  @Test
  public void testListingSameLengthOverwrite() throws Exception {
    overwriteFileInListing("THE TEXT", "the text");
  }

  @Test
  public void testListingLongerLengthOverwrite() throws Exception {
    overwriteFileInListing("THE TEXT", "THE LONGER TEXT");
  }

  @Test
  public void testListingDelete() throws Exception {
    deleteFileInListing();
  }

  private void outOfBandDeletes(Path testFileName, boolean allowAuthoritative)
      throws Exception {
    LOG.info("Allow authoritative param: {}", allowAuthoritative);
    try {
      // Create initial file
      String text = "Hello, World!";
      writeTextFile(guardedFs, testFileName, text, true);

      // Delete the file without S3Guard (raw)
      rawFS.delete(testFileName, true);

      // The check is the same if s3guard is authoritative and if it's not
      // it should be in the ms
      FileStatus status = guardedFs.getFileStatus(testFileName);
      LOG.info("Authoritative: {} status path: {}",
          allowAuthoritative, status.getPath());
      expectExceptionWhenReading(testFileName, text);
    } finally {
      guardedFs.delete(testFileName, true);
    }
  }

  /**
   * Overwrite a file.
   * @param firstText first text
   * @param secondText second text
   * @throws Exception failure
   */
  private void overwriteFile(String firstText, String secondText)
      throws Exception {
    boolean allowAuthoritative = authoritative;
    Path testFilePath = path("/OverwriteFileTest" + UUID.randomUUID());
    LOG.info("Allow authoritative param: {}",  allowAuthoritative);
    try {
      // Create initial file
      writeTextFile(
          guardedFs, testFilePath, firstText, true);

      // Delete and recreate the file without S3Guard
      rawFS.delete(testFilePath, true);
      writeTextFile(
          rawFS, testFilePath, secondText, true);
      FileStatus rawFileStatus = rawFS.getFileStatus(testFilePath);

      // Read the file and verify the data
      FileStatus guardedFileStatus = guardedFs.getFileStatus(testFilePath);
      if(allowAuthoritative) {
        assertNotEquals("Authoritative enabled, so metadata is not "
                + "updated in ms, so mod_time won't match. Expecting "
                + "different values for raw and guarded filestatus."
                + "Raw: " + rawFileStatus.toString() +
                " Guarded: " + guardedFileStatus.toString(),
            rawFileStatus.getModificationTime(),
            guardedFileStatus.getModificationTime());
      } else {
        // If authoritative is not enabled metadata is updated, mod_time
        // will match
        assertEquals("Authoritative is disabled, so metadata is"
                + " updated in ms, so mod_time must match. Expecting "
                + "same values for raw and guarded filestatus."
                + "Raw: " + rawFileStatus.toString() +
                " Guarded: " + guardedFileStatus.toString(),
            rawFileStatus.getModificationTime(),
            guardedFileStatus.getModificationTime());
      }
    } finally {
      guardedFs.delete(testFilePath, true);
    }
  }

  private void overwriteFileInListing(String firstText, String secondText)
      throws Exception {
    boolean allowAuthoritative = authoritative;

    LOG.info("Authoritative mode enabled: {}", allowAuthoritative);
    String rUUID = UUID.randomUUID().toString();
    String testDir = "/dir-" + rUUID + "/";
    String testFile = testDir + "file-1-" + rUUID;
    Path testDirPath = path(testDir);
    Path testFilePath = guardedFs.qualify(path(testFile));

    try {
      // Create initial statusIterator with guarded ms
      writeTextFile(
          guardedFs, testFilePath, firstText, true);

      // Do a listing to cache the lists. Should be authoritative if it's set.
      final FileStatus[] origList = guardedFs.listStatus(testDirPath);
      assertEquals("Added one file to the new dir, so the number of "
              + "files in the dir should be one.", 1, origList.length);
      final DirListingMetadata dirListingMetadata =
          realMs.listChildren(guardedFs.qualify(testDirPath));
      if (allowAuthoritative) {
        assertTrue("DirListingMeta should be authoritative if authoritative "
            + "mode is enabled.",
            dirListingMetadata.isAuthoritative());
      } else {
        assertFalse("DirListingMeta should not be authoritative if "
            + "authoritative mode is disabled.",
            dirListingMetadata.isAuthoritative());
      }

      // Update file with same size without S3Guard (raw)
      rawFS.delete(testFilePath, true);
      writeTextFile(
          rawFS, testFilePath, secondText, true);
      final FileStatus rawFileStatus = rawFS.getFileStatus(testFilePath);

      // set real ms and check if it failed
      final FileStatus[] modList = guardedFs.listStatus(testDirPath);
      assertEquals("Added one file to the new dir and modified the same file, "
          + "so the number of files in the dir should be one.", 1,
          modList.length);
      assertEquals("The only file path in the directory listing should be "
              + "equal to the testFilePath.", testFilePath,
          modList[0].getPath());

      // Read the file and verify the data
      FileStatus guardedFileStatus = guardedFs.getFileStatus(testFilePath);
      if (allowAuthoritative) {
        // If authoritative is allowed metadata is not updated, so mod_time
        // won't match
        assertNotEquals("Authoritative is enabled, so metadata is not "
                + "updated in ms, so mod_time won't match. Expecting "
                + "different values for raw and guarded filestatus."
                + " Raw: " + rawFileStatus.toString() +
                " Guarded: " + guardedFileStatus.toString(),
            rawFileStatus.getModificationTime(),
            guardedFileStatus.getModificationTime());
      } else {
        // If authoritative is not enabled metadata is updated, mod_time
        // will match
        assertEquals("Authoritative is disabled, so metadata is"
                + " updated in ms, so mod_time must match. Expecting "
                + " same values for raw and guarded filestatus."
                + " Raw: " + rawFileStatus.toString() +
                " Guarded: " + guardedFileStatus.toString(),
            rawFileStatus.getModificationTime(),
            guardedFileStatus.getModificationTime());
      }
    } finally {
      guardedFs.delete(testDirPath, true);
    }
  }

  private void deleteFileInListing()
      throws Exception {
    boolean allowAuthoritative = authoritative;
    LOG.info("Authoritative mode enabled: {}", allowAuthoritative);
    String rUUID = UUID.randomUUID().toString();
    String testDir = "/dir-" + rUUID + "/";
    String testFile = testDir + "file-1-" + rUUID;
    Path testDirPath = path(testDir);
    Path testFilePath = guardedFs.qualify(path(testFile));
    String text = "Some random text";

    try {
      // Create initial statusIterator with real ms
      writeTextFile(
          guardedFs, testFilePath, text, true);

      // Do a listing to cache the lists. Should be authoritative if it's set.
      final FileStatus[] origList = guardedFs.listStatus(testDirPath);
      assertEquals("Added one file to the new dir, so the number of "
          + "files in the dir should be one.", 1, origList.length);
      final DirListingMetadata dirListingMetadata =
          realMs.listChildren(guardedFs.qualify(testDirPath));
      if (allowAuthoritative) {
        assertTrue("DirListingMeta should be authoritative if authoritative "
                + "mode is enabled.",
            dirListingMetadata.isAuthoritative());
      } else {
        assertFalse("DirListingMeta should not be authoritative if "
                + "authoritative mode is disabled.",
            dirListingMetadata.isAuthoritative());
      }

      // Delete the file without S3Guard (raw)
      rawFS.delete(testFilePath, true);

      // File status will be still readable from s3guard
      FileStatus status = guardedFs.getFileStatus(testFilePath);
      LOG.info("authoritative: {} status: {}", allowAuthoritative, status);
      expectExceptionWhenReading(testFilePath, text);
    } finally {
      guardedFs.delete(testDirPath, true);
    }
  }

  /**
   * We expect the read to fail with an FNFE: open will be happy.
   * @param testFilePath path of the test file
   * @param text the context in the file.
   * @throws Exception failure other than the FNFE
   */
  private void expectExceptionWhenReading(Path testFilePath, String text)
      throws Exception {
    try (FSDataInputStream in = guardedFs.open(testFilePath)) {
      intercept(FileNotFoundException.class, () -> {
        byte[] bytes = new byte[text.length()];
        return in.read(bytes, 0, bytes.length);
      });
    }
    expectExceptionWhenReadingOpenFileAPI(testFilePath, text);
  }

  /**
   * We expect the read to fail with an FNFE: open will be happy.
   * @param testFilePath path of the test file
   * @param text the context in the file.
   * @throws Exception failure other than the FNFE
   */
  private void expectExceptionWhenReadingOpenFileAPI(
      Path testFilePath, String text)
      throws Exception {
    try (
        FSDataInputStream in = guardedFs.openFile(testFilePath).build().get()
    ) {
      intercept(FileNotFoundException.class, () -> {
        byte[] bytes = new byte[text.length()];
        return in.read(bytes, 0, bytes.length);
      });
    }
  }

}
