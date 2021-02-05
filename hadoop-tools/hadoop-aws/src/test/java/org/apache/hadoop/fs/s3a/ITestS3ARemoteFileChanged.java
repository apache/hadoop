/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Mode;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.readUTF8;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.CHANGE_DETECTED;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.S3_SELECT_CAPABILITY;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.SELECT_SQL;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Test S3A remote file change detection.
 * This is a very parameterized test; the first three parameters
 * define configuration options for the tests, while the final one
 * declares the expected outcomes given those options.
 *
 * This test uses mocking to insert transient failures into the S3 client,
 * underneath the S3A Filesystem instance.
 *
 * This is used to simulate eventual consistency, so force the change policy
 * failure modes to be encountered.
 *
 * If changes are made to the filesystem such that the number of calls to
 * operations such as {@link S3AFileSystem#getObjectMetadata(Path)} are
 * changed, the number of failures which the mock layer must generate may
 * change.
 *
 * As the S3Guard auth mode flag does control whether or not a HEAD is issued
 * in a call to {@code getFileStatus()}; the test parameter {@link #authMode}
 * is used to help predict this count.
 *
 * <i>Important:</i> if you are seeing failures in this test after changing
 * one of the rename/copy/open operations, it may be that an increase,
 * decrease or change in the number of low-level S3 HEAD/GET operations is
 * triggering the failures.
 * Please review the changes to see that you haven't unintentionally done this.
 * If it is intentional, please update the parameters here.
 *
 * If you are seeing failures without such a change, and nobody else is,
 * it is likely that you have a different bucket configuration option which
 * is somehow triggering a regression. If you can work out which option
 * this is, then extend {@link #createConfiguration()} to reset that parameter
 * too.
 *
 * Note: to help debug these issues, set the log for this to DEBUG:
 * <pre>
 *   log4j.logger.org.apache.hadoop.fs.s3a.ITestS3ARemoteFileChanged=DEBUG
 * </pre>
 * The debug information printed will include a trace of where operations
 * are being called from, to help understand why the test is failing.
 */
@RunWith(Parameterized.class)
public class ITestS3ARemoteFileChanged extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ARemoteFileChanged.class);

  private static final String TEST_DATA = "Some test data";

  private static final byte[] TEST_DATA_BYTES = TEST_DATA.getBytes(
      Charsets.UTF_8);
  private static final int TEST_MAX_RETRIES = 4;
  private static final String TEST_RETRY_INTERVAL = "1ms";
  private static final String QUOTED_TEST_DATA =
      "\"" + TEST_DATA + "\"";

  private Optional<AmazonS3> originalS3Client = Optional.empty();

  private static final String INCONSISTENT = "inconsistent";

  private static final String CONSISTENT = "consistent";

  private enum InteractionType {
    READ,
    READ_AFTER_DELETE,
    EVENTUALLY_CONSISTENT_READ,
    COPY,
    EVENTUALLY_CONSISTENT_COPY,
    EVENTUALLY_CONSISTENT_METADATA,
    SELECT,
    EVENTUALLY_CONSISTENT_SELECT
  }

  private final String changeDetectionSource;
  private final String changeDetectionMode;
  private final boolean authMode;
  private final Collection<InteractionType> expectedExceptionInteractions;
  private S3AFileSystem fs;

  /**
   * Test parameters.
   * <ol>
   *   <li>Change detection source: etag or version.</li>
   *   <li>Change detection policy: server, client, client+warn, none</li>
   *   <li>Whether to enable auth mode on the filesystem.</li>
   *   <li>Expected outcomes.</li>
   * </ol>
   * @return the test configuration.
   */
  @Parameterized.Parameters(name = "{0}-{1}-auth-{2}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        // make sure it works with invalid config
        {"bogus", "bogus",
            true,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.EVENTUALLY_CONSISTENT_METADATA,
                InteractionType.SELECT,
                InteractionType.EVENTUALLY_CONSISTENT_SELECT)},

        // test with etag
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_SERVER,
            true,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.EVENTUALLY_CONSISTENT_METADATA,
                InteractionType.SELECT,
                InteractionType.EVENTUALLY_CONSISTENT_SELECT)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_CLIENT,
            false,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.COPY,
                // not InteractionType.EVENTUALLY_CONSISTENT_COPY as copy change
                // detection can't really occur client-side.  The eTag of
                // the new object can't be expected to match.
                InteractionType.EVENTUALLY_CONSISTENT_METADATA,
                InteractionType.SELECT,
                InteractionType.EVENTUALLY_CONSISTENT_SELECT)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_WARN,
            false,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_NONE,
            false,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},

        // test with versionId
        // when using server-side versionId, the exceptions
        // shouldn't happen since the previous version will still be available
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_SERVER,
            true,
            Arrays.asList(
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.EVENTUALLY_CONSISTENT_COPY,
                InteractionType.EVENTUALLY_CONSISTENT_METADATA,
                InteractionType.EVENTUALLY_CONSISTENT_SELECT)},

        // with client-side versionId it will behave similar to client-side eTag
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_CLIENT,
            false,
            Arrays.asList(
                InteractionType.READ,
                InteractionType.READ_AFTER_DELETE,
                InteractionType.EVENTUALLY_CONSISTENT_READ,
                InteractionType.COPY,
                // not InteractionType.EVENTUALLY_CONSISTENT_COPY as copy change
                // detection can't really occur client-side.  The versionId of
                // the new object can't be expected to match.
                InteractionType.EVENTUALLY_CONSISTENT_METADATA,
                InteractionType.SELECT,
                InteractionType.EVENTUALLY_CONSISTENT_SELECT)},

        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_WARN,
            true,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)},
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_NONE,
            false,
            Arrays.asList(
                InteractionType.READ_AFTER_DELETE)}
    });
  }

  public ITestS3ARemoteFileChanged(String changeDetectionSource,
      String changeDetectionMode,
      boolean authMode,
      Collection<InteractionType> expectedExceptionInteractions) {
    this.changeDetectionSource = changeDetectionSource;
    this.changeDetectionMode = changeDetectionMode;
    this.authMode = authMode;
    this.expectedExceptionInteractions = expectedExceptionInteractions;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    // skip all versioned checks if the remote FS doesn't do
    // versions.
    fs = getFileSystem();
    skipIfVersionPolicyAndNoVersionId();
    // cache the original S3 client for teardown.
    originalS3Client = Optional.of(
        fs.getAmazonS3ClientForTesting("caching"));
  }

  @Override
  public void teardown() throws Exception {
    // restore the s3 client so there's no mocking interfering with the teardown
    if (fs != null) {
      originalS3Client.ifPresent(fs::setAmazonS3Client);
    }
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        CHANGE_DETECT_SOURCE,
        CHANGE_DETECT_MODE,
        RETRY_LIMIT,
        RETRY_INTERVAL,
        S3GUARD_CONSISTENCY_RETRY_LIMIT,
        S3GUARD_CONSISTENCY_RETRY_INTERVAL,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    conf.set(CHANGE_DETECT_SOURCE, changeDetectionSource);
    conf.set(CHANGE_DETECT_MODE, changeDetectionMode);
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, authMode);
    conf.set(AUTHORITATIVE_PATH, "");

    // reduce retry limit so FileNotFoundException cases timeout faster,
    // speeding up the tests
    conf.setInt(RETRY_LIMIT, TEST_MAX_RETRIES);
    conf.set(RETRY_INTERVAL, TEST_RETRY_INTERVAL);
    conf.setInt(S3GUARD_CONSISTENCY_RETRY_LIMIT, TEST_MAX_RETRIES);
    conf.set(S3GUARD_CONSISTENCY_RETRY_INTERVAL, TEST_RETRY_INTERVAL);

    if (conf.getClass(S3_METADATA_STORE_IMPL, MetadataStore.class) ==
        NullMetadataStore.class) {
      LOG.debug("Enabling local S3Guard metadata store");
      // favor LocalMetadataStore over NullMetadataStore
      conf.setClass(S3_METADATA_STORE_IMPL,
          LocalMetadataStore.class, MetadataStore.class);
    }
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * Get the path of this method, including parameterized values.
   * @return a path unique to this method and parameters
   * @throws IOException failure.
   */
  protected Path path() throws IOException {
    return super.path(getMethodName());
  }

  /**
   * How many HEAD requests are made in a call to
   * {@link S3AFileSystem#getFileStatus(Path)}?
   * @return a number >= 0.
   */
  private int getFileStatusHeadCount() {
    return authMode ? 0 : 1;
  }

  /**
   * Tests reading a file that is changed while the reader's InputStream is
   * open.
   */
  @Test
  public void testReadFileChangedStreamOpen() throws Throwable {
    describe("Tests reading a file that is changed while the reader's "
        + "InputStream is open.");
    final int originalLength = 8192;
    final byte[] originalDataset = dataset(originalLength, 'a', 32);
    final int newLength = originalLength + 1;
    final byte[] newDataset = dataset(newLength, 'A', 32);
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, originalDataset, originalDataset.length,
        1024, false);

    try(FSDataInputStream instream = fs.open(testpath)) {
      // seek forward and read successfully
      instream.seek(1024);
      assertTrue("no data to read", instream.read() >= 0);

      // overwrite
      writeDataset(fs, testpath, newDataset, newDataset.length, 1024, true);
      // here the new file length is larger. Probe the file to see if this is
      // true, with a spin and wait
      eventually(30 * 1000, 1000,
          () -> {
            assertEquals(newLength, fs.getFileStatus(testpath).getLen());
          });

      // With the new file version in place, any subsequent S3 read by
      // eTag/versionId will fail.  A new read by eTag/versionId will occur in
      // reopen() on read after a seek() backwards.  We verify seek backwards
      // results in the expected exception and seek() forward works without
      // issue.

      // first check seek forward
      instream.seek(2048);
      assertTrue("no data to read", instream.read() >= 0);

      // now check seek backward
      instream.seek(instream.getPos() - 100);

      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        expectReadFailure(instream);
      } else {
        instream.read();
      }

      byte[] buf = new byte[256];

      // seek backward
      instream.seek(0);

      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        expectReadFailure(instream);
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read(0, buf, 0, buf.length));
        intercept(RemoteFileChangedException.class,  "", "readfully",
            () -> instream.readFully(0, buf));
      } else {
        instream.read(buf);
        instream.read(0, buf, 0, buf.length);
        instream.readFully(0, buf);
      }

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      // seek backward
      instream.seek(0);

      if (expectedExceptionInteractions.contains(
          InteractionType.READ_AFTER_DELETE)) {
        intercept(FileNotFoundException.class, "", "read()",
            () -> instream.read());
        intercept(FileNotFoundException.class, "", "readfully",
            () -> instream.readFully(2048, buf));
      } else {
        instream.read();
        instream.readFully(2048, buf);
      }
    }
  }

  /**
   * Tests reading a file where the version visible in S3 does not match the
   * version tracked in the metadata store.
   */
  @Test
  public void testReadFileChangedOutOfSyncMetadata() throws Throwable {
    final Path testpath = writeOutOfSyncFileVersion("fileChangedOutOfSync.dat");

    try (FSDataInputStream instream = fs.open(testpath)) {
      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        expectReadFailure(instream);
      } else {
        instream.read();
      }
    }
  }

  /**
   * Verifies that when the openFile builder is passed in a status,
   * then that is used to eliminate the getFileStatus call in open();
   * thus the version and etag passed down are still used.
   */
  @Test
  public void testOpenFileWithStatus() throws Throwable {
    final Path testpath = path("testOpenFileWithStatus.dat");
    final byte[] dataset = TEST_DATA_BYTES;
    S3AFileStatus originalStatus =
        writeFile(testpath, dataset, dataset.length, true);

    // forge a file status with a different etag
    // no attempt is made to change the versionID as it will
    // get rejected by S3 as an invalid version
    S3AFileStatus forgedStatus =
        S3AFileStatus.fromFileStatus(originalStatus, Tristate.FALSE,
            originalStatus.getETag() + "-fake",
            originalStatus.getVersionId() + "");
    fs.getMetadataStore().put(
        new PathMetadata(forgedStatus, Tristate.FALSE, false));

    // verify the bad etag gets picked up.
    LOG.info("Opening stream with s3guard's (invalid) status.");
    try (FSDataInputStream instream = fs.openFile(testpath)
        .build()
        .get()) {
      try {
        instream.read();
        // No exception only if we don't enforce change detection as exception
        assertTrue(
            "Read did not raise an exception even though the change detection "
                + "mode was " + changeDetectionMode
                + " and the inserted file status was invalid",
            changeDetectionMode.equals(CHANGE_DETECT_MODE_NONE)
                || changeDetectionMode.equals(CHANGE_DETECT_MODE_WARN)
                || changeDetectionSource.equals(CHANGE_DETECT_SOURCE_VERSION_ID));
      } catch (RemoteFileChangedException ignored) {
        // Ignored.
      }
    }

    // By passing in the status open() doesn't need to check s3guard
    // And hence the existing file is opened
    LOG.info("Opening stream with the original status.");
    try (FSDataInputStream instream = fs.openFile(testpath)
        .withFileStatus(originalStatus)
        .build()
        .get()) {
      instream.read();
    }

    // and this holds for S3A Located Status
    LOG.info("Opening stream with S3ALocatedFileStatus.");
    try (FSDataInputStream instream = fs.openFile(testpath)
        .withFileStatus(new S3ALocatedFileStatus(originalStatus, null))
        .build()
        .get()) {
      instream.read();
    }

    // if you pass in a status of a dir, it will be rejected
    S3AFileStatus s2 = new S3AFileStatus(true, testpath, "alice");
    assertTrue("not a directory " + s2, s2.isDirectory());
    LOG.info("Open with directory status");
    interceptFuture(FileNotFoundException.class, "",
        fs.openFile(testpath)
            .withFileStatus(s2)
            .build());

    // now, we delete the file from the store and s3guard
    // when we pass in the status, there's no HEAD request, so it's only
    // in the read call where the 404 surfaces.
    // and there, when versionID is passed to the GET, the data is returned
    LOG.info("Testing opening a deleted file");
    fs.delete(testpath, false);
    try (FSDataInputStream instream = fs.openFile(testpath)
        .withFileStatus(originalStatus)
        .build()
        .get()) {
      if (changeDetectionSource.equals(CHANGE_DETECT_SOURCE_VERSION_ID)
          && changeDetectionMode.equals(CHANGE_DETECT_MODE_SERVER)) {
          // the deleted file is still there if you know the version ID
          // and the check is server-side
          instream.read();
      } else {
        // all other cases, the read will return 404.
        intercept(FileNotFoundException.class,
            () -> instream.read());
      }

    }

    // whereas without that status, you fail in the get() when a HEAD is
    // issued
    interceptFuture(FileNotFoundException.class, "",
        fs.openFile(testpath).build());

  }

  /**
   * Ensures a file can be read when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testReadWithNoVersionMetadata() throws Throwable {
    final Path testpath = writeFileWithNoVersionMetadata("readnoversion.dat");

    assertEquals("Contents of " + testpath,
        TEST_DATA,
        readUTF8(fs, testpath, -1));
  }

  /**
   * Tests using S3 Select on a file where the version visible in S3 does not
   * match the version tracked in the metadata store.
   */
  @Test
  public void testSelectChangedFile() throws Throwable {
    requireS3Select();
    final Path testpath = writeOutOfSyncFileVersion("select.dat");

    if (expectedExceptionInteractions.contains(InteractionType.SELECT)) {
      interceptFuture(RemoteFileChangedException.class, "select",
          fs.openFile(testpath)
              .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build());
    } else {
      fs.openFile(testpath)
          .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
          .build()
          .get()
          .close();
    }
  }

  /**
   * Tests using S3 Select on a file where the version visible in S3 does not
   * initially match the version tracked in the metadata store, but eventually
   * (after retries) does.
   */
  @Test
  public void testSelectEventuallyConsistentFile() throws Throwable {
    describe("Eventually Consistent S3 Select");
    requireS3Guard();
    requireS3Select();
    AmazonS3 s3ClientSpy = spyOnFilesystem();

    final Path testpath1 = writeEventuallyConsistentFileVersion(
        "select1.dat", s3ClientSpy, 0, TEST_MAX_RETRIES, 0);

    // should succeed since the inconsistency doesn't last longer than the
    // configured retry limit
    fs.openFile(testpath1)
        .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
        .build()
        .get()
        .close();

    // select() makes a getFileStatus() call before the consistency checking
    // that will match the stub. As such, we need an extra inconsistency here
    // to cross the threshold
    int getMetadataInconsistencyCount = TEST_MAX_RETRIES + 2;
    final Path testpath2 = writeEventuallyConsistentFileVersion(
        "select2.dat", s3ClientSpy, 0, getMetadataInconsistencyCount, 0);

    if (expectedExceptionInteractions.contains(
        InteractionType.EVENTUALLY_CONSISTENT_SELECT)) {
      // should fail since the inconsistency lasts longer than the configured
      // retry limit
      interceptFuture(RemoteFileChangedException.class, "select",
          fs.openFile(testpath2)
              .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build());
    } else {
      fs.openFile(testpath2)
          .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
          .build()
          .get()
          .close();
    }
  }

  /**
   * Ensures a file can be read via S3 Select when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testSelectWithNoVersionMetadata() throws Throwable {
    requireS3Select();
    final Path testpath =
        writeFileWithNoVersionMetadata("selectnoversion.dat");

    try (FSDataInputStream instream = fs.openFile(testpath)
        .must(SELECT_SQL, "SELECT * FROM S3OBJECT")
        .build()
        .get()) {
      assertEquals(QUOTED_TEST_DATA,
          IOUtils.toString(instream, StandardCharsets.UTF_8).trim());
    }
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version tracked in the metadata store.
   * @throws Throwable failure
   */
  @Test
  public void testRenameChangedFile() throws Throwable {
    final Path testpath = writeOutOfSyncFileVersion("rename.dat");

    final Path dest = path("dest.dat");
    if (expectedExceptionInteractions.contains(InteractionType.COPY)) {
      intercept(RemoteFileChangedException.class, "",
          "expected copy() failure",
          () -> fs.rename(testpath, dest));
    } else {
      fs.rename(testpath, dest);
    }
  }

  /**
   * Inconsistent response counts for getObjectMetadata() and
   * copyObject() for a rename.
   * @param metadataCallsExpectedBeforeRetryLoop number of getObjectMetadata
   * calls expected before the consistency checking retry loop
   * @return the inconsistencies for (metadata, copy)
   */
  private Pair<Integer, Integer> renameInconsistencyCounts(
      int metadataCallsExpectedBeforeRetryLoop) {
    int metadataInconsistencyCount = TEST_MAX_RETRIES
        + metadataCallsExpectedBeforeRetryLoop;
    int copyInconsistencyCount =
        versionCheckingIsOnServer() ? TEST_MAX_RETRIES : 0;

    return Pair.of(metadataInconsistencyCount, copyInconsistencyCount);
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version in the metadata store until a certain number of retries
   * has been met.
   */
  @Test
  public void testRenameEventuallyConsistentFile() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();

    // Total inconsistent response count across getObjectMetadata() and
    // copyObject().
    // The split of inconsistent responses between getObjectMetadata() and
    // copyObject() is arbitrary.
    Pair<Integer, Integer> counts = renameInconsistencyCounts(
        getFileStatusHeadCount());
    int metadataInconsistencyCount = counts.getLeft();
    int copyInconsistencyCount = counts.getRight();
    final Path testpath1 =
        writeEventuallyConsistentFileVersion("rename-eventually1.dat",
            s3ClientSpy,
            0,
            metadataInconsistencyCount,
            copyInconsistencyCount);

    final Path dest1 = path("dest1.dat");
    // shouldn't fail since the inconsistency doesn't last through the
    // configured retry limit
    fs.rename(testpath1, dest1);
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version in the metadata store until a certain number of retries
   * has been met.
   * The test expects failure by AWSClientIOException caused by NPE due to
   * https://github.com/aws/aws-sdk-java/issues/1644
   */
  @Test
  public void testRenameEventuallyConsistentFileNPE() throws Throwable {
    requireS3Guard();
    skipIfVersionPolicyAndNoVersionId();
    AmazonS3 s3ClientSpy = spyOnFilesystem();

    Pair<Integer, Integer> counts = renameInconsistencyCounts(
        getFileStatusHeadCount());
    int metadataInconsistencyCount = counts.getLeft();
    int copyInconsistencyCount = counts.getRight();
    // giving copyInconsistencyCount + 1 here should trigger the failure,
    // exceeding the retry limit
    final Path testpath2 =
        writeEventuallyConsistentFileVersion("rename-eventuallyNPE.dat",
            s3ClientSpy,
            0,
            metadataInconsistencyCount,
            copyInconsistencyCount + 1);
    final Path dest2 = path("destNPE.dat");
    if (expectedExceptionInteractions.contains(
        InteractionType.EVENTUALLY_CONSISTENT_COPY)) {
      // should fail since the inconsistency is set up to persist longer than
      // the configured retry limit
      // the expected exception is not RemoteFileChangedException due to
      // https://github.com/aws/aws-sdk-java/issues/1644
      // If this test is failing after an AWS SDK update,
      // then it means the SDK bug is fixed.
      // Please update this test to match the new behavior.
      AWSClientIOException exception =
          intercept(AWSClientIOException.class,
              "Unable to complete transfer: null",
              "expected copy() failure",
              () -> fs.rename(testpath2, dest2));
      AmazonClientException cause = exception.getCause();
      if (cause == null) {
        // no cause; something else went wrong: throw.
        throw new AssertionError("No inner cause",
            exception);
      }
      Throwable causeCause = cause.getCause();
      if (!(causeCause instanceof NullPointerException)) {
        // null causeCause or it is the wrong type: throw
        throw new AssertionError("Innermost cause is not NPE",
            exception);
      }
    } else {
      fs.rename(testpath2, dest2);
    }
  }

  /**
   * Tests doing a rename() on a file where the version visible in S3 does not
   * match the version in the metadata store until a certain number of retries
   * has been met.
   * The test expects failure by RemoteFileChangedException.
   */
  @Test
  public void testRenameEventuallyConsistentFileRFCE() throws Throwable {
    requireS3Guard();
    skipIfVersionPolicyAndNoVersionId();
    AmazonS3 s3ClientSpy = spyOnFilesystem();

    Pair<Integer, Integer> counts = renameInconsistencyCounts(
        getFileStatusHeadCount());
    int metadataInconsistencyCount = counts.getLeft();
    int copyInconsistencyCount = counts.getRight();
    // giving metadataInconsistencyCount + 1 here should trigger the failure,
    // exceeding the retry limit
    final Path testpath2 =
        writeEventuallyConsistentFileVersion("rename-eventuallyRFCE.dat",
            s3ClientSpy,
            0,
            metadataInconsistencyCount + 1,
            copyInconsistencyCount);
    final Path dest2 = path("destRFCE.dat");
    if (expectedExceptionInteractions.contains(
        InteractionType.EVENTUALLY_CONSISTENT_METADATA)) {
      // should fail since the inconsistency is set up to persist longer than
      // the configured retry limit
      intercept(RemoteFileChangedException.class,
          CHANGE_DETECTED,
          "expected copy() failure",
          () -> fs.rename(testpath2, dest2));
    } else {
      fs.rename(testpath2, dest2);
    }
  }

  /**
   * Tests doing a rename() on a directory containing
   * an file which is eventually consistent.
   * There is no call to getFileStatus on the source file whose
   * inconsistency is simulated; the state of S3Guard auth mode is not
   * relevant.
   */
  @Test
  public void testRenameEventuallyConsistentDirectory() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    Path basedir = path();
    Path sourcedir = new Path(basedir, "sourcedir");
    fs.mkdirs(sourcedir);
    Path destdir = new Path(basedir, "destdir");
    Path inconsistentFile = new Path(sourcedir, INCONSISTENT);
    Path consistentFile = new Path(sourcedir, CONSISTENT);

    // write the consistent data
    writeDataset(fs, consistentFile, TEST_DATA_BYTES, TEST_DATA_BYTES.length,
        1024, true, true);

    Pair<Integer, Integer> counts = renameInconsistencyCounts(0);
    int metadataInconsistencyCount = counts.getLeft();
    int copyInconsistencyCount = counts.getRight();

    writeEventuallyConsistentData(
        s3ClientSpy,
        inconsistentFile,
        TEST_DATA_BYTES,
        0,
        metadataInconsistencyCount,
        copyInconsistencyCount);

    // must not fail since the inconsistency doesn't last through the
    // configured retry limit
    fs.rename(sourcedir, destdir);
  }

  /**
   * Tests doing a rename() on a file which is eventually visible.
   */
  @Test
  public void testRenameEventuallyVisibleFile() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    Path basedir = path();
    Path sourcedir = new Path(basedir, "sourcedir");
    fs.mkdirs(sourcedir);
    Path destdir = new Path(basedir, "destdir");
    Path inconsistentFile = new Path(sourcedir, INCONSISTENT);
    Path consistentFile = new Path(sourcedir, CONSISTENT);

    // write the consistent data
    writeDataset(fs, consistentFile, TEST_DATA_BYTES, TEST_DATA_BYTES.length,
        1024, true, true);

    Pair<Integer, Integer> counts = renameInconsistencyCounts(0);
    int metadataInconsistencyCount = counts.getLeft();

    writeDataset(fs, inconsistentFile, TEST_DATA_BYTES, TEST_DATA_BYTES.length,
        1024, true, true);

    stubTemporaryNotFound(s3ClientSpy, metadataInconsistencyCount,
        inconsistentFile);

    // must not fail since the inconsistency doesn't last through the
    // configured retry limit
    fs.rename(sourcedir, destdir);
  }

  /**
   * Tests doing a rename() on a file which never quite appears will
   * fail with a RemoteFileChangedException rather than have the exception
   * downgraded to a failure.
   */
  @Test
  public void testRenameMissingFile()
      throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    Path basedir = path();
    Path sourcedir = new Path(basedir, "sourcedir");
    fs.mkdirs(sourcedir);
    Path destdir = new Path(basedir, "destdir");
    Path inconsistentFile = new Path(sourcedir, INCONSISTENT);
    Path consistentFile = new Path(sourcedir, CONSISTENT);

    // write the consistent data
    writeDataset(fs, consistentFile, TEST_DATA_BYTES, TEST_DATA_BYTES.length,
        1024, true, true);

    Pair<Integer, Integer> counts = renameInconsistencyCounts(0);
    int metadataInconsistencyCount = counts.getLeft();

    writeDataset(fs, inconsistentFile, TEST_DATA_BYTES, TEST_DATA_BYTES.length,
        1024, true, true);

    stubTemporaryNotFound(s3ClientSpy, metadataInconsistencyCount + 1,
        inconsistentFile);

    String expected = fs.hasMetadataStore()
        ? RemoteFileChangedException.FILE_NEVER_FOUND
        : RemoteFileChangedException.FILE_NOT_FOUND_SINGLE_ATTEMPT;
    RemoteFileChangedException ex = intercept(
        RemoteFileChangedException.class,
        expected,
        () -> fs.rename(sourcedir, destdir));
    assertEquals("Path in " + ex,
        inconsistentFile, ex.getPath());
    if (!(ex.getCause() instanceof FileNotFoundException)) {
      throw ex;
    }
  }

  /**
   * Ensures a file can be renamed when there is no version metadata
   * (ETag, versionId).
   */
  @Test
  public void testRenameWithNoVersionMetadata() throws Throwable {
    final Path testpath =
        writeFileWithNoVersionMetadata("renamenoversion.dat");

    final Path dest = path("noversiondest.dat");
    fs.rename(testpath, dest);
    assertEquals("Contents of " + dest,
        TEST_DATA,
        readUTF8(fs, dest, -1));
  }

  /**
   * Ensures S3Guard and retries allow an eventually consistent read.
   */
  @Test
  public void testReadAfterEventuallyConsistentWrite() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    final Path testpath1 =
        writeEventuallyConsistentFileVersion("eventually1.dat",
            s3ClientSpy, TEST_MAX_RETRIES, 0 , 0);

    try (FSDataInputStream instream1 = fs.open(testpath1)) {
      // succeeds on the last retry
      instream1.read();
    }
  }

  /**
   * Ensures S3Guard and retries allow an eventually consistent read.
   */
  @Test
  public void testReadAfterEventuallyConsistentWrite2() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    final Path testpath2 =
        writeEventuallyConsistentFileVersion("eventually2.dat",
            s3ClientSpy, TEST_MAX_RETRIES + 1, 0, 0);

    try (FSDataInputStream instream2 = fs.open(testpath2)) {
      if (expectedExceptionInteractions.contains(
          InteractionType.EVENTUALLY_CONSISTENT_READ)) {
        // keeps retrying and eventually gives up with RemoteFileChangedException
        expectReadFailure(instream2);
      } else {
        instream2.read();
      }
    }
  }

  /**
   * Ensures read on re-open (after seek backwards) when S3 does not return the
   * version of the file tracked in the metadata store fails immediately.  No
   * retries should happen since a retry is not expected to recover.
   */
  @Test
  public void testEventuallyConsistentReadOnReopen() throws Throwable {
    requireS3Guard();
    AmazonS3 s3ClientSpy = spyOnFilesystem();
    String filename = "eventually-reopen.dat";
    final Path testpath =
        writeEventuallyConsistentFileVersion(filename,
            s3ClientSpy, 0, 0, 0);

    try (FSDataInputStream instream = fs.open(testpath)) {
      instream.read();
      // overwrite the file, returning inconsistent version for
      // (effectively) infinite retries
      writeEventuallyConsistentFileVersion(filename, s3ClientSpy,
          Integer.MAX_VALUE, 0, 0);
      instream.seek(0);
      if (expectedExceptionInteractions.contains(InteractionType.READ)) {
        // if it retries at all, it will retry forever, which should fail
        // the test.  The expected behavior is immediate
        // RemoteFileChangedException.
        expectReadFailure(instream);
      } else {
        instream.read();
      }
    }
  }

  /**
   * Writes a file with old ETag and versionId in the metadata store such
   * that the metadata is out of sync with S3.  Attempts to read such a file
   * should result in {@link RemoteFileChangedException}.
   */
  private Path writeOutOfSyncFileVersion(String filename) throws IOException {
    final Path testpath = path(filename);
    final byte[] dataset = TEST_DATA_BYTES;
    S3AFileStatus originalStatus =
        writeFile(testpath, dataset, dataset.length, false);

    // overwrite with half the content
    S3AFileStatus newStatus = writeFile(testpath, dataset, dataset.length / 2,
        true);

    // put back the original etag, versionId
    S3AFileStatus forgedStatus =
        S3AFileStatus.fromFileStatus(newStatus, Tristate.FALSE,
            originalStatus.getETag(), originalStatus.getVersionId());
    fs.getMetadataStore().put(
        new PathMetadata(forgedStatus, Tristate.FALSE, false));

    return testpath;
  }

  /**
   * Write data to a file; return the status from the filesystem.
   * @param path file path
   * @param dataset dataset to write from
   * @param length number of bytes from the dataset to write.
   * @param overwrite overwrite flag
   * @return the retrieved file status.
   */
  private S3AFileStatus writeFile(final Path path,
      final byte[] dataset,
      final int length,
      final boolean overwrite) throws IOException {
    writeDataset(fs, path, dataset, length,
        1024, overwrite);
    return (S3AFileStatus) fs.getFileStatus(path);
  }

  /**
   * Writes {@link #TEST_DATA} to a file where the file will be inconsistent
   * in S3 for a set of operations.
   * The duration of the inconsistency is controlled by the
   * getObjectInconsistencyCount, getMetadataInconsistencyCount, and
   * copyInconsistentCallCount parameters.
   * The inconsistency manifests in AmazonS3#getObject,
   * AmazonS3#getObjectMetadata, and AmazonS3#copyObject.
   * This method sets up the provided s3ClientSpy to return a response to each
   * of these methods indicating an inconsistency where the requested object
   * version (eTag or versionId) is not available until a certain retry
   * threshold is met.
   * Providing inconsistent call count values above or
   * below the overall retry limit allows a test to simulate a condition that
   * either should or should not result in an overall failure from retry
   * exhaustion.
   * @param filename name of file (will be under test path)
   * @param s3ClientSpy s3 client to patch
   * @param getObjectInconsistencyCount number of GET inconsistencies
   * @param getMetadataInconsistencyCount number of HEAD inconsistencies
   * @param copyInconsistencyCount number of COPY inconsistencies.
   * @return the path written
   * @throws IOException failure to write the test data.
   */
  private Path writeEventuallyConsistentFileVersion(String filename,
      AmazonS3 s3ClientSpy,
      int getObjectInconsistencyCount,
      int getMetadataInconsistencyCount,
      int copyInconsistencyCount)
      throws IOException {
    return writeEventuallyConsistentData(s3ClientSpy,
        path(filename),
        TEST_DATA_BYTES,
        getObjectInconsistencyCount,
        getMetadataInconsistencyCount,
        copyInconsistencyCount);
  }

  /**
   * Writes data to a path and configures the S3 client for inconsistent
   * HEAD, GET or COPY operations.
   * @param testpath absolute path of file
   * @param s3ClientSpy s3 client to patch
   * @param dataset bytes to write.
   * @param getObjectInconsistencyCount number of GET inconsistencies
   * @param getMetadataInconsistencyCount number of HEAD inconsistencies
   * @param copyInconsistencyCount number of COPY inconsistencies.
   * @return the path written
   * @throws IOException failure to write the test data.
   */
  private Path writeEventuallyConsistentData(final AmazonS3 s3ClientSpy,
      final Path testpath,
      final byte[] dataset,
      final int getObjectInconsistencyCount,
      final int getMetadataInconsistencyCount,
      final int copyInconsistencyCount)
      throws IOException {
    writeDataset(fs, testpath, dataset, dataset.length,
        1024, true);
    S3AFileStatus originalStatus = (S3AFileStatus) fs.getFileStatus(testpath);

    // overwrite with half the content
    writeDataset(fs, testpath, dataset, dataset.length / 2,
        1024, true);

    LOG.debug("Original file info: {}: version={}, etag={}", testpath,
        originalStatus.getVersionId(), originalStatus.getETag());

    S3AFileStatus newStatus = (S3AFileStatus) fs.getFileStatus(testpath);
    LOG.debug("Updated file info: {}: version={}, etag={}", testpath,
        newStatus.getVersionId(), newStatus.getETag());

    LOG.debug("File {} will be inconsistent for {} HEAD and {} GET requests",
        testpath, getMetadataInconsistencyCount, getObjectInconsistencyCount);

    stubTemporaryUnavailable(s3ClientSpy, getObjectInconsistencyCount,
        testpath, newStatus);

    stubTemporaryWrongVersion(s3ClientSpy, getObjectInconsistencyCount,
        testpath, originalStatus);

    if (versionCheckingIsOnServer()) {
      // only stub inconsistency when mode is server since no constraints that
      // should trigger inconsistency are passed in any other mode
      LOG.debug("File {} will be inconsistent for {} COPY operations",
          testpath, copyInconsistencyCount);
      stubTemporaryCopyInconsistency(s3ClientSpy, testpath, newStatus,
          copyInconsistencyCount);
    }

    stubTemporaryMetadataInconsistency(s3ClientSpy, testpath, originalStatus,
        newStatus, getMetadataInconsistencyCount);

    return testpath;
  }

  /**
   * Log the call hierarchy at debug level, helps track down
   * where calls to operations are coming from.
   */
  private void logLocationAtDebug() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call hierarchy", new Exception("here"));
    }
  }

  /**
   * Stubs {@link AmazonS3#getObject(GetObjectRequest)}
   * within s3ClientSpy to return null until inconsistentCallCount calls have
   * been made.  The null response simulates what occurs when an object
   * matching the specified ETag or versionId is not available.
   * @param s3ClientSpy the spy to stub
   * @param inconsistentCallCount the number of calls that should return the
   * null response
   * @param testpath the path of the object the stub should apply to
   */
  private void stubTemporaryUnavailable(AmazonS3 s3ClientSpy,
      int inconsistentCallCount, Path testpath,
      S3AFileStatus newStatus) {
    Answer<S3Object> temporarilyUnavailableAnswer = new Answer<S3Object>() {
      private int callCount = 0;

      @Override
      public S3Object answer(InvocationOnMock invocation) throws Throwable {
        // simulates ETag or versionId constraint not met until
        // inconsistentCallCount surpassed
        callCount++;
        if (callCount <= inconsistentCallCount) {
          LOG.info("Temporarily unavailable {} count {} of {}",
              testpath, callCount, inconsistentCallCount);
          logLocationAtDebug();
          return null;
        }
        return (S3Object) invocation.callRealMethod();
      }
    };

    // match the requests that would be made in either server-side change
    // detection mode
    doAnswer(temporarilyUnavailableAnswer).when(s3ClientSpy)
        .getObject(
            matchingGetObjectRequest(
                testpath, newStatus.getETag(), null));
    doAnswer(temporarilyUnavailableAnswer).when(s3ClientSpy)
        .getObject(
            matchingGetObjectRequest(
                testpath, null, newStatus.getVersionId()));
  }

  /**
   * Stubs {@link AmazonS3#getObject(GetObjectRequest)}
   * within s3ClientSpy to return an object modified to contain metadata
   * from originalStatus until inconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param inconsistentCallCount the number of calls that should return the
   * null response
   * @param originalStatus the status metadata to inject into the
   * inconsistentCallCount responses
   */
  private void stubTemporaryWrongVersion(AmazonS3 s3ClientSpy,
      int inconsistentCallCount, Path testpath,
      S3AFileStatus originalStatus) {
    Answer<S3Object> temporarilyWrongVersionAnswer = new Answer<S3Object>() {
      private int callCount = 0;

      @Override
      public S3Object answer(InvocationOnMock invocation) throws Throwable {
        // simulates old ETag or versionId until inconsistentCallCount surpassed
        callCount++;
        S3Object s3Object = (S3Object) invocation.callRealMethod();
        if (callCount <= inconsistentCallCount) {
          LOG.info("Temporary Wrong Version {} count {} of {}",
              testpath, callCount, inconsistentCallCount);
          logLocationAtDebug();
          S3Object objectSpy = Mockito.spy(s3Object);
          ObjectMetadata metadataSpy =
              Mockito.spy(s3Object.getObjectMetadata());
          when(objectSpy.getObjectMetadata()).thenReturn(metadataSpy);
          when(metadataSpy.getETag()).thenReturn(originalStatus.getETag());
          when(metadataSpy.getVersionId())
              .thenReturn(originalStatus.getVersionId());
          return objectSpy;
        }
        return s3Object;
      }
    };

    // match requests that would be made in client-side change detection
    doAnswer(temporarilyWrongVersionAnswer).when(s3ClientSpy).getObject(
        matchingGetObjectRequest(testpath, null, null));
  }

  /**
   * Stubs {@link AmazonS3#copyObject(CopyObjectRequest)}
   * within s3ClientSpy to return null (indicating preconditions not met) until
   * copyInconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param newStatus the status metadata containing the ETag and versionId
   * that should be matched in order for the stub to apply
   * @param copyInconsistentCallCount how many times to return the
   * precondition failed error
   */
  private void stubTemporaryCopyInconsistency(AmazonS3 s3ClientSpy,
      Path testpath, S3AFileStatus newStatus,
      int copyInconsistentCallCount) {
    Answer<CopyObjectResult> temporarilyPreconditionsNotMetAnswer =
        new Answer<CopyObjectResult>() {
      private int callCount = 0;

      @Override
      public CopyObjectResult answer(InvocationOnMock invocation)
          throws Throwable {
        callCount++;
        if (callCount <= copyInconsistentCallCount) {
          String message = "preconditions not met on call " + callCount
              + " of " + copyInconsistentCallCount;
          LOG.info("Copying {}: {}", testpath, message);
          logLocationAtDebug();
          return null;
        }
        return (CopyObjectResult) invocation.callRealMethod();
      }
    };

    // match requests made during copy
    doAnswer(temporarilyPreconditionsNotMetAnswer).when(s3ClientSpy).copyObject(
        matchingCopyObjectRequest(testpath, newStatus.getETag(), null));
    doAnswer(temporarilyPreconditionsNotMetAnswer).when(s3ClientSpy).copyObject(
        matchingCopyObjectRequest(testpath, null, newStatus.getVersionId()));
  }

  /**
   * Stubs {@link AmazonS3#getObjectMetadata(GetObjectMetadataRequest)}
   * within s3ClientSpy to return metadata from originalStatus until
   * metadataInconsistentCallCount calls have been made.
   * @param s3ClientSpy the spy to stub
   * @param testpath the path of the object the stub should apply to
   * @param originalStatus the inconsistent status metadata to return
   * @param newStatus the status metadata to return after
   * metadataInconsistentCallCount is met
   * @param metadataInconsistentCallCount how many times to return the
   * inconsistent metadata
   */
  private void stubTemporaryMetadataInconsistency(AmazonS3 s3ClientSpy,
      Path testpath, S3AFileStatus originalStatus,
      S3AFileStatus newStatus, int metadataInconsistentCallCount) {
    Answer<ObjectMetadata> temporarilyOldMetadataAnswer =
        new Answer<ObjectMetadata>() {
      private int callCount = 0;

      @Override
      public ObjectMetadata answer(InvocationOnMock invocation)
          throws Throwable {
        ObjectMetadata objectMetadata =
            (ObjectMetadata) invocation.callRealMethod();
        callCount++;
        if (callCount <= metadataInconsistentCallCount) {
          LOG.info("Inconsistent metadata {} count {} of {}",
              testpath, callCount, metadataInconsistentCallCount);
          logLocationAtDebug();
          ObjectMetadata metadataSpy =
              Mockito.spy(objectMetadata);
          when(metadataSpy.getETag()).thenReturn(originalStatus.getETag());
          when(metadataSpy.getVersionId())
              .thenReturn(originalStatus.getVersionId());
          return metadataSpy;
        }
        return objectMetadata;
      }
    };

    // match requests made during select
    doAnswer(temporarilyOldMetadataAnswer).when(s3ClientSpy).getObjectMetadata(
        matchingMetadataRequest(testpath, null));
    doAnswer(temporarilyOldMetadataAnswer).when(s3ClientSpy).getObjectMetadata(
        matchingMetadataRequest(testpath, newStatus.getVersionId()));
  }

  /**
   * Writes a file with null ETag and versionId in the metadata store.
   */
  private Path writeFileWithNoVersionMetadata(String filename)
      throws IOException {
    final Path testpath = path(filename);
    S3AFileStatus originalStatus = writeFile(testpath, TEST_DATA_BYTES,
        TEST_DATA_BYTES.length, false);

    // remove ETag and versionId
    S3AFileStatus newStatus = S3AFileStatus.fromFileStatus(originalStatus,
        Tristate.FALSE, null, null);
    fs.getMetadataStore().put(new PathMetadata(newStatus, Tristate.FALSE,
        false));

    return testpath;
  }

  /**
   * The test is invalid if the policy uses versionId but the bucket doesn't
   * have versioning enabled.
   *
   * Tests the given file for a versionId to detect whether bucket versioning
   * is enabled.
   */
  private void skipIfVersionPolicyAndNoVersionId(Path testpath)
      throws IOException {
    if (fs.getChangeDetectionPolicy().getSource() == Source.VersionId) {
      // skip versionId tests if the bucket doesn't have object versioning
      // enabled
      Assume.assumeTrue(
          "Target filesystem does not support versioning",
          fs.getObjectMetadata(fs.pathToKey(testpath)).getVersionId() != null);
    }
  }

  /**
   * Like {@link #skipIfVersionPolicyAndNoVersionId(Path)} but generates a new
   * file to test versionId against.
   */
  private void skipIfVersionPolicyAndNoVersionId() throws IOException {
    if (fs.getChangeDetectionPolicy().getSource() == Source.VersionId) {
      Path versionIdFeatureTestFile = path("versionIdTest");
      writeDataset(fs, versionIdFeatureTestFile, TEST_DATA_BYTES,
          TEST_DATA_BYTES.length, 1024, true, true);
      skipIfVersionPolicyAndNoVersionId(versionIdFeatureTestFile);
    }
  }

  private GetObjectRequest matchingGetObjectRequest(Path path, String eTag,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getBucketName().equals(fs.getBucket())
          && request.getKey().equals(fs.pathToKey(path))) {
        if (eTag == null && !request.getMatchingETagConstraints().isEmpty()) {
          return false;
        }
        if (eTag != null &&
            !request.getMatchingETagConstraints().contains(eTag)) {
          return false;
        }
        if (versionId == null && request.getVersionId() != null) {
          return false;
        }
        if (versionId != null && !versionId.equals(request.getVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  private CopyObjectRequest matchingCopyObjectRequest(Path path, String eTag,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getSourceBucketName().equals(fs.getBucket())
          && request.getSourceKey().equals(fs.pathToKey(path))) {
        if (eTag == null && !request.getMatchingETagConstraints().isEmpty()) {
          return false;
        }
        if (eTag != null &&
            !request.getMatchingETagConstraints().contains(eTag)) {
          return false;
        }
        if (versionId == null && request.getSourceVersionId() != null) {
          return false;
        }
        if (versionId != null &&
            !versionId.equals(request.getSourceVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  private GetObjectMetadataRequest matchingMetadataRequest(Path path,
      String versionId) {
    return ArgumentMatchers.argThat(request -> {
      if (request.getBucketName().equals(fs.getBucket())
          && request.getKey().equals(fs.pathToKey(path))) {
        if (versionId == null && request.getVersionId() != null) {
          return false;
        }
        if (versionId != null &&
            !versionId.equals(request.getVersionId())) {
          return false;
        }
        return true;
      }
      return false;
    });
  }

  /**
   * Match any getObjectMetadata request against a given path.
   * @param path path to to match.
   * @return the matching request.
   */
  private GetObjectMetadataRequest matchingMetadataRequest(Path path) {
    return ArgumentMatchers.argThat(request -> {
      return request.getBucketName().equals(fs.getBucket())
          && request.getKey().equals(fs.pathToKey(path));
    });
  }

  /**
   * Skip a test case if it needs S3Guard and the filesystem does
   * not have it.
   */
  private void requireS3Guard() {
    Assume.assumeTrue("S3Guard must be enabled", fs.hasMetadataStore());
  }

  /**
   * Skip a test case if S3 Select is not supported on this store.
   */
  private void requireS3Select() {
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
  }

  /**
   * Spy on the filesystem at the S3 client level.
   * @return a mocked S3 client to which the test FS is bonded.
   */
  private AmazonS3 spyOnFilesystem() {
    AmazonS3 s3ClientSpy = Mockito.spy(
        fs.getAmazonS3ClientForTesting("mocking"));
    fs.setAmazonS3Client(s3ClientSpy);
    return s3ClientSpy;
  }

  /**
   * Expect reading this stream to fail.
   * @param instream input stream.
   * @return the caught exception.
   * @throws Exception an other exception
   */

  private RemoteFileChangedException expectReadFailure(
      final FSDataInputStream instream)
      throws Exception {
    return intercept(RemoteFileChangedException.class, "",
        "read() returned",
        () -> readToText(instream.read()));
  }

  /**
   * Convert the result of a read to a text string for errors.
   * @param r result of the read() call.
   * @return a string for exception text.
   */
  private String readToText(int r) {
    return r < 32
        ? (String.format("%02d", r))
        : (String.format("%c", (char) r));
  }

  /**
   * Is the version checking on the server?
   * @return true if the server returns 412 errors.
   */
  private boolean versionCheckingIsOnServer() {
    return fs.getChangeDetectionPolicy().getMode() == Mode.Server;
  }

  /**
   * Stubs {@link AmazonS3#getObject(GetObjectRequest)}
   * within s3ClientSpy to return throw a FileNotFoundException
   * until inconsistentCallCount calls have been made.
   * This simulates the condition where the S3 endpoint is caching
   * a 404 request, or there is a tombstone in the way which has yet
   * to clear.
   * @param s3ClientSpy the spy to stub
   * @param inconsistentCallCount the number of calls that should return the
   * null response
   * @param testpath the path of the object the stub should apply to
   */
  private void stubTemporaryNotFound(AmazonS3 s3ClientSpy,
      int inconsistentCallCount, Path testpath) {
    Answer<ObjectMetadata> notFound = new Answer<ObjectMetadata>() {
      private int callCount = 0;

      @Override
      public ObjectMetadata answer(InvocationOnMock invocation
      ) throws Throwable {
        // simulates delayed visibility.
        callCount++;
        if (callCount <= inconsistentCallCount) {
          LOG.info("Temporarily unavailable {} count {} of {}",
              testpath, callCount, inconsistentCallCount);
          logLocationAtDebug();
          throw new FileNotFoundException(testpath.toString());
        }
        return (ObjectMetadata) invocation.callRealMethod();
      }
    };

    // HEAD requests will fail
    doAnswer(notFound).when(s3ClientSpy).getObjectMetadata(
        matchingMetadataRequest(testpath));
  }

}
