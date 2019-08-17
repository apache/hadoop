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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Assume;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.InputStream;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.FailureInjectionPolicy.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests S3A behavior under forced inconsistency via {@link
 * InconsistentAmazonS3Client}.
 *
 * These tests are for validating expected behavior *without* S3Guard, but
 * may also run with S3Guard enabled.  For tests that validate S3Guard's
 * consistency features, see {@link ITestS3GuardListConsistency}.
 */
public class ITestS3AInconsistency extends AbstractS3ATestBase {

  private static final int OPEN_READ_ITERATIONS = 10;

  public static final int INCONSISTENCY_MSEC = 800;

  private static final int INITIAL_RETRY = 128;

  private static final int RETRIES = 4;

  /** By using a power of 2 for the initial time, the total is a shift left. */
  private static final int TOTAL_RETRY_DELAY = INITIAL_RETRY << RETRIES;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // reduce retry limit so FileNotFoundException cases timeout faster,
    // speeding up the tests
    removeBaseAndBucketOverrides(conf,
        CHANGE_DETECT_SOURCE,
        CHANGE_DETECT_MODE,
        RETRY_LIMIT,
        RETRY_INTERVAL,
        METADATASTORE_AUTHORITATIVE,
        S3_CLIENT_FACTORY_IMPL);
    conf.setClass(S3_CLIENT_FACTORY_IMPL, InconsistentS3ClientFactory.class,
        S3ClientFactory.class);
    conf.set(FAIL_INJECT_INCONSISTENCY_KEY, DEFAULT_DELAY_KEY_SUBSTRING);
    // the reads are always inconsistent
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 1.0f);
    // but the inconsistent time is less than exponential growth of the
    // retry interval (128 -> 256 -> 512 -> 1024
    conf.setLong(FAIL_INJECT_INCONSISTENCY_MSEC, INCONSISTENCY_MSEC);
    conf.setInt(RETRY_LIMIT, RETRIES);
    conf.set(RETRY_INTERVAL, String.format("%dms", INITIAL_RETRY));
    return conf;
  }

  @Test
  public void testGetFileStatus() throws Exception {
    S3AFileSystem fs = getFileSystem();

    // 1. Make sure no ancestor dirs exist
    Path dir = path("ancestor");
    fs.delete(dir, true);
    waitUntilDeleted(dir);

    // 2. Create a descendant file, which implicitly creates ancestors
    // This file has delayed visibility.
    touch(getFileSystem(),
        path("ancestor/file-" + DEFAULT_DELAY_KEY_SUBSTRING));

    // 3. Assert expected behavior.  If S3Guard is enabled, we should be able
    // to get status for ancestor.  If S3Guard is *not* enabled, S3A will
    // fail to infer the existence of the ancestor since visibility of the
    // child file is delayed, and its key prefix search will return nothing.
    try {
      FileStatus status = fs.getFileStatus(dir);
      if (fs.hasMetadataStore()) {
        assertTrue("Ancestor is dir", status.isDirectory());
      } else {
        fail("getFileStatus should fail due to delayed visibility.");
      }
    } catch (FileNotFoundException e) {
      if (fs.hasMetadataStore()) {
        fail("S3Guard failed to list parent of inconsistent child.");
      }
      LOG.info("File not found, as expected.");
    }
  }


  /**
   * Ensure that deleting a file with an open read stream does eventually cause
   * readers to get a FNFE, even with S3Guard and its retries enabled.
   * In real usage, S3Guard should be enabled for all clients that modify the
   * file, so the delete would be immediately recorded in the MetadataStore.
   * Here, however, we test deletion from under S3Guard to make sure it still
   * eventually propagates the FNFE after any retry policies are exhausted.
   */
  @Test
  public void testOpenDeleteRead() throws Exception {
    S3AFileSystem fs = getFileSystem();
    ChangeDetectionPolicy changeDetectionPolicy =
        fs.getChangeDetectionPolicy();
    Assume.assumeFalse("FNF not expected when using a bucket with"
            + " object versioning",
        changeDetectionPolicy.getSource() == Source.VersionId);

    Path p = path("testOpenDeleteRead.txt");
    writeTextFile(fs, p, "1337c0d3z", true);
    try (InputStream s = fs.open(p)) {
      // Disable s3guard, delete file underneath it, re-enable s3guard
      MetadataStore metadataStore = fs.getMetadataStore();
      fs.setMetadataStore(new NullMetadataStore());
      fs.delete(p, false);
      fs.setMetadataStore(metadataStore);
      eventually(TOTAL_RETRY_DELAY * 2, INITIAL_RETRY * 2, () -> {
        intercept(FileNotFoundException.class, () -> s.read());
      });
    }
  }

  /**
   * Test read() path behavior when getFileStatus() succeeds but subsequent
   * read() on the input stream fails due to eventual consistency.
   * There are many points in the InputStream codepaths that can fail. We set
   * a probability of failure and repeat the test multiple times to achieve
   * decent coverage.
   */
  @Test
  public void testOpenFailOnRead() throws Exception {

    S3AFileSystem fs = getFileSystem();

    // 1. Patch in a different failure injection policy with <1.0 probability
    Configuration conf = fs.getConf();
    conf.setFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY, 0.5f);
    InconsistentAmazonS3Client.setFailureInjectionPolicy(fs,
        new FailureInjectionPolicy(conf));

    // 2. Make sure no ancestor dirs exist
    Path dir = path("ancestor");
    fs.delete(dir, true);
    waitUntilDeleted(dir);

    // 3. Create a descendant file, which implicitly creates ancestors
    // This file has delayed visibility.
    describe("creating test file");
    Path path = path("ancestor/file-to-read-" + DEFAULT_DELAY_KEY_SUBSTRING);
    writeTextFile(getFileSystem(), path, "Reading is fun", false);

    // 4. Clear inconsistency so the first getFileStatus() can succeed, if we
    // are not using S3Guard. If we are using S3Guard, it should tolerate the
    // delayed visibility.
    if (!fs.hasMetadataStore()) {
      InconsistentAmazonS3Client.clearInconsistency(fs);
    }

    // ? Do we need multiple iterations when S3Guard is disabled?  For now,
    // leaving it in
    for (int i = 0; i < OPEN_READ_ITERATIONS; i++) {
      doOpenFailOnReadTest(fs, path, i);
    }
  }

  private void doOpenFailOnReadTest(S3AFileSystem fs, Path path, int iteration)
      throws Exception {

    // 4. Open the file
    describe(String.format("i=%d: opening test file", iteration));
    try(InputStream in = fs.open(path)) {
      // 5. Assert expected behavior on read() failure.
      int l = 4;
      byte[] buf = new byte[l];
      describe("reading test file");
      // Use both read() variants
      if ((iteration % 2) == 0) {
        assertEquals(l, in.read(buf, 0, l));
      } else {
        in.read();
      }
    } catch (FileNotFoundException e) {
      if (fs.hasMetadataStore()) {
        LOG.error("Error:", e);
        ContractTestUtils.fail("S3Guard failed to handle fail-on-read", e);
      } else {
        LOG.info("File not found on read(), as expected.");
      }
    }
  }

  private void waitUntilDeleted(final Path p) throws Exception {
    LambdaTestUtils.eventually(30 * 1000, 1000,
        () -> assertPathDoesNotExist("Dir should be deleted", p));
  }
}
