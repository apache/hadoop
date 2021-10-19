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
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;

import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.checkListingContainsPath;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.checkListingDoesNotContainPath;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.junit.Assume.assumeTrue;

public class ITestAuthoritativePath extends AbstractS3ATestBase {

  public Path testRoot;

  private S3AFileSystem fullyAuthFS;
  private S3AFileSystem rawFS;

  private MetadataStore ms;

  @Before
  public void setup() throws Exception {
    super.setup();

    long timestamp = System.currentTimeMillis();
    testRoot = path("" + timestamp);

    S3AFileSystem fs = getFileSystem();
    // These test will fail if no ms
    assumeTrue("FS needs to have a metadatastore.",
        fs.hasMetadataStore());
    assumeTrue("Metadatastore should persist authoritative bit",
        metadataStorePersistsAuthoritativeBit(fs.getMetadataStore()));

    // This test setup shares a single metadata store across instances,
    // so that test runs with a local FS work.
    // but this needs to be addressed in teardown, where the Auth fs
    // needs to be detached from the metadata store before it is closed,
    ms = fs.getMetadataStore();

    fullyAuthFS = createFullyAuthFS();
    assertTrue("No S3Guard store for fullyAuthFS",
        fullyAuthFS.hasMetadataStore());
    assertTrue("Authoritative mode off in fullyAuthFS",
        fullyAuthFS.hasAuthoritativeMetadataStore());

    rawFS = createRawFS();
    assertFalse("UnguardedFS still has S3Guard",
        rawFS.hasMetadataStore());
  }

  private void cleanUpFS(S3AFileSystem fs) {
    // detach from the (shared) metadata store.
    if (fs != null) {
      fs.setMetadataStore(new NullMetadataStore());
    }

    IOUtils.cleanupWithLogger(LOG, fs);
  }

  @Override
  public void teardown() throws Exception {
    if (fullyAuthFS != null) {
      fullyAuthFS.delete(testRoot, true);
    }

    cleanUpFS(fullyAuthFS);
    cleanUpFS(rawFS);
    super.teardown();
  }

  private S3AFileSystem createFullyAuthFS()
      throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    config.setBoolean(METADATASTORE_AUTHORITATIVE, true);
    final S3AFileSystem newFS = createFS(uri, config);
    // set back the same metadata store instance
    newFS.setMetadataStore(ms);
    return newFS;
  }

  private S3AFileSystem createSinglePathAuthFS(String authPath)
      throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    config.set(AUTHORITATIVE_PATH, authPath.toString());
    final S3AFileSystem newFS = createFS(uri, config);
    // set back the same metadata store instance
    newFS.setMetadataStore(ms);
    return newFS;
  }

  private S3AFileSystem createMultiPathAuthFS(String first, String middle, String last)
        throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    config.set(AUTHORITATIVE_PATH, first + "," + middle + "," + last);
    final S3AFileSystem newFS = createFS(uri, config);
    // set back the same metadata store instance
    newFS.setMetadataStore(ms);
    return newFS;
  }

  private S3AFileSystem createRawFS() throws Exception {
    S3AFileSystem testFS = getFileSystem();
    Configuration config = new Configuration(testFS.getConf());
    URI uri = testFS.getUri();

    removeBaseAndBucketOverrides(uri.getHost(), config,
        S3_METADATA_STORE_IMPL);
    removeBaseAndBucketOverrides(uri.getHost(), config,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    return createFS(uri, config);
  }

  /**
   * Create and initialize a new filesystem.
   * This filesystem MUST be closed in test teardown.
   * @param uri FS URI
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

  private void runTestOutsidePath(S3AFileSystem partiallyAuthFS, Path nonAuthPath) throws Exception {
    Path inBandPath = new Path(nonAuthPath, "out-of-path-in-band");
    Path outOfBandPath = new Path(nonAuthPath, "out-of-path-out-of-band");

    touch(fullyAuthFS, inBandPath);

    // trigger an authoritative write-back
    fullyAuthFS.listStatus(inBandPath.getParent());

    touch(rawFS, outOfBandPath);

    // listing lacks outOfBandPath => short-circuited by auth mode
    checkListingDoesNotContainPath(fullyAuthFS, outOfBandPath);

    // partiallyAuthFS differs from fullyAuthFS because we're outside the path
    checkListingContainsPath(partiallyAuthFS, outOfBandPath);

    // sanity check that in-band operations are always visible
    checkListingContainsPath(fullyAuthFS, inBandPath);
    checkListingContainsPath(partiallyAuthFS, inBandPath);

  }

  private void runTestInsidePath(S3AFileSystem partiallyAuthFS, Path authPath) throws Exception {
    Path inBandPath = new Path(authPath, "in-path-in-band");
    Path outOfBandPath = new Path(authPath, "in-path-out-of-band");

    touch(fullyAuthFS, inBandPath);

    // trigger an authoritative write-back
    fullyAuthFS.listStatus(inBandPath.getParent());

    touch(rawFS, outOfBandPath);

    // listing lacks outOfBandPath => short-circuited by auth mode
    checkListingDoesNotContainPath(fullyAuthFS, outOfBandPath);
    checkListingDoesNotContainPath(partiallyAuthFS, outOfBandPath);

    // sanity check that in-band operations are always successful
    checkListingContainsPath(fullyAuthFS, inBandPath);
    checkListingContainsPath(partiallyAuthFS, inBandPath);
  }

  @Test
  public void testSingleAuthPath() throws Exception {
    Path authPath = new Path(testRoot, "testSingleAuthPath-auth");
    Path nonAuthPath = new Path(testRoot, "testSingleAuthPath");
    S3AFileSystem fs = createSinglePathAuthFS(authPath.toString());
    try {
      assertTrue("No S3Guard store for partially authoritative FS",
            fs.hasMetadataStore());

      runTestInsidePath(fs, authPath);
      runTestOutsidePath(fs, nonAuthPath);
    } finally {
      cleanUpFS(fs);
    }
  }

  @Test
  public void testAuthPathWithOtherBucket() throws Exception {
    Path authPath;
    Path nonAuthPath;
    S3AFileSystem fs = null;
    String landsat = "s3a://landsat-pds/data";
    String decoy2 = "/decoy2";

    try {
      authPath = new Path(testRoot, "testMultiAuthPath-first");
      nonAuthPath = new Path(testRoot, "nonAuth-1");
      fs = createMultiPathAuthFS(authPath.toString(), landsat, decoy2);
      assertTrue("No S3Guard store for partially authoritative FS",
          fs.hasMetadataStore());

      runTestInsidePath(fs, authPath);
      runTestOutsidePath(fs, nonAuthPath);
    } finally {
      cleanUpFS(fs);
    }
  }

  @Test
  public void testMultiAuthPath() throws Exception {
    Path authPath;
    Path nonAuthPath;
    S3AFileSystem fs = null;
    String decoy1 = "/decoy1";
    String decoy2 = "/decoy2";

    try {
      authPath = new Path(testRoot, "testMultiAuthPath-first");
      nonAuthPath = new Path(testRoot, "nonAuth-1");
      fs = createMultiPathAuthFS(authPath.toString(), decoy1, decoy2);
      assertTrue("No S3Guard store for partially authoritative FS",
            fs.hasMetadataStore());

      runTestInsidePath(fs, authPath);
      runTestOutsidePath(fs, nonAuthPath);
    } finally {
      cleanUpFS(fs);
    }

    try {
      authPath = new Path(testRoot, "testMultiAuthPath-middle");
      nonAuthPath = new Path(testRoot, "nonAuth-2");
      fs = createMultiPathAuthFS(decoy1, authPath.toString(), decoy2);
      assertTrue("No S3Guard store for partially authoritative FS",
            fs.hasMetadataStore());

      runTestInsidePath(fs, authPath);
      runTestOutsidePath(fs, nonAuthPath);
    } finally {
      cleanUpFS(fs);
    }

    try {
      authPath = new Path(testRoot, "testMultiAuthPath-last");
      nonAuthPath = new Path(testRoot, "nonAuth-3");
      fs = createMultiPathAuthFS(decoy1, decoy2, authPath.toString());
      assertTrue("No S3Guard store for partially authoritative FS",
            fs.hasMetadataStore());

      runTestInsidePath(fs, authPath);
      runTestOutsidePath(fs, nonAuthPath);
    } finally {
      cleanUpFS(fs);
    }
  }

  @Test
  public void testPrefixVsDirectory() throws Exception {
    S3AFileSystem fs = createSinglePathAuthFS("/auth");
    Collection<String> authPaths = S3Guard.getAuthoritativePaths(fs);

    try{
      Path totalMismatch = new Path(testRoot, "/non-auth");
      assertFalse(S3Guard.allowAuthoritative(totalMismatch, fs,
          false, authPaths));

      Path prefixMatch = new Path(testRoot, "/authoritative");
      assertFalse(S3Guard.allowAuthoritative(prefixMatch, fs,
          false, authPaths));

      Path directoryMatch = new Path(testRoot, "/auth/oritative");
      assertTrue(S3Guard.allowAuthoritative(directoryMatch, fs,
          false, authPaths));

      Path unqualifiedMatch = new Path(testRoot.toUri().getPath(), "/auth/oritative");
      assertTrue(S3Guard.allowAuthoritative(unqualifiedMatch, fs,
          false, authPaths));
    } finally {
      cleanUpFS(fs);
    }
  }
}
