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
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.SELECT_SQL;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;

/**
 * Test S3A remote file change detection.
 */
@RunWith(Parameterized.class)
public class ITestS3ARemoteFileChanged extends AbstractS3ATestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ARemoteFileChanged.class);

  private final String changeDetectionSource;
  private final String changeDetectionMode;
  private final boolean expectChangeException;
  private final boolean expectFileNotFoundException;

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        // make sure it works with invalid config
        {"bogus", "bogus", true, true},

        // test with etag
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_SERVER, true, true},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_CLIENT, true, true},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_WARN, false, true},
        {CHANGE_DETECT_SOURCE_ETAG, CHANGE_DETECT_MODE_NONE, false, true},

        // test with versionId
        // when using server-side versionId, the exceptions shouldn't happen
        // since the previous version will still be available
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_SERVER, false,
            false},

        // with client-side versionId it will behave similar to client-side eTag
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_CLIENT, true,
            true},

        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_WARN, false, true},
        {CHANGE_DETECT_SOURCE_VERSION_ID, CHANGE_DETECT_MODE_NONE, false, true}
    });
  }

  public ITestS3ARemoteFileChanged(String changeDetectionSource,
      String changeDetectionMode,
      boolean expectException,
      boolean expectFileNotFoundException) {
    this.changeDetectionSource = changeDetectionSource;
    this.changeDetectionMode = changeDetectionMode;
    this.expectChangeException = expectException;
    this.expectFileNotFoundException = expectFileNotFoundException;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    removeBucketOverrides(bucketName, conf,
        CHANGE_DETECT_SOURCE,
        CHANGE_DETECT_MODE);
    conf.set(CHANGE_DETECT_SOURCE, changeDetectionSource);
    conf.set(CHANGE_DETECT_MODE, changeDetectionMode);
    if (conf.getClass(S3_METADATA_STORE_IMPL, MetadataStore.class) ==
        NullMetadataStore.class) {
      // favor LocalMetadataStore over NullMetadataStore
      conf.setClass(S3_METADATA_STORE_IMPL,
          LocalMetadataStore.class, MetadataStore.class);
    }
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

  /**
   * Tests reading a file that is changed while the reader's InputStream is open
   */
  @Test
  public void testReadFileChangedStreamOpen() throws Throwable {
    final int originalLength = 8192;
    final byte[] originalDataset = dataset(originalLength, 'a', 32);
    final int newLength = originalLength + 1;
    final byte[] newDataset = dataset(newLength, 'A', 32);
    final S3AFileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, originalDataset, originalDataset.length,
        1024, false);

    if (fs.getChangeDetectionPolicy().getSource() == Source.VersionId) {
      // skip versionId tests if the bucket doesn't have object versioning
      // enabled
      Assume.assumeTrue(
          "Target filesystem does not support versioning",
          fs.getObjectMetadata(fs.pathToKey(testpath)).getVersionId() != null);
    }

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

      if (expectChangeException) {
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read());
      } else {
        instream.read();
      }

      byte[] buf = new byte[256];

      // seek backward
      instream.seek(0);

      if (expectChangeException) {
        intercept(RemoteFileChangedException.class, "", "read",
            () -> instream.read(buf));
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

      if (expectFileNotFoundException) {
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
   * version tracked in the metadata store.  This simulates the condition
   * of an eventually consistent read-after-overwrite.  The metadata store
   * will track the new version but reads from S3 may not yet return that
   * version.
   */
  @Test
  public void testReadFileChangedNotVisibleInS3() throws Throwable {
    // This test is invalid for server-side versionId.  Passing a bogus
    // versionId as a server-side qualification results in a 400 (Bad Request)
    // saying the versionId is invalid.  Maybe that's what happens in a
    // read-after-overwrite inconsistency scenario?  Or maybe if you read
    // with versionId server-side qualified then read-after-overwrite
    // inconsistency is impossible.
    Assume.assumeFalse(
        changeDetectionMode.equals(CHANGE_DETECT_MODE_SERVER) &&
            changeDetectionSource.equals(CHANGE_DETECT_SOURCE_VERSION_ID));
    final S3AFileSystem fs = getFileSystem();
    final Path testpath = writeOutOfSyncFileVersion(fs);
    final FSDataInputStream instream = fs.open(testpath);
    if (expectChangeException) {
      intercept(RemoteFileChangedException.class, "", "read()",
          () -> {
            instream.read();
          });
    } else {
      instream.read();
    }
  }

  /**
   * Tests using S3 Select on a file where the version visible in S3 does not
   * match the version tracked in the metadata store.
   */
  @Test
  public void testSelectChangedFile() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    final Path testpath = writeOutOfSyncFileVersion(fs);
    if (expectChangeException) {
      interceptFuture(RemoteFileChangedException.class, "select",
          fs.openFile(testpath)
              .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build());
    }
    else {
      fs.openFile(testpath)
          .must(SELECT_SQL, "SELECT * FROM S3OBJECT").build();
    }
  }

  /**
   * Writes a file with bogus ETag and versionId in the metadata store such
   * that the metadata is out of sync with S3.  Attempts to read such a file
   * should always result in {@link RemoteFileChangedException}.
   */
  private Path writeOutOfSyncFileVersion(S3AFileSystem fs) throws IOException {
    final Path testpath = path("outOfSync" +
        System.currentTimeMillis() + ".txt");
    final byte[] dataset = dataset(8192, 'a', 32);
    writeDataset(fs, testpath, dataset, dataset.length,
        1024, false);
    S3AFileStatus fileStatus = (S3AFileStatus) fs.getFileStatus(testpath);
    S3AFileStatus newStatus = new S3AFileStatus(fileStatus.getLen(), fileStatus.getModificationTime(),
        fileStatus.getAccessTime(),
        fileStatus.getPath(), fileStatus.getBlockSize(), fileStatus.getOwner(), fileStatus.getGroup(),
        fileStatus.getPermission(), "bogusETag", "bogusVersionId");

    fs.getMetadataStore().put(
        new PathMetadata(newStatus, Tristate.FALSE, false));
    return testpath;
  }
}
