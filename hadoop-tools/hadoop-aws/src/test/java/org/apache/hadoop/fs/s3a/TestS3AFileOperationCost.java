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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.MetricDiff;
import static org.apache.hadoop.test.GenericTestUtils.getTestDir;

/**
 * Use metrics to assert about the cost of file status queries.
 * {@link S3AFileSystem#getFileStatus(Path)}.
 */
public class TestS3AFileOperationCost extends AbstractFSContractTestBase {

  private MetricDiff metadataRequests;
  private MetricDiff listRequests;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AFileOperationCost.class);

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
  }

  @Test
  public void testCostOfGetFileStatusOnFile() throws Throwable {
    describe("performing getFileStatus on a file");
    Path simpleFile = path("simple.txt");
    S3AFileSystem fs = getFileSystem();
    touch(fs, simpleFile);
    resetMetricDiffs();
    S3AFileStatus status = fs.getFileStatus(simpleFile);
    assertTrue("not a file: " + status, status.isFile());
    metadataRequests.assertDiffEquals(1);
    listRequests.assertDiffEquals(0);
  }

  private void resetMetricDiffs() {
    reset(metadataRequests, listRequests);
  }

  @Test
  public void testCostOfGetFileStatusOnEmptyDir() throws Throwable {
    describe("performing getFileStatus on an empty directory");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("empty");
    fs.mkdirs(dir);
    resetMetricDiffs();
    S3AFileStatus status = fs.getFileStatus(dir);
    assertTrue("not empty: " + status, status.isEmptyDirectory());
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(0);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingFile() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missing");
    resetMetricDiffs();
    try {
      S3AFileStatus status = fs.getFileStatus(path);
      fail("Got a status back from a missing file path " + status);
    } catch (FileNotFoundException expected) {
      // expected
    }
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfGetFileStatusOnMissingSubPath() throws Throwable {
    describe("performing getFileStatus on a missing file");
    S3AFileSystem fs = getFileSystem();
    Path path = path("missingdir/missingpath");
    resetMetricDiffs();
    try {
      S3AFileStatus status = fs.getFileStatus(path);
      fail("Got a status back from a missing file path " + status);
    } catch (FileNotFoundException expected) {
      // expected
    }
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfGetFileStatusOnNonEmptyDir() throws Throwable {
    describe("performing getFileStatus on a non-empty directory");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("empty");
    fs.mkdirs(dir);
    Path simpleFile = new Path(dir, "simple.txt");
    touch(fs, simpleFile);
    resetMetricDiffs();
    S3AFileStatus status = fs.getFileStatus(dir);
    if (status.isEmptyDirectory()) {
      // erroneous state
      String fsState = fs.toString();
      fail("FileStatus says directory isempty: " + status
          + "\n" + ContractTestUtils.ls(fs, dir)
          + "\n" + fsState);
    }
    metadataRequests.assertDiffEquals(2);
    listRequests.assertDiffEquals(1);
  }

  @Test
  public void testCostOfCopyFromLocalFile() throws Throwable {
    describe("testCostOfCopyFromLocalFile");
    File localTestDir = getTestDir("tmp");
    localTestDir.mkdirs();
    File tmpFile = File.createTempFile("tests3acost", ".txt",
        localTestDir);
    tmpFile.delete();
    try {
      URI localFileURI = tmpFile.toURI();
      FileSystem localFS = FileSystem.get(localFileURI,
          getFileSystem().getConf());
      Path localPath = new Path(localFileURI);
      int len = 10 * 1024;
      byte[] data = dataset(len, 'A', 'Z');
      writeDataset(localFS, localPath, data, len, 1024, true);
      S3AFileSystem s3a = getFileSystem();
      MetricDiff copyLocalOps = new MetricDiff(s3a,
          INVOCATION_COPY_FROM_LOCAL_FILE);
      MetricDiff putRequests = new MetricDiff(s3a,
          OBJECT_PUT_REQUESTS);
      MetricDiff putBytes = new MetricDiff(s3a,
          OBJECT_PUT_BYTES);

      Path remotePath = path("copied");
      s3a.copyFromLocalFile(false, true, localPath, remotePath);
      verifyFileContents(s3a, remotePath, data);
      copyLocalOps.assertDiffEquals(1);
      putRequests.assertDiffEquals(1);
      putBytes.assertDiffEquals(len);
      // print final stats
      LOG.info("Filesystem {}", s3a);
    } finally {
      tmpFile.delete();
    }
  }
}
