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

import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import org.assertj.core.api.Assertions;
import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.AccessDeniedException;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createFiles;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.isBulkDeleteEnabled;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.failIf;
import static org.apache.hadoop.test.LambdaTestUtils.*;
import static org.apache.hadoop.util.functional.RemoteIterators.mappingRemoteIterator;
import static org.apache.hadoop.util.functional.RemoteIterators.toList;

/**
 * ITest for failure handling, primarily multipart deletion.
 */
public class ITestS3AFailureHandling extends AbstractS3ATestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFailureHandling.class);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setBoolean(Constants.ENABLE_MULTI_DELETE, true);
    return conf;
  }

  /**
   * Assert that a read operation returned an EOF value.
   * @param operation specific operation
   * @param readResult result
   */
  private void assertIsEOF(String operation, int readResult) {
    assertEquals("Expected EOF from "+ operation
        + "; got char " + (char) readResult, -1, readResult);
  }

  @Test
  public void testMultiObjectDeleteNoFile() throws Throwable {
    describe("Deleting a missing object");
    removeKeys(getFileSystem(), "ITestS3AFailureHandling/missingFile");
  }

  /**
   * See HADOOP-18112.
   */
  @Test
  public void testMultiObjectDeleteLargeNumKeys() throws Exception {
    S3AFileSystem fs =  getFileSystem();
    Path path = path("largeDir");
    mkdirs(path);
    final boolean bulkDeleteEnabled = isBulkDeleteEnabled(getFileSystem());

    // with single object delete, only create a few files for a faster
    // test run.
    int filesToCreate;
    int pages;
    if (bulkDeleteEnabled) {
      filesToCreate = 1005;
      pages = 5;
    } else {
      filesToCreate = 250;
      pages = 0;
    }


    createFiles(fs, path, 1, filesToCreate, 0);
    RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
            fs.listFiles(path, false);
    List<String> keys  = toList(mappingRemoteIterator(locatedFileStatusRemoteIterator,
        locatedFileStatus -> fs.pathToKey(locatedFileStatus.getPath())));
    // After implementation of paging during multi object deletion,
    // no exception is encountered.
    Long bulkDeleteReqBefore = getNumberOfBulkDeleteRequestsMadeTillNow(fs);
    try (AuditSpan span = span()) {
      fs.removeKeys(buildDeleteRequest(keys.toArray(new String[0])), false);
    }
    Long bulkDeleteReqAfter = getNumberOfBulkDeleteRequestsMadeTillNow(fs);
    // number of delete requests is 5 as we have default page size of 250.

    Assertions.assertThat(bulkDeleteReqAfter - bulkDeleteReqBefore)
            .describedAs("Number of batched bulk delete requests")
            .isEqualTo(pages);
  }

  private Long getNumberOfBulkDeleteRequestsMadeTillNow(S3AFileSystem fs) {
    return fs.getIOStatistics().counters()
            .get(StoreStatisticNames.OBJECT_BULK_DELETE_REQUEST);
  }

  private void removeKeys(S3AFileSystem fileSystem, String... keys)
      throws IOException {
    try (AuditSpan span = span()) {
      fileSystem.removeKeys(buildDeleteRequest(keys), false);
    }
  }

  private List<ObjectIdentifier> buildDeleteRequest(
      final String[] keys) {
    List<ObjectIdentifier> request = new ArrayList<>(
        keys.length);
    for (String key : keys) {
      request.add(ObjectIdentifier.builder().key(key).build());
    }
    return request;
  }

  @Test
  public void testMultiObjectDeleteSomeFiles() throws Throwable {
    Path valid = path("ITestS3AFailureHandling/validFile");
    touch(getFileSystem(), valid);
    NanoTimer timer = new NanoTimer();
    removeKeys(getFileSystem(), getFileSystem().pathToKey(valid),
        "ITestS3AFailureHandling/missingFile");
    timer.end("removeKeys");
  }


  private Path maybeGetCsvPath() {
    Configuration conf = getConfiguration();
    String csvFile = conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    Assume.assumeTrue("CSV test file is not the default",
        DEFAULT_CSVTEST_FILE.equals(csvFile));
    return new Path(csvFile);
  }

  /**
   * Test low-level failure handling with low level delete request.
   */
  @Test
  public void testMultiObjectDeleteNoPermissions() throws Throwable {
    describe("Delete the landsat CSV file and expect it to fail");
    Path csvPath = maybeGetCsvPath();
    S3AFileSystem fs = (S3AFileSystem) csvPath.getFileSystem(
        getConfiguration());
    // create a span, expect it to be activated.
    fs.getAuditSpanSource().createSpan(StoreStatisticNames.OP_DELETE,
        csvPath.toString(), null);
    List<ObjectIdentifier> keys
        = buildDeleteRequest(
            new String[]{
                fs.pathToKey(csvPath),
                "missing-key.csv"
            });
    MultiObjectDeleteException ex = intercept(
        MultiObjectDeleteException.class,
        () -> fs.removeKeys(keys, false));
    final List<Path> undeleted = ex.errors().stream()
        .map(S3Error::key)
        .map(fs::keyToQualifiedPath)
        .collect(Collectors.toList());
    final String undeletedFiles = undeleted.stream()
        .map(Path::toString)
        .collect(Collectors.joining(", "));
    failIf(undeleted.size() != 2,
        "undeleted list size wrong: " + undeletedFiles,
        ex);
    assertTrue("no CSV in " +undeletedFiles, undeleted.contains(csvPath));
  }

  /**
   * Test low-level failure handling with a single-entry file.
   * This is deleted as a single call, so isn't that useful.
   */
  @Test
  public void testSingleObjectDeleteNoPermissionsTranslated() throws Throwable {
    describe("Delete the landsat CSV file and expect it to fail");
    Path csvPath = maybeGetCsvPath();
    S3AFileSystem fs = (S3AFileSystem) csvPath.getFileSystem(
        getConfiguration());
    AccessDeniedException aex = intercept(AccessDeniedException.class,
        () -> fs.delete(csvPath, false));
    Throwable cause = aex.getCause();
    failIf(cause == null, "no nested exception", aex);
  }
}
