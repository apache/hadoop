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

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.AccessDeniedException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.failIf;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

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

  private void removeKeys(S3AFileSystem fileSystem, String... keys)
      throws IOException {
    try (AuditSpan span = span()) {
      fileSystem.removeKeys(buildDeleteRequest(keys), false);
    }
  }

  private List<DeleteObjectsRequest.KeyVersion> buildDeleteRequest(
      final String[] keys) {
    List<DeleteObjectsRequest.KeyVersion> request = new ArrayList<>(
        keys.length);
    for (String key : keys) {
      request.add(new DeleteObjectsRequest.KeyVersion(key));
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
    List<DeleteObjectsRequest.KeyVersion> keys
        = buildDeleteRequest(
            new String[]{
                fs.pathToKey(csvPath),
                "missing-key.csv"
            });
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
