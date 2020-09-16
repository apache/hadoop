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
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport;

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
import static org.apache.hadoop.fs.s3a.impl.TestPartialDeleteFailures.keysToDelete;
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
    fileSystem.removeKeys(buildDeleteRequest(keys), false, null);
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
    List<DeleteObjectsRequest.KeyVersion> keys
        = buildDeleteRequest(
            new String[]{
                fs.pathToKey(csvPath),
                "missing-key.csv"
            });
    MultiObjectDeleteException ex = intercept(
        MultiObjectDeleteException.class,
        () -> fs.removeKeys(keys, false, null));

    final List<Path> undeleted
        = extractUndeletedPaths(ex, fs::keyToQualifiedPath);
    String undeletedFiles = join(undeleted);
    failIf(undeleted.size() != 2,
        "undeleted list size wrong: " + undeletedFiles,
        ex);
    assertTrue("no CSV in " +undeletedFiles, undeleted.contains(csvPath));

    // and a full split, after adding a new key
    String marker = "/marker";
    Path markerPath = fs.keyToQualifiedPath(marker);
    keys.add(new DeleteObjectsRequest.KeyVersion(marker));

    Pair<List<KeyPath>, List<KeyPath>> pair =
        new MultiObjectDeleteSupport(fs.createStoreContext(), null)
        .splitUndeletedKeys(ex, keys);
    assertEquals(undeleted, toPathList(pair.getLeft()));
    List<KeyPath> right = pair.getRight();
    Assertions.assertThat(right)
        .hasSize(1);
    assertEquals(markerPath, right.get(0).getPath());
  }

  /**
   * See what happens when you delete two entries which do not exist.
   * It must not raise an exception.
   */
  @Test
  public void testMultiObjectDeleteMissingEntriesSucceeds() throws Throwable {
    describe("Delete keys which don't exist");
    Path base = path("missing");
    S3AFileSystem fs = getFileSystem();
    List<DeleteObjectsRequest.KeyVersion> keys = keysToDelete(
        Lists.newArrayList(new Path(base, "1"), new Path(base, "2")));
    fs.removeKeys(keys, false, null);
  }

  private String join(final Iterable iterable) {
    return "[" + StringUtils.join(iterable, ",") + "]";
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
