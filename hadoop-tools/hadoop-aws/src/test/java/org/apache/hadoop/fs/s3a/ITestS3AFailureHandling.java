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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVPath;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test S3A Failure translation.
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
    List<DeleteObjectsRequest.KeyVersion> request = new ArrayList<>(
        keys.length);
    for (String key : keys) {
      request.add(new DeleteObjectsRequest.KeyVersion(key));
    }
    fileSystem.removeKeys(request, false, false);
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

  @Test
  public void testMultiObjectDeleteNoPermissions() throws Throwable {
    Path testFile = getLandsatCSVPath(getConfiguration());
    S3AFileSystem fs = (S3AFileSystem)testFile.getFileSystem(
        getConfiguration());
    intercept(MultiObjectDeleteException.class,
        () -> removeKeys(fs, fs.pathToKey(testFile)));
  }
}
