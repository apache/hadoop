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

package org.apache.hadoop.fs.s3a.select;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.select.SelectConstants.SELECT_SQL;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.SELECT_UNSUPPORTED;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_UNSUPPORTED_VERSION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.ExitUtil.disableSystemExit;

/**
 * Verify that s3 select is unsupported.
 */
public class ITestSelectUnsupported extends AbstractS3ATestBase {

  /**
   * S3 Select SQL statement.
   */
  private static final String STATEMENT = "SELECT *" +
      " FROM S3Object s" +
      " WHERE s._1 = 'foo'";

  /**
   * A {@code .must(SELECT_SQL, _)} option MUST raise {@code UnsupportedOperationException}.
   */
  @Test
  public void testSelectOpenFileMustFailure() throws Throwable {

    intercept(UnsupportedOperationException.class, SELECT_UNSUPPORTED, () ->
        getFileSystem().openFile(methodPath())
            .must(SELECT_SQL, STATEMENT)
            .build()
            .get());
  }

  /**
   * A {@code .opt(SELECT_SQL, _)} option is ignored..
   */
  @Test
  public void testSelectOpenFileMayIsIgnored() throws Throwable {

    final Path path = methodPath();
    final S3AFileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    fs.openFile(path)
        .opt(SELECT_SQL, STATEMENT)
        .build()
        .get()
        .close();
  }

  @Test
  public void testPathCapabilityNotAvailable() throws Throwable {
    describe("verify that the FS lacks the path capability");
    Assertions.assertThat(getFileSystem().hasPathCapability(methodPath(), SELECT_SQL))
        .describedAs("S3 Select reported as present")
        .isFalse();
  }

  @Test
  public void testS3GuardToolFails() throws Throwable {

    // ensure that the command doesn't actually exit the VM.
    disableSystemExit();
    final ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class, SELECT_UNSUPPORTED,
            () -> S3GuardTool.main(new String[]{
                "select", "-sql", STATEMENT
            }));
    Assertions.assertThat(ex.getExitCode())
        .describedAs("exit code of exception")
        .isEqualTo(EXIT_UNSUPPORTED_VERSION);
  }
}
