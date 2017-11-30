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

package org.apache.hadoop.fs.s3a.s3guard;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.*;

/**
 * Test the S3Guard CLI entry point.
 */
public class TestS3GuardCLI extends Assert {

  /**
   * Run a S3GuardTool command from a varags list.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(String... args)
      throws Exception {
    Configuration conf = new Configuration(false);
    return S3GuardTool.run(conf, args);
  }

  /**
   * Run a S3GuardTool command from a varags list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param status expected status code of an exception
   * @param args argument list
   * @throws Exception any exception
   */
  protected void runToFailure(int status, String... args)
      throws Exception {
    ExitUtil.ExitException ex =
        LambdaTestUtils.intercept(ExitUtil.ExitException.class,
            () -> run(args));
    if (ex.status != status) {
      throw ex;
    }
  }

  @Test
  public void testInfoNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, BucketInfo.NAME);
  }

  @Test
  public void testInfoWrongFilesystem() throws Throwable {
    runToFailure(INVALID_ARGUMENT,
        BucketInfo.NAME, "file://");
  }

  @Test
  public void testNoCommand() throws Throwable {
    runToFailure(E_USAGE);
  }

  @Test
  public void testUnknownCommand() throws Throwable {
    runToFailure(E_USAGE, "unknown");
  }

  @Test
  public void testPruneNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Prune.NAME);
  }

  @Test
  public void testDiffNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Diff.NAME);
  }

  @Test
  public void testImportNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Import.NAME);
  }

  @Test
  public void testDestroyNoArgs() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Destroy.NAME);
  }

  @Test
  public void testDestroyUnknownTableNoRegion() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Destroy.NAME,
        "-meta", "dynamodb://ireland-team");
  }

  @Test
  public void testInitBucketAndRegion() throws Throwable {
    runToFailure(INVALID_ARGUMENT, Init.NAME,
        "-meta", "dynamodb://ireland-team",
        "-region", "eu-west-1",
        S3ATestConstants.DEFAULT_CSVTEST_FILE
    );
  }

}
