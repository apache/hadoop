/**
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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/** Test Gridmix exit codes for different error types */
public class TestGridmixExitCodes {

  @BeforeClass
  public static void init() throws IOException {
    TestGridmixSubmission.init();
  }

  @AfterClass
  public static void shutDown() throws IOException {
    TestGridmixSubmission.shutDown();
  }

  /**
   * Test Gridmix exit codes for different error types like
   * <li> when less than 2 arguments are provided to Gridmix
   * <li> when input data dir already exists and -generate option is specified
   * <li> Specifying negative input-data-size using -generate option
   * <li> specifying a non-existing option to Gridmix command-line
   * <li> Wrong combination of arguments to Gridmix run
   * <li> Unable to create ioPath dir
   * <li> Bad class specified as a user resolver
   */
  @Test
  public void testGridmixExitCodes() throws Exception {
    testTooFewArgs();
    testNegativeInputDataSize();
    testNonexistingOption();
    testWrongArgs();
    testBadUserResolvers();

    testBadIOPath();
    testExistingInputDataDir();
  }

  /**
   * Specify less than 2 arguments to Gridmix and verify the exit code
   */
  private void testTooFewArgs() throws Exception {
    int expectedExitCode = Gridmix.ARGS_ERROR;
    // Provide only 1 argument to Gridmix
    String[] argv = new String[1];
    argv[0] = "ioPath";
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * Specify -ve input data size to be generated and verify the exit code
   */
  private void testNegativeInputDataSize() throws Exception {
    int expectedExitCode = Gridmix.ARGS_ERROR;

    String[] argv = new String[4];
    argv[0] = "-generate";
    argv[1] = "-5m"; // -ve size
    argv[2] = "ioPath";
    argv[3] = "-";
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * Specify a non-existing option to Gridmix command-line and verify
   * the exit code
   */
  private void testNonexistingOption() throws Exception {
    int expectedExitCode = Gridmix.ARGS_ERROR;
    String[] argv = new String[3];
    argv[0] = "-unknownOption";
    argv[1] = "dummyArg1";
    argv[2] = "dummyArg2";
    // No need to call prepareArgs() as -unknownOption should make Gridmix fail
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * Specify wrong combination of arguments to Gridmix run and verify
   * the exit code. This is done by specifying RoundRobinUserResolver and not
   * specifying -users option
   */
  private void testWrongArgs() throws Exception {
    int expectedExitCode = Gridmix.ARGS_ERROR;
    String[] argv = TestGridmixSubmission.prepareArgs(true,
        RoundRobinUserResolver.class.getName());
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * <li> Specify a non-existing class as a userResolver class and validate the
   * exit code
   * <li> Specify an existing class which doesn't implement {@link UserResolver}
   * as a userResolver class and validate the exit code
   */
  private void testBadUserResolvers() throws Exception {
    int expectedExitCode = Gridmix.ARGS_ERROR;

    // Verify the case of an existing class that doesn't implement the
    // interface UserResolver
    String[] argv = TestGridmixSubmission.prepareArgs(true,
        WrongUserResolver.class.getName());
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);

    // Verify the case of a nonexisting class name as user resolver class
    argv = TestGridmixSubmission.prepareArgs(true, "NonExistingUserResolver");
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /** A class which doesn't implement the interface {@link UserResolver} */
  static class WrongUserResolver {}


  /**
   * Setup such that creation of ioPath dir fails and verify the exit code
   */
  private void testBadIOPath() throws Exception {
    // Create foo as a file (not as a directory).
    GridmixTestUtils.dfs.create(TestGridmixSubmission.ioPath);
    // This ioPath cannot be created as a directory now.
    int expectedExitCode = Gridmix.STARTUP_FAILED_ERROR;

    String[] argv = TestGridmixSubmission.prepareArgs(true,
        EchoUserResolver.class.getName());
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * Create input data dir and specify -generate option verify the exit code
   */
  private void testExistingInputDataDir() throws Exception {
    createInputDataDirectory(TestGridmixSubmission.ioPath);
    int expectedExitCode = Gridmix.STARTUP_FAILED_ERROR;

    String[] argv = TestGridmixSubmission.prepareArgs(true,
        EchoUserResolver.class.getName());
    TestGridmixSubmission.testGridmixExitCode(true, argv, expectedExitCode);
  }

  /**
   * Create input data directory of Gridmix run
   * @param ioPath ioPath argument of Gridmix run
   */
  private static void createInputDataDirectory(Path ioPath)
      throws IOException {
    Path inputDir = Gridmix.getGridmixInputDataPath(ioPath);
    FileSystem.mkdirs(GridmixTestUtils.dfs, inputDir,
                      new FsPermission((short)0777));
  }
}
