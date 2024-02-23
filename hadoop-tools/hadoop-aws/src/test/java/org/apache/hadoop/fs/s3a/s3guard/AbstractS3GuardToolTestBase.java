/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.UnknownStoreException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.BucketInfo.IS_MARKER_AWARE;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.E_S3GUARD_UNSUPPORTED;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.INVALID_ARGUMENT;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.UNSUPPORTED_COMMANDS;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.runS3GuardCommand;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.MARKERS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Common functionality for S3GuardTool test cases.
 */
public abstract class AbstractS3GuardToolTestBase extends AbstractS3ATestBase {

  protected static final String S3A_THIS_BUCKET_DOES_NOT_EXIST
      = "s3a://this-bucket-does-not-exist-00000000000";

  /**
   * List of tools to close in test teardown.
   */
  private final List<S3GuardTool> toolsToClose = new ArrayList<>();

  /**
   * Declare that the tool is to be closed in teardown.
   * @param tool tool to close
   * @return the tool.
   */
  protected <T extends S3GuardTool> T toClose(T tool) {
    toolsToClose.add(tool);
    return tool;
  }

  protected static void expectResult(int expected,
      String message,
      S3GuardTool tool,
      String... args) throws Exception {
    assertEquals(message, expected, tool.run(args));
  }

  /**
   * Expect a command to succeed.
   * @param message any extra text to include in the assertion error message
   * @param tool tool to run
   * @param args arguments to the command
   * @return the output of any successful run
   * @throws Exception failure
   */
  public static String expectSuccess(
      String message,
      S3GuardTool tool,
      Object... args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    exec(SUCCESS, message, tool, buf, args);
    return buf.toString();
  }

  /**
   * Run a S3GuardTool command from a varags list.
   * @param conf configuration
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Configuration conf, Object... args)
      throws Exception {
    return runS3GuardCommand(conf, args);
  }

  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Object... args) throws Exception {
    return runS3GuardCommand(getConfiguration(), args);
  }

  /**
   * Run a S3GuardTool command from a varags list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param status expected status code of the exception
   * @param args argument list
   * @throws Exception any exception
   */
  protected void runToFailure(int status, Object... args)
      throws Exception {
    final Configuration conf = getConfiguration();
    ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class, () ->
            runS3GuardCommand(conf, args));
    if (ex.status != status) {
      throw ex;
    }
  }

  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    toolsToClose.forEach(t -> IOUtils.cleanupWithLogger(LOG, t));
  }

  @Test
  public void testBucketInfoUnguarded() throws Exception {
    final Configuration conf = getConfiguration();
    URI fsUri = getFileSystem().getUri();

    // run a bucket info command
    S3GuardTool.BucketInfo infocmd = toClose(new S3GuardTool.BucketInfo(conf));
    String info = exec(infocmd, S3GuardTool.BucketInfo.NAME,
        "-" + S3GuardTool.BucketInfo.UNGUARDED_FLAG,
        fsUri.toString());

    assertTrue("Output should contain information about S3A client " + info,
        info.contains("S3A Client"));
  }

  /**
   * Verify that the {@code -markers aware} option works.
   * This test case is in this class for ease of backporting.
   */
  @Test
  public void testBucketInfoMarkerAware() throws Throwable {
    final Configuration conf = getConfiguration();
    URI fsUri = getFileSystem().getUri();

    // run a bucket info command
    S3GuardTool.BucketInfo infocmd = toClose(new S3GuardTool.BucketInfo(conf));
    String info = exec(infocmd, S3GuardTool.BucketInfo.NAME,
        "-" + MARKERS, S3GuardTool.BucketInfo.MARKERS_AWARE,
        fsUri.toString());

    assertTrue("Output should contain information about S3A client " + info,
        info.contains(IS_MARKER_AWARE));
  }

  /**
   * Verify that the {@code -markers} option fails on unknown options.
   * This test case is in this class for ease of backporting.
   */
  @Test
  public void testBucketInfoMarkerPolicyUnknown() throws Throwable {
    final Configuration conf = getConfiguration();
    URI fsUri = getFileSystem().getUri();

    // run a bucket info command and expect failure
    S3GuardTool.BucketInfo infocmd = toClose(new S3GuardTool.BucketInfo(conf));
    intercept(ExitUtil.ExitException.class, "" + EXIT_NOT_ACCEPTABLE, () ->
        exec(infocmd, S3GuardTool.BucketInfo.NAME,
            "-" + MARKERS, "unknown",
            fsUri.toString()));
  }

  /**
   * Make an S3GuardTool of the specific subtype with binded configuration
   * to a nonexistent table.
   * @param tool
   */
  private S3GuardTool makeBindedTool(Class<? extends S3GuardTool> tool)
      throws Exception {
    Configuration conf = getConfiguration();
    return tool.getDeclaredConstructor(Configuration.class).newInstance(conf);
  }

  @Test
  public void testToolsNoBucket() throws Throwable {
    List<Class<? extends S3GuardTool>> tools =
        Arrays.asList(
            S3GuardTool.BucketInfo.class,
            S3GuardTool.Uploads.class);

    for (Class<? extends S3GuardTool> tool : tools) {
      S3GuardTool cmdR = makeBindedTool(tool);
      describe("Calling " + cmdR.getName() + " on a bucket that does not exist.");
      String[] argsR = new String[]{
          cmdR.getName(),
          S3A_THIS_BUCKET_DOES_NOT_EXIST
      };
      intercept(UnknownStoreException.class, () -> {
        final int e = cmdR.run(argsR);
        return String.format("Outcome of %s on missing bucket: %d", tool, e);
      });
    }
  }

  @Test
  public void testToolsNoArgsForBucket() throws Throwable {
    List<Class<? extends S3GuardTool>> tools =
        Arrays.asList(
            S3GuardTool.BucketInfo.class,
            S3GuardTool.Uploads.class);

    for (Class<? extends S3GuardTool> tool : tools) {
      S3GuardTool cmdR = makeBindedTool(tool);
      describe("Calling " + cmdR.getName() + " without any arguments.");
      assertExitCode(INVALID_ARGUMENT,
          intercept(ExitUtil.ExitException.class,
              () -> cmdR.run(new String[]{tool.getName()})));
    }
  }

  @Test
  public void testUnsupported() throws Throwable {
    describe("Verify the unsupported tools are rejected");
    for (String tool : UNSUPPORTED_COMMANDS) {
      describe("Probing %s", tool);
      runToFailure(E_S3GUARD_UNSUPPORTED, tool);
    }
  }

  @Test
  public void testProbeForMagic() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    String name = fs.getUri().toString();
    S3GuardTool.BucketInfo cmd = new S3GuardTool.BucketInfo(
        getConfiguration());
    // this must always work
    exec(cmd, S3GuardTool.BucketInfo.MAGIC_FLAG, name);
  }

  /**
   * Assert that an exit exception had a specific error code.
   * @param expectedErrorCode expected code.
   * @param e exit exception
   * @throws AssertionError with the exit exception nested inside
   */
  protected void assertExitCode(final int expectedErrorCode,
      final ExitUtil.ExitException e) {
    if (e.getExitCode() != expectedErrorCode) {
      throw new AssertionError("Expected error code " +
          expectedErrorCode + " in " + e, e);
    }
  }

}
