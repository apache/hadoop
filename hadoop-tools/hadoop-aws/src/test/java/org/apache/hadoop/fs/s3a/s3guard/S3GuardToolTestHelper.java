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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;

/**
 * Helper class for tests which make CLI invocations of the S3Guard tools.
 * That's {@link AbstractS3GuardToolTestBase} and others.
 */
public final class S3GuardToolTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3GuardToolTestHelper.class);

  private S3GuardToolTestHelper() {
  }

  /**
   * Execute a command, returning the buffer if the command actually completes.
   * If an exception is raised the output is logged before the exception is
   * rethrown.
   * @param cmd command
   * @param args argument list
   * @throws Exception on any failure
   */
  public static String exec(S3GuardTool cmd, Object... args) throws Exception {
    return expectExecResult(0, cmd, args);
  }

  /**
   * Execute a command, returning the buffer if the command actually completes.
   * If an exception is raised which doesn't provide the exit code
   * the output is logged before the exception is rethrown.
   * @param expectedResult the expected result
   * @param cmd command
   * @param args argument list
   * @throws Exception on any failure
   */
  public static String expectExecResult(
      final int expectedResult,
      final S3GuardTool cmd,
      final Object... args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try {
      exec(expectedResult, "", cmd, buf, args);
      return buf.toString();
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Command {} failed: \n{}", cmd, buf);
      throw e;
    }
  }

  /**
   * Given an array of objects, conver to an array of strings.
   * @param oargs object args
   * @return string equivalent
   */
  public static String[] varargsToString(final Object[] oargs) {
    return Arrays.stream(oargs)
        .map(Object::toString)
        .toArray(String[]::new);
  }

  /**
   * Execute a command, saving the output into the buffer.
   * @param expectedResult expected result of the command.
   * @param errorText error text to include in the assertion.
   * @param cmd command
   * @param buf buffer to use for tool output (not SLF4J output)
   * @param args argument list
   * @throws Exception on any failure other than exception which
   * implements ExitCodeProvider and whose exit code matches that expected
   */
  public static void exec(final int expectedResult,
      final String errorText,
      final S3GuardTool cmd,
      final ByteArrayOutputStream buf,
      final Object... oargs)
      throws Exception {
    final String[] args = varargsToString(oargs);
    LOG.info("exec {}", (Object) args);
    int r;
    try (PrintStream out = new PrintStream(buf)) {
      r = cmd.run(args, out);
      out.flush();
    } catch (Exception ex) {
      if (ex instanceof ExitCodeProvider) {
        // it returns an exit code
        final ExitCodeProvider ec = (ExitCodeProvider) ex;
        if (ec.getExitCode() == expectedResult) {
          // and the exit code matches what is expected -all is good.
          return;
        }
      }
      throw ex;
    }
    if (expectedResult != r) {
      String message = errorText.isEmpty() ? "" : (errorText + ": ")
          + "Command " + cmd + " failed\n" + buf;
      assertEquals(message, expectedResult, r);
    }
  }

  /**
   * Run a S3GuardTool command from a varags list.
   * <p></p>
   * Warning: if the filesystem is retrieved from the cache,
   * it will be closed afterwards.
   * @param conf configuration
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  public static int runS3GuardCommand(Configuration conf, Object... args)
      throws Exception {
    return S3GuardTool.run(conf, varargsToString(args));
  }

  /**
   * Run a S3GuardTool command from a varags list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param conf configuration
   * @param status expected status code of the exception
   * @param args argument list
   * @throws Exception any exception
   */
  public static void runS3GuardCommandToFailure(Configuration conf,
      int status,
      Object... args) throws Exception {

    ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class,
            () -> {
              int ec = runS3GuardCommand(conf, args);
              if (ec != 0) {
                throw new ExitUtil.ExitException(ec, "exit code " + ec);
              }
            });
    if (ex.status != status) {
      throw ex;
    }
  }
}
