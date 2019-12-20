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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * If an exception is raised the output is logged instead.
   * @param cmd command
   * @param args argument list
   * @throws Exception on any failure
   */
  public static String exec(S3GuardTool cmd, String... args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try {
      exec(0, "", cmd, buf, args);
      return buf.toString();
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Command {} failed: \n{}", cmd, buf);
      throw e;
    }
  }

  /**
   * Execute a command, saving the output into the buffer.
   * @param expectedResult expected result of the command.
   * @param errorText error text to include in the assertion.
   * @param cmd command
   * @param buf buffer to use for tool output (not SLF4J output)
   * @param args argument list
   * @throws Exception on any failure
   */
  public static void exec(final int expectedResult,
      final String errorText,
      final S3GuardTool cmd,
      final ByteArrayOutputStream buf,
      final String... args)
      throws Exception {
    LOG.info("exec {}", (Object) args);
    int r;
    try (PrintStream out = new PrintStream(buf)) {
      r = cmd.run(args, out);
      out.flush();
    }
    if (expectedResult != r) {
      String message = errorText.isEmpty() ? "" : (errorText + ": ")
          + "Command " + cmd + " failed\n" + buf;
      assertEquals(message, expectedResult, r);
    }
  }

}
