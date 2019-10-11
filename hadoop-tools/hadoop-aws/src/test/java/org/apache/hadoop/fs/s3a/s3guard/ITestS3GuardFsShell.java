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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.util.ToolRunner;

/**
 * Test FS shell and S3Guard (and of course, the rest of S3).
 */
public class ITestS3GuardFsShell extends AbstractS3ATestBase {

  /**
   * Run a shell command.
   * @param args array of arguments.
   * @return the exit code.
   * @throws Exception any exception raised.
   */
  private int fsShell(String[] args) throws Exception {
    FsShell shell = new FsShell(getConfiguration());
    try {
      return ToolRunner.run(shell, args);
    } finally {
      shell.close();
    }
  }

  /**
   * Execute a command and verify that it returned the specific exit code.
   * @param expected expected value
   * @param args array of arguments.
   * @throws Exception any exception raised.
   */
  private void exec(int expected, String[] args) throws Exception {
    int result = fsShell(args);
    String argslist = Arrays.stream(args).collect(Collectors.joining(" "));
    assertEquals("hadoop fs " + argslist, expected, result);
  }

  /**
   * Execute a shell command expecting a result of 0.
   * @param args array of arguments.
   * @throws Exception any exception raised.
   */
  private void exec(String[] args) throws Exception {
    exec(0, args);
  }

  /**
   * Issue a mkdir without a trailing /.
   */
  @Test
  public void testMkdirNoTrailing() throws Throwable {
    Path dest = path("normal");
    try {
      String destStr = dest.toString();
      mkdirs(destStr);
      isDir(destStr);
      rmdir(destStr);
      isNotFound(destStr);
    } finally {
      getFileSystem().delete(dest, true);
    }
  }

  /**
   * Issue a mkdir with a trailing /.
   */
  @Test
  public void testMkdirTrailing() throws Throwable {
    Path base = path("trailing");
    getFileSystem().delete(base, true);
    try {
      String destStr = base.toString() + "/";
      mkdirs(destStr);
      isDir(destStr);
      isDir(base.toString());
      rmdir(destStr);
      isNotFound(destStr);
    } finally {
      getFileSystem().delete(base, true);
    }
  }

  /**
   * Create the destination path and then call mkdir, expect it to still work.
   */
  @Test
  public void testMkdirTrailingExists() throws Throwable {
    Path base = path("trailingexists");
    getFileSystem().mkdirs(base);
    try {
      String destStr = base.toString() + "/";
      // the path already exists
      isDir(destStr);
      mkdirs(destStr);
      isDir(destStr);
      rmdir(base.toString());
      isNotFound(destStr);
    } finally {
      getFileSystem().delete(base, true);
    }
  }

  private void isNotFound(final String destStr) throws Exception {
    exec(1, new String[]{"-test", "-d", destStr});
  }

  private void mkdirs(final String destStr) throws Exception {
    exec(new String[]{"-mkdir", "-p", destStr});
  }

  private void rmdir(final String destStr) throws Exception {
    exec(new String[]{"-rmdir", destStr});
  }

  private void isDir(final String destStr) throws Exception {
    exec(new String[]{"-test", "-d", destStr});
  }

}
