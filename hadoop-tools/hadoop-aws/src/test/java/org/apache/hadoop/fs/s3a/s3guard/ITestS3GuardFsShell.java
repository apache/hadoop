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
 * Test FS shell and S3Guard (and of course, the rest of S3)
 */
public class ITestS3GuardFsShell extends AbstractS3ATestBase {

  private int fsShell(String[] args) throws Exception {
    FsShell shell = new FsShell(getConfiguration());
    try {
      return ToolRunner.run(shell, args);
    } finally {
      shell.close();
    }
  }

  private void assertShell(int expected, String[] args) throws Exception {
    int result = fsShell(args);
    String argslist = Arrays.stream(args).collect(Collectors.joining(" "));
    assertEquals("hadoop fs " + argslist, expected, result);
  }

  private void exec(String[] args) throws Exception {
    assertShell(0, args);
  }

  @Test
  public void testMkdirNormal() throws Throwable {
    Path dest = path("normal");
    try {
      String destStr = dest.toString();
      exec(new String[]{"-mkdir", "-p", destStr});
      exec(new String[]{"-test", "-d", destStr});
      exec(new String[]{"-rmdir", destStr});
      assertShell(1, new String[]{"-test", "-d", destStr});
    } finally {
      getFileSystem().delete(dest, true);
    }
  }
  @Test

  public void testMkdirTrailing() throws Throwable {
    Path dest = path("trailing");
    try {
      String destStr = dest.toString() + "/";
      exec(new String[]{"-mkdir", "-p", destStr});
      exec(new String[]{"-test", "-d", destStr});
      exec(new String[]{"-rmdir", destStr});
      assertShell(1, new String[]{"-test", "-d", destStr});
    } finally {
      getFileSystem().delete(dest, true);
    }
  }
}
