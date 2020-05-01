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
package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Concat.
 */
public class TestFsShellConcat {

  private static Configuration conf;
  private static FsShell shell;
  private static LocalFileSystem lfs;
  private static Path testRootDir;
  private static Path dstPath;

  @Before
  public void before() throws IOException {
    conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(new Path(GenericTestUtils.getTempPath(
        "testFsShellCopy")));

    if (lfs.exists(testRootDir)) {
      lfs.delete(testRootDir, true);
    }
    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
    dstPath = new Path(testRootDir, "dstFile");
    lfs.create(dstPath).close();

    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      OutputStream out = lfs.create(new Path(testRootDir, "file-" + i));
      out.write(random.nextInt());
      out.close();
    }
  }

  @Test
  public void testConcat() throws Exception {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      Path target = (Path)args[0];
      Path[] src = (Path[]) args[1];
      mockConcat(target, src);
      return null;
    }).when(mockFs).concat(any(Path.class), any(Path[].class));
    Concat.setTstFs(mockFs);
    shellRun(0, "-concat", dstPath.toString(), testRootDir+"/file-*");

    assertTrue(lfs.exists(dstPath));
    assertEquals(1, lfs.listStatus(testRootDir).length);
  }

  @Test
  public void testUnsupportedFs() throws Exception {
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Mockito.doThrow(
        new UnsupportedOperationException("Mock unsupported exception."))
        .when(mockFs).concat(any(Path.class), any(Path[].class));
    Mockito.doAnswer(invocationOnMock -> new URI("mockfs:///")).when(mockFs)
        .getUri();
    Concat.setTstFs(mockFs);
    PrintStream oldErr = System.err;
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));
    shellRun(1, "-concat", dstPath.toString(), testRootDir + "/file-*");
    System.setErr(oldErr);
    System.err.print(err.toString());
    assertTrue(err.toString()
        .contains("Dest filesystem 'mockfs' doesn't support concat"));
  }

  private void shellRun(int n, String... args) {
    assertEquals(n, shell.run(args));
  }

  /**
   * Simple simulation of concat.
   */
  private void mockConcat(Path target, Path[] srcArray) throws IOException {
    Path tmp = new Path(target.getParent(), target.getName() + ".bak");
    lfs.rename(target, tmp);
    OutputStream out = lfs.create(target);
    try {
      InputStream in = lfs.open(tmp);
      try {
        IOUtils.copyBytes(in, out, 1024);
      } finally {
        if (in != null) {
          in.close();
        }
        lfs.delete(tmp, true);
      }
      for (int i = 0; i < srcArray.length; i++) {
        in = lfs.open(srcArray[i]);
        try {
          IOUtils.copyBytes(in, out, 1024);
        } finally {
          if (in != null) {
            in.close();
          }
          lfs.delete(srcArray[i], true);
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}
