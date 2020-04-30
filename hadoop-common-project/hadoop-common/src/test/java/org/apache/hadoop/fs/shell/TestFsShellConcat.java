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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Concat.
 */
public class TestFsShellConcat {

  static Configuration conf;
  static FsShell shell;
  static LocalFileSystem lfs;
  static Path testRootDir, dstPath;

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
    shellRun(0, "-concat", dstPath.toString(), testRootDir+"/*");

    assertTrue(lfs.exists(dstPath));
    assertEquals(1, lfs.listStatus(testRootDir).length);
  }

  private void shellRun(int n, String ... args) throws Exception {
    assertEquals(n, shell.run(args));
  }

  /**
   * Simple simulation of concat.
   */
  private void mockConcat(Path target, Path[] srcArray) throws IOException {
    OutputStream out = lfs.create(target);
    try {
      for (int i = 0; i < srcArray.length; i++) {
        InputStream in = lfs.open(srcArray[i]);
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
