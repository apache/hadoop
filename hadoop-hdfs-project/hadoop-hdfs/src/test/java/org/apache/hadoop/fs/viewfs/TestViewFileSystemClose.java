package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestViewFileSystemClose {

  /**
   * Verify that all child file systems of a ViewFileSystem will be shut down
   * when the cache is disabled.
   * @throws IOException
   */
  @Test
  public void testFileSystemLeak() throws IOException {

    Configuration conf = new Configuration();
    conf.set("fs.viewfs.impl", ViewFileSystem.class.getName());
    conf.setBoolean("fs.viewfs.enable.inner.cache", false);
    conf.setBoolean("fs.viewfs.impl.disable.cache", true);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    String rootPath = "hdfs://localhost/tmp";
    ConfigUtil.addLink(conf, "/data", new Path(rootPath, "data").toUri());
    ViewFileSystem viewFs =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, conf);

    FileSystem[] children = viewFs.getChildFileSystems();
    viewFs.close();
    FileSystem.closeAll();
    for (FileSystem fs : children) {
      try {
        fs.create(new Path(rootPath, "neverSuccess"));
        fail();
      } catch (IOException ioe) {
        assertTrue(ioe.getMessage().contains("Filesystem closed"));
      }
    }
  }
}
