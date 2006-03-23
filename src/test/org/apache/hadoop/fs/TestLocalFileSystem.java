package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import java.io.*;
import junit.framework.*;

/**
 * This class tests the local file system via the FileSystem abstraction.
 * @author Owen O'Malley
 */
public class TestLocalFileSystem extends TestCase {

  private void writeFile(FileSystem fs, File name) throws IOException {
    FSDataOutputStream stm = fs.create(name);
    stm.writeBytes("42\n");
    stm.close();
  }
  
  private void cleanupFile(FileSystem fs, File name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name);
    assertTrue(!fs.exists(name));
  }
  
  /**
   * Test the capability of setting the working directory.
   */
  public void testWorkingDirectory() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getNamed("local", conf);
    File origDir = fileSys.getWorkingDirectory();
    File subdir = new File("build/test/data/work-dir/new subdir");
    File subdirAbsolute = subdir.getAbsoluteFile();
    try {
      // make sure it doesn't already exist
      assertTrue(!fileSys.exists(subdir));
      // make it and check for it
      fileSys.mkdirs(subdir);
      assertTrue(fileSys.isDirectory(subdir));
      
      fileSys.setWorkingDirectory(subdir);
      
      // create a directory and check for it
      File dir1 = new File("dir1");
      File dir1Absolute = dir1.getAbsoluteFile();
      fileSys.mkdirs(dir1);
      assertTrue(fileSys.isDirectory(dir1));
      assertTrue(fileSys.isDirectory(dir1Absolute));
      
      // delete the directory and make sure it went away
      fileSys.delete(dir1);
      assertTrue(!fileSys.exists(dir1));
      assertTrue(!fileSys.exists(dir1Absolute));
      
      // create files and manipulate them.
      File file1 = new File("file1");
      File file2 = new File("sub/file2");
      File file2_abs = file2.getAbsoluteFile();
      assertEquals(file2_abs, new File(subdirAbsolute, file2.getPath()));
      writeFile(fileSys, file1);
      fileSys.copyFromLocalFile(file1, file2);
      assertTrue(fileSys.exists(file1));
      assertTrue(fileSys.isFile(file1));
      cleanupFile(fileSys, file2_abs);
      fileSys.copyToLocalFile(file1, file2);
      cleanupFile(fileSys, file2_abs);
      
      // try a rename
      fileSys.rename(file1, file2);
      assertTrue(!fileSys.exists(file1));
      assertTrue(fileSys.exists(file2_abs));
      fileSys.rename(file2, file1);
      
      // try reading a file
      InputStream stm = fileSys.openRaw(file1);
      byte[] buffer = new byte[3];
      int bytesRead = stm.read(buffer, 0, 3);
      assertEquals("42\n", new String(buffer, 0, bytesRead));
      stm.close();
    } finally {
      fileSys.setWorkingDirectory(origDir);
      fileSys.delete(subdir);
    }
  }
}
