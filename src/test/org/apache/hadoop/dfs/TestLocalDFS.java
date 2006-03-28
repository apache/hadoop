package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * This class tests the DFS class via the FileSystem interface in a single node
 * mini-cluster.
 * @author Owen O'Malley
 */
public class TestLocalDFS extends TestCase {

  private void writeFile(FileSystem fileSys, File name) throws IOException {
    DataOutputStream stm = fileSys.create(name);
    stm.writeBytes("oom");
    stm.close();
  }
  
  private void readFile(FileSystem fileSys, File name) throws IOException {
    DataInputStream stm = fileSys.open(name);
    byte[] buffer = new byte[4];
    int bytesRead = stm.read(buffer, 0 ,4);
    assertEquals("oom", new String(buffer, 0 , bytesRead));
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, File name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests get/set working directory in DFS.
   */
  public void testWorkingDirectory() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      File orig_path = fileSys.getWorkingDirectory();
      assertTrue(orig_path.isAbsolute());
      File file1 = new File("somewhat/random.txt");
      writeFile(fileSys, file1);
      assertTrue(fileSys.exists(new File(orig_path, file1.getPath())));
      fileSys.delete(file1);
      File subdir1 = new File("/somewhere").getAbsoluteFile();
      fileSys.setWorkingDirectory(subdir1);
      writeFile(fileSys, file1);
      cleanupFile(fileSys, new File(subdir1, file1.getPath()));
      File subdir2 = new File("else");
      fileSys.setWorkingDirectory(subdir2);
      writeFile(fileSys, file1);
      readFile(fileSys, file1);
      cleanupFile(fileSys, new File(new File(subdir1, subdir2.getPath()),
                                     file1.getPath()));
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
