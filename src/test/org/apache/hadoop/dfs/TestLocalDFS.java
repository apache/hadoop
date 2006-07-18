package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the DFS class via the FileSystem interface in a single node
 * mini-cluster.
 * @author Owen O'Malley
 */
public class TestLocalDFS extends TestCase {

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    DataOutputStream stm = fileSys.create(name);
    stm.writeBytes("oom");
    stm.close();
  }
  
  private void readFile(FileSystem fileSys, Path name) throws IOException {
    DataInputStream stm = fileSys.open(name);
    byte[] buffer = new byte[4];
    int bytesRead = stm.read(buffer, 0 ,4);
    assertEquals("oom", new String(buffer, 0 , bytesRead));
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests get/set working directory in DFS.
   */
  public void testWorkingDirectory() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf, false);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path orig_path = fileSys.getWorkingDirectory();
      assertTrue(orig_path.isAbsolute());
      Path file1 = new Path("somewhat/random.txt");
      writeFile(fileSys, file1);
      assertTrue(fileSys.exists(new Path(orig_path, file1.toString())));
      fileSys.delete(file1);
      Path subdir1 = new Path("/somewhere");
      fileSys.setWorkingDirectory(subdir1);
      writeFile(fileSys, file1);
      cleanupFile(fileSys, new Path(subdir1, file1.toString()));
      Path subdir2 = new Path("else");
      fileSys.setWorkingDirectory(subdir2);
      writeFile(fileSys, file1);
      readFile(fileSys, file1);
      cleanupFile(fileSys, new Path(new Path(subdir1, subdir2.toString()),
                                     file1.toString()));
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
