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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestFileCreation extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 2 * blockSize;

  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');
  
  //
  // creates a file but does not close it
  //
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
  }

  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      String[][] locations = fileSys.getFileCacheHints(name, 0, fileSize);
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].length < repl) {
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    // do a sanity check. Read the file
    byte[] actual = new byte[fileSize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 1");
  }

  private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      this.assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }

  /**
   * Test that file data becomes available before file is closed.
   */
  public void testFileCreation() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.isDirectory(path));
      System.out.println(fs.getFileStatus(path).isDir()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      // create a new a file in home directory. Do not close it.
      //
      Path file1 = new Path("filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file filestatus.dat with one "
                         + " replicas.");

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");

      // write to file
      writeFile(stm);

      // verify that file size has changed
      assertTrue(file1 + " should be of size " + fileSize, 
                  fs.getFileStatus(file1).getLen() == fileSize);

      // Make sure a client can read it before it is closed.
      checkFile(fs, file1, 1);
      stm.close();

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
