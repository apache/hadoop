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

import java.io.IOException;
import java.util.Random;
import junit.framework.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A JUnit test for checking if restarting DFS preserves integrity.
 *
 * @author Milind Bhandarkar
 */
public class TestRestartDFS extends TestCase {
  
  private static final int NFILES = 20;
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8*1024;
    private static String[] dirNames = {
      "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    };
    private String name = "";
    private int size;
    private long seed;
    
    MyFile() {
      int nLevels = gen.nextInt(MAX_LEVELS);
      if(nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        name = sb.toString();
      }
      long fidx = -1;
      while (fidx < 0) { fidx = gen.nextLong(); }
      name = name + Long.toString(fidx);
      size = gen.nextInt(MAX_SIZE);
      seed = gen.nextLong();
    }
    
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }
  
  public TestRestartDFS(String testName) {
    super(testName);
  }

  
  
  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  /** create NFILES with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  private static MyFile[] createFiles(String fsname, String topdir)
  throws IOException {
    MyFile[] files = new MyFile[NFILES];
    
    for (int idx = 0; idx < NFILES; idx++) {
      files[idx] = new MyFile();
    }
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < NFILES; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      if (!fs.mkdirs(fPath.getParent())) {
        throw new IOException("Mkdirs failed to create " + 
                              fPath.getParent().toString());
      }
      FSDataOutputStream out = fs.create(fPath);
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
      toWrite = null;
    }
    
    return files;
  }
  
  /** check if the files have been copied correctly. */
  private static boolean checkFiles(String fsname, String topdir, MyFile[] files) 
  throws IOException {
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < NFILES; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      FSDataInputStream in = fs.open(fPath);
      byte[] toRead = new byte[files[idx].getSize()];
      byte[] toCompare = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toCompare);
      assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
      in.close();
      for (int i = 0; i < toRead.length; i++) {
        if (toRead[i] != toCompare[i]) {
          return false;
        }
      }
      toRead = null;
      toCompare = null;
    }
    
    return true;
  }
  
  /** delete directory and everything underneath it.*/
  private static void deldir(String fsname, String topdir)
  throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getNamed(fsname, conf);
    Path root = new Path(topdir);
    fs.delete(root);
  }
  
  /** check if DFS remains in proper condition after a restart */
  public void testRestartDFS() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    MyFile[] files = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(65314, conf, 4, false);
      namenode = conf.get("fs.default.name", "local");
      if (!"local".equals(namenode)) {
        files = createFiles(namenode, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
    assertTrue("Error creating files", files != null);
    try {
      Configuration conf = new Configuration();
      // Here we restart the MiniDFScluster without formatting namenode
      cluster = new MiniDFSCluster(65320, conf, 4, false, false);
      namenode = conf.get("fs.default.name", "local");
      if (!"local".equals(namenode)) {
        assertTrue("Filesystem corrupted after restart.",
            checkFiles(namenode, "/srcdat", files));
        deldir(namenode, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
