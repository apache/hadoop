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

import java.io.*;
import junit.framework.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Path;

/**
 * A JUnit test for corrupted file handling.
 */
public class TestFileCorruption extends TestCase {
  
  public TestFileCorruption(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  /** check if DFS can handle corrupted blocks properly */
  public void testFileCorruption() throws Exception {
    MiniDFSCluster cluster = null;
    DFSTestUtil util = new DFSTestUtil("TestFileCorruption", 20, 3, 8*1024);
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      // Now deliberately remove the blocks
      File data_dir = new File(System.getProperty("test.build.data"),
          "dfs/data/data5/current");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (!blocks[idx].getName().startsWith("blk_")) {
          continue;
        }
        System.out.println("Deliberately removing file "+blocks[idx].getName());
        assertTrue("Cannot remove file.", blocks[idx].delete());
      }
      assertTrue("Corrupted replicas not handled properly.",
          util.checkFiles(fs, "/srcdat"));
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** check if local FS can handle corrupted blocks properly */
  public void testLocalFileCorruption() throws Exception {
    Configuration conf = new Configuration();
    Path file = new Path(System.getProperty("test.build.data"), "corruptFile");
    FileSystem fs = FileSystem.getLocal(conf);
    DataOutputStream dos = fs.create(file);
    dos.writeBytes("original bytes");
    dos.close();
    // Now deliberately corrupt the file
    dos = new DataOutputStream(new FileOutputStream(file.toString()));
    dos.writeBytes("corruption");
    dos.close();
    // Now attempt to read the file
    DataInputStream dis = fs.open(file,512);
    try {
      System.out.println("A ChecksumException is expected to be logged.");
      dis.readByte();
    } catch (ChecksumException ignore) {
      //expect this exception but let any NPE get thrown
    }
    fs.delete(file);
  }
}
