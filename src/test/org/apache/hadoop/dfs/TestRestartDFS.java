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
 */
public class TestRestartDFS extends TestCase {
  
  private static Configuration conf = new Configuration();

  public TestRestartDFS(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  /** check if DFS remains in proper condition after a restart */
  public void testRestartDFS() throws Exception {
    MiniDFSCluster cluster = null;
    DFSTestUtil files = new DFSTestUtil("TestRestartDFS", 20, 3, 8*1024);
    try {
      cluster = new MiniDFSCluster(conf, 4, true, null);
      FileSystem fs = cluster.getFileSystem();
      files.createFiles(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
    try {
      // Here we restart the MiniDFScluster without formatting namenode
      cluster = new MiniDFSCluster(conf, 4, false, null);
      FileSystem fs = cluster.getFileSystem();
      assertTrue("Filesystem corrupted after restart.",
                 files.checkFiles(fs, "/srcdat"));
      files.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
