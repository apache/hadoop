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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import junit.framework.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * A JUnit test for doing fsck
 */
public class TestFsck extends TestCase {
 
  public TestFsck(String testName) {
    super(testName);
  }

  
  
  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  /** do fsck */
  public void testFsck() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 4, true, null);
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      PrintStream oldOut = System.out;
      ByteArrayOutputStream bStream = new ByteArrayOutputStream();
      PrintStream newOut = new PrintStream(bStream, true);
      System.setOut(newOut);
      assertEquals(0, new DFSck().doMain(conf, new String[] {"/"}));
      System.setOut(oldOut);
      String outStr = bStream.toString();
      assertTrue(-1 != outStr.indexOf("HEALTHY"));
      System.out.println(outStr);
      cluster.shutdown();
      
      // restart the cluster; bring up namenode but not the data nodes
      cluster = new MiniDFSCluster(conf, 0, false, null);
      oldOut = System.out;
      bStream = new ByteArrayOutputStream();
      newOut = new PrintStream(bStream, true);
      System.setOut(newOut);
      assertEquals(0, new DFSck().doMain(conf, new String[] {"/"}));
      System.setOut(oldOut);
      outStr = bStream.toString();
      // expect the result is corrupt
      assertTrue(outStr.contains("CORRUPT"));
      System.out.println(outStr);
      
      // bring up data nodes & cleanup cluster
      cluster.startDataNodes(conf, 4, true, null, null);
      util.cleanup(cluster.getFileSystem(), "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** do fsck on non-existent path*/
  public void testFsckNonExistent() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 4, true, null);
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      PrintStream oldOut = System.out;
      ByteArrayOutputStream bStream = new ByteArrayOutputStream();
      PrintStream newOut = new PrintStream(bStream, true);
      System.setOut(newOut);
      assertEquals(0, new DFSck().doMain(conf, new String[] {"/non-existent"}));
      System.setOut(oldOut);
      String outStr = bStream.toString();
      assertEquals(-1, outStr.indexOf("HEALTHY"));
      System.out.println(outStr);
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
