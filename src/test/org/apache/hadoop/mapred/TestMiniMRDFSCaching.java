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

package org.apache.hadoop.mapred;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

/**
 * A JUnit test to test caching with DFS
 * 
 * @author Mahadev Konar
 */
public class TestMiniMRDFSCaching extends TestCase {

  public void testWithDFS() throws IOException {
    MiniMRCluster mr = null;
    MiniDFSCluster dfs = null;
    String namenode = null;
    FileSystem fileSys = null;
    try {
      JobConf conf = new JobConf();
      dfs = new MiniDFSCluster(65314, conf, true);
      fileSys = dfs.getFileSystem();
      namenode = fileSys.getName();
      mr = new MiniMRCluster(60050, 50060, 2, namenode, true, 4);
      // run the wordcount example with caching
      boolean ret = MRCaching.launchMRCache("localhost:"+mr.getJobTrackerPort(),
                                            "/testing/wc/input",
                                            "/testing/wc/output", namenode,
                                            conf,
                                            "The quick brown fox\nhas many silly\n"
                                                + "red fox sox\n");
      assertTrue("Archives not matching", ret);
    } finally {
      if (fileSys != null) {
        fileSys.close();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  public static void main(String[] argv) throws Exception {
    TestMiniMRDFSCaching td = new TestMiniMRDFSCaching();
    td.testWithDFS();
  }
}
