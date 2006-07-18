/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 *
 * @author Milind Bhandarkar
 */
public class TestMiniMRWithDFS extends TestCase {
  
    static final int NUM_MAPS = 10;
    static final int NUM_SAMPLES = 100000;
    
  public void testWithDFS() throws IOException {
      String namenode = null;
      MiniDFSCluster dfs = null;
      MiniMRCluster mr = null;
      FileSystem fileSys = null;
      try {
          Configuration conf = new Configuration();
          dfs = new MiniDFSCluster(65314, conf, true);
          fileSys = dfs.getFileSystem();
          namenode = fileSys.getName();
          mr = new MiniMRCluster(50050, 50060, 4, namenode, true);
          double estimate = PiEstimator.launch(NUM_MAPS, NUM_SAMPLES, "localhost:50050", namenode);
          double error = Math.abs(Math.PI - estimate);
          assertTrue("Error in PI estimation "+error+" exceeds 0.01", (error < 0.01));
      } finally {
          if (fileSys != null) { fileSys.close(); }
          if (dfs != null) { dfs.shutdown(); }
          if (mr != null) { mr.shutdown();
          }
      }
  }
  
}
