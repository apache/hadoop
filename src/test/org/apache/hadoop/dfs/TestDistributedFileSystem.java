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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDistributedFileSystem extends junit.framework.TestCase {
  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = new Configuration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   */
  public void testDFSClose() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();

    try {
      // create two files
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

}
