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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestListFiles;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * This class tests the FileStatus API.
 */
public class TestListFilesInDFS extends TestListFiles {
  {
    GenericTestUtils.setLogLevel(FileSystem.LOG, Level.ALL);
  }


  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void testSetUp() throws Exception {
    setTestPaths(new Path("/tmp/TestListFilesInDFS"));
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
    fs.delete(TEST_DIR, true);
  }
  
  @AfterClass
  public static void testShutdown() throws Exception {
    if (cluster != null) {
      fs.close();
      cluster.shutdown();
    }
  }
  
  protected static Path getTestDir() {
    return new Path("/main_");
  }
}
