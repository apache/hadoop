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

import static org.junit.Assert.*;
import org.junit.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Tests the scenario where file system of the jobtracker system
 * dir is not on the default file system.
 */
public class TestJobTrackerWithNonDefaultFS {
  private Configuration conf;
  private MiniDFSCluster dfs;
  private FileSystem defaultFs;
  private final String TEST_DIR =
      System.getProperty("test.build.data", "/tmp");

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, 1, true, null);
    defaultFs = dfs.getFileSystem();
    FileSystem.setDefaultUri(conf, defaultFs.getUri());
  }

  @After
  public void tearDown() throws IOException {
    if (dfs != null) {
      dfs.shutdown();
    }
  }

  /**
  * Validates that JobTracker can start properly when its system dir
  * resides on a non-default FileSystem.
  */
  @Test
  public void testSystemDir() throws IOException {
    Path systemDirPath = new Path(TEST_DIR + "/mapred/system");
    Path systemDir = new Path("file:///" + systemDirPath.toString());
    JobConf jobConf = new JobConf(conf);
    jobConf.set("mapred.system.dir", systemDir.toUri().toString());

    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(0, 0, 0,
          defaultFs.getUri().toString(),
          1, null, null, null, jobConf);

      Path jtSystemDir = new Path(mr.getJobTrackerRunner().getJobTracker()
          .getSystemDir());

      assertEquals("Check if JobTracker's system dir property is picked up",
          systemDir, jtSystemDir);

      assertTrue("Check if the system dir exists on the right file system",
          FileSystem.get(systemDir.toUri(), conf).exists(systemDirPath));
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}