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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.raid.protocol.PolicyInfo;

public class TestRaidFilter extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static Log LOG =
    LogFactory.getLog("org.apache.hadoop.raid.TestRaidFilter");

  Configuration conf;
  MiniDFSCluster dfs = null;
  FileSystem fs = null;

  private void mySetup() throws Exception {
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, 2, true, null);
    dfs.waitActive();
    fs = dfs.getFileSystem();
    String namenode = fs.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
  }

  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }

  public void testLayeredPolicies() throws Exception {
    mySetup();
    Path src1 = new Path("/user/foo");
    Path src2 = new Path("/user/foo/bar");

    PolicyInfo info1 = new PolicyInfo("p1", conf);
    info1.setSrcPath(src1.toString());
    info1.setErasureCode("xor");
    info1.setDescription("test policy");
    info1.setProperty("targetReplication", "1");
    info1.setProperty("metaReplication", "1");
    info1.setProperty("modTimePeriod", "0");

    PolicyInfo info2 = new PolicyInfo("p2", conf);
    info2.setSrcPath(src2.toString());
    info2.setErasureCode("xor");
    info2.setDescription("test policy");
    info2.setProperty("targetReplication", "1");
    info2.setProperty("metaReplication", "1");
    info2.setProperty("modTimePeriod", "0");

    ArrayList<PolicyInfo> all = new ArrayList<PolicyInfo>();
    all.add(info1);
    all.add(info2);

    try {
      long blockSize = 1024;
      byte[] bytes = new byte[(int)blockSize];
      Path f1 = new Path(src1, "f1");
      Path f2 = new Path(src2, "f2");
      FSDataOutputStream stm1 = fs.create(f1, false, 4096, (short)1, blockSize);
      FSDataOutputStream stm2 = fs.create(f2, false, 4096, (short)1, blockSize);
      FSDataOutputStream[]  stms = new FSDataOutputStream[]{stm1, stm2};
      for (FSDataOutputStream stm: stms) {
        stm.write(bytes);
        stm.write(bytes);
        stm.write(bytes);
        stm.close();
      }

      Thread.sleep(1000);

      FileStatus stat1 = fs.getFileStatus(f1);
      FileStatus stat2 = fs.getFileStatus(f2);

      RaidFilter.Statistics stats = new RaidFilter.Statistics();
      RaidFilter.TimeBasedFilter filter = new RaidFilter.TimeBasedFilter(
        conf, RaidNode.xorDestinationPath(conf), info1, all,
        System.currentTimeMillis(), stats);
      System.out.println("Stats " + stats);

      assertTrue(filter.check(stat1));
      assertFalse(filter.check(stat2));

    } finally {
      myTearDown();
    }
  }
}
