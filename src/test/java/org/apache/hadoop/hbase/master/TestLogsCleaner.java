/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperWrapper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private ReplicationZookeeperWrapper zkHelper;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    zkHelper = new ReplicationZookeeperWrapper(
        ZooKeeperWrapper.createInstance(conf, HRegionServer.class.getName()),
        conf, new AtomicBoolean(true), "test-cluster");
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testLogCleaning() throws Exception{
    Configuration c = TEST_UTIL.getConfiguration();
    Path oldLogDir = new Path(TEST_UTIL.getTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    String fakeMachineName = URLEncoder.encode("regionserver:60020", "UTF8");

    FileSystem fs = FileSystem.get(c);
    AtomicBoolean stop = new AtomicBoolean(false);
    LogsCleaner cleaner = new LogsCleaner(1000, stop,c, fs, oldLogDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    // Case 1: 2 invalid files, which would be deleted directly
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));
    // Case 2: 1 "recent" file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));
    System.out.println("Now is: " + now);
    for (int i = 0; i < 30; i++) {
      // Case 3: old files which would be deletable for the first log cleaner
      // (TimeToLiveLogCleaner), and also for the second (ReplicationLogCleaner)
      Path fileName = new Path(oldLogDir, fakeMachineName + "." +
          (now - 6000000 - i) );
      fs.createNewFile(fileName);
      // Case 4: put 3 old log files in ZK indicating that they are scheduled
      // for replication so these files would pass the first log cleaner
      // (TimeToLiveLogCleaner) but would be rejected by the second
      // (ReplicationLogCleaner)
      if (i % (30/3) == 0) {
        zkHelper.addLogToList(fileName.getName(), fakeMachineName);
        System.out.println("Replication log file: " + fileName);
      }
    }
    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    // Case 2: 1 newer file, not even deletable for the first log cleaner
    // (TimeToLiveLogCleaner), so we are not going down the chain
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + 10000) ));

    assertEquals(34, fs.listStatus(oldLogDir).length);

    // This will take care of 20 old log files (default max we can delete)
    cleaner.chore();

    assertEquals(14, fs.listStatus(oldLogDir).length);

    // We will delete all remaining log files which are not scheduled for
    // replication and those that are invalid
    cleaner.chore();

    // We end up with the current log file, a newer one and the 3 old log
    // files which are scheduled for replication
    assertEquals(5, fs.listStatus(oldLogDir).length);

    for (FileStatus file : fs.listStatus(oldLogDir)) {
      System.out.println("Keeped log files: " + file.getPath().getName());
    }
  }

}
