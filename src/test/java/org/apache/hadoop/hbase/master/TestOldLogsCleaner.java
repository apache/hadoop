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
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestOldLogsCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();


  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
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
    OldLogsCleaner cleaner = new OldLogsCleaner(1000, stop,c, fs, oldLogDir);

    // Create 2 invalid files, 1 "recent" file, 1 very new file and 30 old files
    long now = System.currentTimeMillis();
    fs.delete(oldLogDir, true);
    fs.mkdirs(oldLogDir);
    fs.createNewFile(new Path(oldLogDir, "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + "a"));
    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + now));
    System.out.println("Now is: " + now);
    for (int i = 0; i < 30; i++) {
      fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now - 6000000 - i) ));
    }
    for (FileStatus stat : fs.listStatus(oldLogDir)) {
      System.out.println(stat.getPath().toString());
    }

    fs.createNewFile(new Path(oldLogDir, fakeMachineName + "." + (now + 10000) ));

    assertEquals(34, fs.listStatus(oldLogDir).length);

    // This will take care of 20 old log files (default max we can delete)
    cleaner.chore();

    assertEquals(14, fs.listStatus(oldLogDir).length);

    // We will delete all remaining log files and those that are invalid
    cleaner.chore();

    // We end up with the current log file and a newer one
    assertEquals(2, fs.listStatus(oldLogDir).length);
  }

}
