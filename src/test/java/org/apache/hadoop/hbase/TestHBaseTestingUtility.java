/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test our testing utility class
 */
public class TestHBaseTestingUtility {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private HBaseTestingUtility hbt;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    this.hbt = new HBaseTestingUtility();
    this.hbt.cleanupTestDir();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test public void testMiniCluster() throws Exception {
    MiniHBaseCluster cluster = this.hbt.startMiniCluster();
    try {
      assertEquals(1, cluster.getLiveRegionServerThreads().size());
    } finally {
      cluster.shutdown();
    }
  }

  @Test public void testMiniDFSCluster() throws Exception {
    MiniDFSCluster cluster = this.hbt.startMiniDFSCluster(1);
    FileSystem dfs = cluster.getFileSystem();
    Path dir = new Path("dir");
    Path qualifiedDir = dfs.makeQualified(dir);
    LOG.info("dir=" + dir + ", qualifiedDir=" + qualifiedDir);
    assertFalse(dfs.exists(qualifiedDir));
    assertTrue(dfs.mkdirs(qualifiedDir));
    assertTrue(dfs.delete(qualifiedDir, true));
    try {
    } finally {
      cluster.shutdown();
    }
  }

  @Test public void testSetupClusterTestBuildDir() {
    File testdir = this.hbt.setupClusterTestBuildDir();
    LOG.info("uuid-subdir=" + testdir);
    assertFalse(testdir.exists());
    assertTrue(testdir.mkdirs());
    assertTrue(testdir.exists());
  }

  @Test public void testTestDir() throws IOException {
    Path testdir = HBaseTestingUtility.getTestDir();
    LOG.info("testdir=" + testdir);
    FileSystem fs = this.hbt.getTestFileSystem();
    assertTrue(!fs.exists(testdir));
    assertTrue(fs.mkdirs(testdir));
    assertTrue(this.hbt.cleanupTestDir());
  }
}