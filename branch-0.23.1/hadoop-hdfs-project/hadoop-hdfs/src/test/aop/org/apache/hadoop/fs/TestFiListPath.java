/*
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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A large directory listing may have to go through multiple RPCs.
 * The directory to be listed may be removed before all contents are listed.
 * 
 * This test uses AspectJ to simulate the scenario.
 */
public class TestFiListPath {
  private static final Log LOG = LogFactory.getLog(TestFiListPath.class);
  private static final int LIST_LIMIT = 1;
  
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static Path TEST_PATH = new Path("/tmp");

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, LIST_LIMIT);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitClusterUp();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void prepare() throws IOException {
    fs.mkdirs(TEST_PATH);
    for (int i=0; i<LIST_LIMIT+1; i++) {
      fs.mkdirs(new Path(TEST_PATH, "dir"+i));
    }
  }
  
  @After
  public void cleanup() throws IOException {
    fs.delete(TEST_PATH, true);
  }
  
  /** Remove the target directory after the getListing RPC */
  @Test
  public void testTargetDeletionForListStatus() throws Exception {
    LOG.info("Test Target Delete For listStatus");
    try {
      fs.listStatus(TEST_PATH);
      fail("Test should fail with FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertEquals("File " + TEST_PATH + " does not exist.", e.getMessage());
      LOG.info(StringUtils.stringifyException(e));
    }
  }
  
  /** Remove the target directory after the getListing RPC */
  @Test
  public void testTargetDeletionForListLocatedStatus() throws Exception {
    LOG.info("Test Target Delete For listLocatedStatus");
    RemoteIterator<LocatedFileStatus> itor = fs.listLocatedStatus(TEST_PATH);
    itor.next();
    assertFalse (itor.hasNext());
  }
}
