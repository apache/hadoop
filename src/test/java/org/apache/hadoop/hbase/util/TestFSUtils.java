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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

/**
 * Test {@link FSUtils}.
 */
public class TestFSUtils {
  @Test public void testIsHDFS() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    htu.getConfiguration().setBoolean("dfs.support.append", false);
    assertFalse(FSUtils.isHDFS(htu.getConfiguration()));
    htu.getConfiguration().setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = null;
    try {
      cluster = htu.startMiniDFSCluster(1);
      assertTrue(FSUtils.isHDFS(htu.getConfiguration()));
      assertTrue(FSUtils.isAppendSupported(htu.getConfiguration()));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }
}