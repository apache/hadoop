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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement
    .DatanodeAdminBackoffMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement
    .DatanodeAdminMonitorInterface;
import org.junit.Test;

import java.io.IOException;

/**
 * This class tests decommission using the alternative backoff monitor. It
 * works by sub-classing the original decommission tests and then setting the
 * config to enable the alternative monitor version.
 */

public class TestDecommissionWithBackoffMonitor extends TestDecommission {

  @Override
  public void setup() throws IOException {
    super.setup();
    Configuration conf = getConf();
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS,
        DatanodeAdminBackoffMonitor.class, DatanodeAdminMonitorInterface.class);
  }

  @Override
  @Test
  public void testBlocksPerInterval() {
    // This test is not valid in the decommission monitor V2 so
    // effectively commenting it out by overriding and having it do nothing.
  }
}