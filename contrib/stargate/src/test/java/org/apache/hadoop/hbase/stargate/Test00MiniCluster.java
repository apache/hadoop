/*
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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;

public class Test00MiniCluster extends MiniClusterTestCase {
  public void testDFSMiniCluster() {
    assertNotNull(dfsCluster);
  }

  public void testZooKeeperMiniCluster() {
    assertNotNull(zooKeeperCluster);
  }

  public void testHBaseMiniCluster() throws IOException {
    assertNotNull(hbaseCluster);
    assertNotNull(new HTable(conf, HConstants.META_TABLE_NAME));
  }

  public void testStargateServlet() throws IOException {
    assertNotNull(server);
  }
}
