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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY;


/**
 * Test in progress tailing with small txn id per call.
 *
 * The number of edits that needs to be tailed during
 * bootstrapStandby can be large, but the number of edits
 * that can be tailed using RPC call can be limited
 * (configured by dfs.ha.tail-edits.qjm.rpc.max-txns).
 * This is to test that even with small number of configured
 * txnid, bootstrapStandby can still work. See HDFS-14806.
 */
public class TestBootstrapStandbyWithInProgressTailing
    extends TestBootstrapStandbyWithQJM {
  @Override
  public Configuration createConfig() {
    Configuration conf = super.createConfig();

    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setInt("dfs.ha.tail-edits.qjm.rpc.max-txns", 1);

    return conf;
  }
}
