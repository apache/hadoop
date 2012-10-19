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
package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.junit.Test;


public class TestMiniJournalCluster {
  @Test
  public void testStartStop() throws IOException {
    Configuration conf = new Configuration();
    MiniJournalCluster c = new MiniJournalCluster.Builder(conf)
      .build();
    try {
      URI uri = c.getQuorumJournalURI("myjournal");
      String[] addrs = uri.getAuthority().split(";");
      assertEquals(3, addrs.length);
      
      JournalNode node = c.getJournalNode(0);
      String dir = node.getConf().get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY);
      assertEquals(
          new File(MiniDFSCluster.getBaseDirectory() + "journalnode-0")
            .getAbsolutePath(),
          dir);
    } finally {
      c.shutdown();
    }
  }
}
