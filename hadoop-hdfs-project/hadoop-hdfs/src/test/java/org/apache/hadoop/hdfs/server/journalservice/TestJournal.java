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
package org.apache.hadoop.hdfs.server.journalservice;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.junit.Assert;
import org.junit.Test;

public class TestJournal {
  static final Log LOG = LogFactory.getLog(TestJournal.class);
  
  static Configuration newConf(String name) {
    Configuration conf = new HdfsConfiguration();
    File dir = new File(MiniDFSCluster.getBaseDirectory(), name + "-edits");
    Assert.assertTrue(dir.mkdirs());
    conf.set(DFSConfigKeys.DFS_JOURNAL_EDITS_DIR_KEY, dir.toURI().toString());
    return conf;
  }

  @Test
  public void testFormat() throws Exception {
    final Configuration conf = newConf("testFormat");
    final Journal j = new Journal(conf);
    LOG.info("Initial  : " + j.getStorage());
    Assert.assertFalse(j.isFormatted());

    //format
    final int namespaceId = 123;
    final String clusterId = "my-cluster-id";
    j.format(namespaceId, clusterId);
    Assert.assertTrue(j.isFormatted());

    final StorageInfo info = j.getStorage();
    LOG.info("Formatted: " + info);
    
    Assert.assertEquals(namespaceId, info.getNamespaceID());
    Assert.assertEquals(clusterId, info.getClusterID());
    j.close();
    
    //create another Journal object
    final StorageInfo another = new Journal(conf).getStorage();
    Assert.assertEquals(info.toString(), another.toString());
  }
}