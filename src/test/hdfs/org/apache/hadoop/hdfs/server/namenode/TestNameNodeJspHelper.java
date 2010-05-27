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
package org.apache.hadoop.hdfs.server.namenode;


import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNameNodeJspHelper {

  private MiniDFSCluster cluster = null;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    cluster  = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  @Test
  public void testDelegationToken() throws IOException, InterruptedException {
    NameNode nn = cluster.getNameNode();
    String tokenString = NamenodeJspHelper.getDelegationToken(nn, "SomeUser");
    //tokenString returned must be null because security is disabled
    Assert.assertEquals(null, tokenString);
  }

}
