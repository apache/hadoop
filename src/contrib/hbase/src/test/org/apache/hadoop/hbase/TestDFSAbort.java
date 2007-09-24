/**
 * Copyright 2007 The Apache Software Foundation
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

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Test ability of HBase to handle DFS failure
 */
public class TestDFSAbort extends HBaseClusterTestCase {

  /** constructor */
  public TestDFSAbort() {
    super();
//    conf.setInt("ipc.client.timeout", 5000);            // reduce ipc client timeout
//    conf.setInt("ipc.client.connect.max.retries", 5);   // and number of retries
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(this.getClass().getPackage().getName()).setLevel(Level.DEBUG);
  }
  
  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    HTableDescriptor desc = new HTableDescriptor(getName());
    desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY_STR));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
  }
  
  /**
   * @throws Exception
   */
  public void testDFSAbort() throws Exception {
    // By now the Mini DFS is running, Mini HBase is running and we have
    // created a table. Now let's yank the rug out from HBase
    cluster.getDFSCluster().shutdown();
    // Now wait for Mini HBase Cluster to shut down
    cluster.join();
  }
  
  /**
   * @param args unused
   */
  public static void main(@SuppressWarnings("unused") String[] args) {
    TestRunner.run(new TestSuite(TestDFSAbort.class));
  }
}
