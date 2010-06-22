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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Class that tests the multi-cluster in 1 JVM case, useful
 * only for "unit'ish tests".
 */
public class TestMultiClusters {

  private static final byte[] TABLE_NAME = Bytes.toBytes("test");
  private static final byte[] FAM_NAME = Bytes.toBytes("fam");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUAL_NAME = Bytes.toBytes("qual");
  private static final byte[] VALUE = Bytes.toBytes("value");

  /**
   * Basic sanity test that spins up 2 HDFS and HBase clusters that share the
   * same ZK ensemble. We then create the same table in both and make sure that
   * what we insert in one place doesn't end up in the other.
   * @throws Exception
   */
  @Test (timeout=100000)
  public void twoClusters() throws Exception{
    Configuration conf1 = HBaseConfiguration.create();
    // Different path for different clusters
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    HBaseTestingUtility utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();

    Configuration conf2 = HBaseConfiguration.create();
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    HBaseTestingUtility utility2 = new HBaseTestingUtility(conf2);
    // They share the same ensemble, but homed differently
    utility2.setZkCluster(utility1.getZkCluster());

    utility1.startMiniCluster();
    utility2.startMiniCluster();

    HTable table1 = utility1.createTable(TABLE_NAME, FAM_NAME);
    HTable table2 = utility2.createTable(TABLE_NAME, FAM_NAME);

    Put put = new Put(ROW);
    put.add(FAM_NAME, QUAL_NAME, VALUE);
    table1.put(put);

    Get get = new Get(ROW);
    get.addColumn(FAM_NAME, QUAL_NAME);
    Result res = table1.get(get);
    assertEquals(1, res.size());

    res = table2.get(get);
    assertEquals(0, res.size());
  }

}
