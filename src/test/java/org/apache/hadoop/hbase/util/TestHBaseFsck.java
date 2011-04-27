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

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseFsck {

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static byte[] TABLE = Bytes.toBytes("table");
  private final static byte[] FAM = Bytes.toBytes("fam");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  private int doFsck(boolean fix) throws Exception {
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.displayFullReport();
    fsck.setTimeLag(0);
    fsck.setFixErrors(fix);
    // Most basic check ever, 0 tables
    return fsck.doWork();
  }

  @Test
  public void testHBaseFsck() throws Exception {
    int result = doFsck(false);
    assertEquals(0, result);

    TEST_UTIL.createTable(TABLE, FAM);

    // We created 1 table, should be fine
    result = doFsck(false);
    assertEquals(0, result);

    // Now let's mess it up and change the assignment in .META. to
    // point to a different region server
    HTable meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
    ResultScanner scanner = meta.getScanner(new Scan());

    resforloop : for (Result res : scanner) {
      long startCode = Bytes.toLong(res.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER));

      for (JVMClusterUtil.RegionServerThread rs :
          TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {

        ServerName sn = rs.getRegionServer().getServerName();

        // When we find a diff RS, change the assignment and break
        if (startCode != sn.getStartcode()) {
          Put put = new Put(res.getRow());
          put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(sn.getHostAndPort()));
          put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
            Bytes.toBytes(sn.getStartcode()));
          meta.put(put);
          break resforloop;
        }
      }
    }

    // Try to fix the data
    result = doFsck(true);
    assertEquals(-1, result);

    Thread.sleep(15000);
    result = doFsck(false);
    // Should have fixed
    assertEquals(0, result);
    new HTable(conf, TABLE).getScanner(new Scan());
  }
}
