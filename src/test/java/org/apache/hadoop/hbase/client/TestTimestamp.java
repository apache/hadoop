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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TimestampTestBase;

/**
 * Tests user specifiable time stamps putting, getting and scanning.  Also
 * tests same in presence of deletes.  Test cores are written so can be
 * run against an HRegion and against an HTable: i.e. both local and remote.
 */
public class TestTimestamp extends HBaseClusterTestCase {
  public static String COLUMN_NAME = "colfamily11";

  /** constructor */
  public TestTimestamp() {
    super();
  }

  /**
   * Basic test of timestamps.
   * Do the above tests from client side.
   * @throws IOException
   */
  public void testTimestamps() throws IOException {
    HTable t = createTable();
    Incommon incommon = new HTableIncommon(t);
    TimestampTestBase.doTestDelete(incommon, new FlushCache() {
      public void flushcache() throws IOException {
        cluster.flushcache();
      }
     });

    // Perhaps drop and readd the table between tests so the former does
    // not pollute this latter?  Or put into separate tests.
    TimestampTestBase.doTestTimestampScanning(incommon, new FlushCache() {
      public void flushcache() throws IOException {
        cluster.flushcache();
      }
    });
  }

  /*
   * Create a table named TABLE_NAME.
   * @return An instance of an HTable connected to the created table.
   * @throws IOException
   */
  private HTable createTable() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(getName());
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, getName());
  }
}
