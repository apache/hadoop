/**
 * Copyright 2008 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HConnectionManager;

/**
 * Test is flakey.  Needs work.  Fails too often on hudson.
 */
public class DisabledTestMetaUtils extends HBaseClusterTestCase {
  public void testColumnEdits() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    final String oldColumn = "oldcolumn:";
    // Add three tables
    for (int i = 0; i < 5; i++) {
      HTableDescriptor htd = new HTableDescriptor(getName() + i);
      htd.addFamily(new HColumnDescriptor(oldColumn));
      admin.createTable(htd);
    }
    this.cluster.shutdown();
    this.cluster = null;
    MetaUtils utils = new MetaUtils(this.conf);
    // Add a new column to the third table, getName() + '2', and remove the old.
    final byte [] editTable = Bytes.toBytes(getName() + 2);
    final byte [] newColumn = Bytes.toBytes("newcolumn:");
    utils.addColumn(editTable, new HColumnDescriptor(newColumn));
    utils.deleteColumn(editTable, Bytes.toBytes(oldColumn));
    utils.shutdown();
    // Delete again so we go get it all fresh.
    HConnectionManager.deleteConnectionInfo(conf, false);
    // Now assert columns were added and deleted.
    this.cluster = new MiniHBaseCluster(this.conf, 1);
    // Now assert columns were added and deleted.
    HTable t = new HTable(conf, editTable);
    HTableDescriptor htd = t.getTableDescriptor();
    HColumnDescriptor hcd = htd.getFamily(newColumn);
    assertTrue(hcd != null);
    assertNull(htd.getFamily(Bytes.toBytes(oldColumn)));
  }
}
