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

package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests master cleanup of rows in meta table where there is no HRegionInfo
 */
public class TestEmptyMetaInfo extends HBaseClusterTestCase {
  /**
   * Insert some bogus rows in meta. Master should clean them up.
   * @throws IOException
   */
  public void testEmptyMetaInfo() throws IOException {
    HTable t = new HTable(conf, HConstants.META_TABLE_NAME);
    final int COUNT = 5;
    final byte [] tableName = Bytes.toBytes(getName());
    for (int i = 0; i < COUNT; i++) {
      byte [] regionName = HRegionInfo.createRegionName(tableName,
        Bytes.toBytes(i == 0? "": Integer.toString(i)),
        Long.toString(System.currentTimeMillis()));
      BatchUpdate b = new BatchUpdate(regionName);
      b.put(HConstants.COL_SERVER, Bytes.toBytes("localhost:1234"));
      t.commit(b);
    }
    long sleepTime =
      conf.getLong("hbase.master.meta.thread.rescanfrequency", 10000);
    int tries = conf.getInt("hbase.client.retries.number", 5);
    int count = 0;
    do {
      tries -= 1;
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        // ignore
      }
      Scanner scanner = t.getScanner(HConstants.ALL_META_COLUMNS, tableName);
      try {
        count = 0;
        for (RowResult r: scanner) {
          if (r.size() > 0) {
            count += 1;
          }
        }
      } finally {
        scanner.close();
      }
    } while (count != 0 && tries >= 0);
    assertTrue(tries >= 0);
    assertEquals(0, count);
  }
}