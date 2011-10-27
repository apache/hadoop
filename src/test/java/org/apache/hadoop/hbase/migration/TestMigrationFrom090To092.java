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
package org.apache.hadoop.hbase.migration;


import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.MetaMigrationRemovingHTD;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.Test;

/**
 * Migration tests that do not need spin up of a cluster.
 * @deprecated Remove after we release 0.92
 */
public class TestMigrationFrom090To092 {
  @Test
  public void testMigrateHRegionInfoFromVersion0toVersion1()
  throws IOException {
    HTableDescriptor htd =
      getHTableDescriptor("testMigrateHRegionInfoFromVersion0toVersion1");
    HRegionInfo090x ninety =
      new HRegionInfo090x(htd, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    byte [] bytes = Writables.getBytes(ninety);
    // Now deserialize into an HRegionInfo
    HRegionInfo hri = Writables.getHRegionInfo(bytes);
    Assert.assertEquals(hri.getTableNameAsString(),
      ninety.getTableDesc().getNameAsString());
    Assert.assertEquals(HRegionInfo.VERSION, hri.getVersion());
  }

  private HTableDescriptor getHTableDescriptor(final String name) {
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(new HColumnDescriptor("family"));
    return htd;
  }
}