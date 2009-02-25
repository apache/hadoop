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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TimestampTestBase;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests user specifiable time stamps putting, getting and scanning.  Also
 * tests same in presence of deletes.  Test cores are written so can be
 * run against an HRegion and against an HTable: i.e. both local and remote.
 */
public class TestTimestamp extends HBaseClusterTestCase {
  private static final Log LOG =
    LogFactory.getLog(TestTimestamp.class.getName());

  private static final String COLUMN_NAME = "contents:";
  private static final byte [] COLUMN = Bytes.toBytes(COLUMN_NAME);
  private static final int VERSIONS = 3;
  
  /**
   * Test that delete works according to description in <a
   * href="https://issues.apache.org/jira/browse/HADOOP-1784">hadoop-1784</a>.
   * @throws IOException
   */
  public void testDelete() throws IOException {
    final HRegion r = createRegion();
    try {
      final HRegionIncommon region = new HRegionIncommon(r);
      TimestampTestBase.doTestDelete(region, region);
    } finally {
      r.close();
      r.getLog().closeAndDelete();
    }
    LOG.info("testDelete() finished");    
  }

  /**
   * Test scanning against different timestamps.
   * @throws IOException
   */
  public void testTimestampScanning() throws IOException {
    final HRegion r = createRegion();
    try {
      final HRegionIncommon region = new HRegionIncommon(r);
      TimestampTestBase.doTestTimestampScanning(region, region);
    } finally {
      r.close();
      r.getLog().closeAndDelete();
    }
    LOG.info("testTimestampScanning() finished");
  }

  private HRegion createRegion() throws IOException {
    HTableDescriptor htd = createTableDescriptor(getName());
    htd.addFamily(new HColumnDescriptor(COLUMN, VERSIONS,
      CompressionType.NONE, false, false, Integer.MAX_VALUE,
      HConstants.FOREVER, false));
    return createNewHRegion(htd, null, null);
  }
}