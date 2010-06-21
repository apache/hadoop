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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestMetaScanner {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaScanner() throws Exception {
    LOG.info("Starting testMetaScanner");
    final byte[] TABLENAME = Bytes.toBytes("testMetaScanner");
    final byte[] FAMILY = Bytes.toBytes("family");
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    Configuration conf = TEST_UTIL.getConfiguration();
    HTable table = new HTable(conf, TABLENAME);
    TEST_UTIL.createMultiRegions(conf, table, FAMILY,
        new byte[][]{
          HConstants.EMPTY_START_ROW,
          Bytes.toBytes("region_a"),
          Bytes.toBytes("region_b")});
    // Make sure all the regions are deployed
    TEST_UTIL.countRows(table);
    
    MetaScanner.MetaScannerVisitor visitor = 
      mock(MetaScanner.MetaScannerVisitor.class);
    doReturn(true).when(visitor).processRow((Result)anyObject());

    // Scanning the entire table should give us three rows
    MetaScanner.metaScan(conf, visitor, TABLENAME);
    verify(visitor, times(3)).processRow((Result)anyObject());
    
    // Scanning the table with a specified empty start row should also
    // give us three META rows
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, HConstants.EMPTY_BYTE_ARRAY, 1000);
    verify(visitor, times(3)).processRow((Result)anyObject());
    
    // Scanning the table starting in the middle should give us two rows:
    // region_a and region_b
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, Bytes.toBytes("region_ac"), 1000);
    verify(visitor, times(2)).processRow((Result)anyObject());
    
    // Scanning with a limit of 1 should only give us one row
    reset(visitor);
    doReturn(true).when(visitor).processRow((Result)anyObject());
    MetaScanner.metaScan(conf, visitor, TABLENAME, Bytes.toBytes("region_ac"), 1);
    verify(visitor, times(1)).processRow((Result)anyObject());
        
  }
}
