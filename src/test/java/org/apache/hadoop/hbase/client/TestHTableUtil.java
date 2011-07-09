/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class provides tests for the {@link HTableUtil} class
 *
 */
public class TestHTableUtil {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  /**
   *
   * @throws Exception
   */
  @Test
  public void testBucketPut() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBucketPut");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    ht.setAutoFlush( false );
    
    List<Put> puts = new ArrayList<Put>();
    puts.add( createPut("row1") );
    puts.add( createPut("row2") );
    puts.add( createPut("row3") );
    puts.add( createPut("row4") );
    
    HTableUtil.bucketRsPut( ht, puts );
    
    Scan scan = new Scan();
    scan.addColumn(FAMILY, QUALIFIER);
    int count = 0;
    for(Result result : ht.getScanner(scan)) {
      count++;
    }
    LOG.info("bucket put count=" + count);
    assertEquals(count, puts.size());
   }

  private Put createPut(String row) {
    Put put = new Put( Bytes.toBytes(row));
    put.add(FAMILY, QUALIFIER, VALUE);
    return put;
  }
  
  /**
  *
  * @throws Exception
  */
 @Test
 public void testBucketBatch() throws Exception {
   byte [] TABLE = Bytes.toBytes("testBucketBatch");
   HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

   List<Row> rows = new ArrayList<Row>();
   rows.add( createPut("row1") );
   rows.add( createPut("row2") );
   rows.add( createPut("row3") );
   rows.add( createPut("row4") );
   
   HTableUtil.bucketRsBatch( ht, rows );
   
   Scan scan = new Scan();
   scan.addColumn(FAMILY, QUALIFIER);
   
   int count = 0;
   for(Result result : ht.getScanner(scan)) {
     count++;
   }
   LOG.info("bucket batch count=" + count);
   assertEquals(count, rows.size());
 }

}
