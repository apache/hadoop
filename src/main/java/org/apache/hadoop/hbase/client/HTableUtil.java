/**
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

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 * Utility class for HTable.
 * 
 *
 */
public class HTableUtil {

  private static final int INITIAL_LIST_SIZE = 250;
	
  /**
   * Processes a List of Puts and writes them to an HTable instance in RegionServer buckets via the htable.put method. 
   * This will utilize the writeBuffer, thus the writeBuffer flush frequency may be tuned accordingly via htable.setWriteBufferSize. 
   * <br><br>
   * The benefit of submitting Puts in this manner is to minimize the number of RegionServer RPCs in each flush.
   * <br><br>
   * Assumption #1:  Regions have been pre-created for the table.  If they haven't, then all of the Puts will go to the same region, 
   * defeating the purpose of this utility method. See the Apache HBase book for an explanation of how to do this.
   * <br>
   * Assumption #2:  Row-keys are not monotonically increasing.  See the Apache HBase book for an explanation of this problem.  
   * <br>
   * Assumption #3:  That the input list of Puts is big enough to be useful (in the thousands or more).  The intent of this
   * method is to process larger chunks of data.
   * <br>
   * Assumption #4:  htable.setAutoFlush(false) has been set.  This is a requirement to use the writeBuffer.
   * <br><br>
   * @param htable HTable instance for target HBase table
   * @param puts List of Put instances
   * @throws IOException if a remote or network exception occurs
   * 
   */
  public static void bucketRsPut(HTable htable, List<Put> puts) throws IOException {

    Map<String, List<Put>> putMap = createRsPutMap(htable, puts);
    for (List<Put> rsPuts: putMap.values()) {
      htable.put( rsPuts );
    }
    htable.flushCommits();
  }
	
  /**
   * Processes a List of Rows (Put, Delete) and writes them to an HTable instance in RegionServer buckets via the htable.batch method. 
   * <br><br>
   * The benefit of submitting Puts in this manner is to minimize the number of RegionServer RPCs, thus this will
   * produce one RPC of Puts per RegionServer.
   * <br><br>
   * Assumption #1:  Regions have been pre-created for the table.  If they haven't, then all of the Puts will go to the same region, 
   * defeating the purpose of this utility method. See the Apache HBase book for an explanation of how to do this.
   * <br>
   * Assumption #2:  Row-keys are not monotonically increasing.  See the Apache HBase book for an explanation of this problem.  
   * <br>
   * Assumption #3:  That the input list of Rows is big enough to be useful (in the thousands or more).  The intent of this
   * method is to process larger chunks of data.
   * <br><br>
   * This method accepts a list of Row objects because the underlying .batch method accepts a list of Row objects.
   * <br><br>
   * @param htable HTable instance for target HBase table
   * @param rows List of Row instances
   * @throws IOException if a remote or network exception occurs
   */
  public static void bucketRsBatch(HTable htable, List<Row> rows) throws IOException {

    try {
      Map<String, List<Row>> rowMap = createRsRowMap(htable, rows);
      for (List<Row> rsRows: rowMap.values()) {
        htable.batch( rsRows );
      }
    } catch (InterruptedException e) {
      throw new IOException(e); 
    }
		
  }

  private static Map<String,List<Put>> createRsPutMap(HTable htable, List<Put> puts) throws IOException {

    Map<String, List<Put>> putMap = new HashMap<String, List<Put>>();
    for (Put put: puts) {
      HRegionLocation rl = htable.getRegionLocation( put.getRow() );
      String hostname = rl.getHostname();
      List<Put> recs = putMap.get( hostname);
      if (recs == null) {
        recs = new ArrayList<Put>(INITIAL_LIST_SIZE);
    		putMap.put( hostname, recs);
      }
      recs.add(put);
    }
    return putMap;
  }

  private static Map<String,List<Row>> createRsRowMap(HTable htable, List<Row> rows) throws IOException {

    Map<String, List<Row>> rowMap = new HashMap<String, List<Row>>();
    for (Row row: rows) {
      HRegionLocation rl = htable.getRegionLocation( row.getRow() );
      String hostname = rl.getHostname();
      List<Row> recs = rowMap.get( hostname);
      if (recs == null) {
        recs = new ArrayList<Row>(INITIAL_LIST_SIZE);
        rowMap.put( hostname, recs);
      }
      recs.add(row);
    }
    return rowMap;
  }
		
}
