/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;


public class TestExplicitColumnTracker extends HBaseTestCase
implements HConstants {
  private boolean PRINT = false;

  private final byte [] col1 = Bytes.toBytes("col1");
  private final byte [] col2 = Bytes.toBytes("col2");
  private final byte [] col3 = Bytes.toBytes("col3");
  private final byte [] col4 = Bytes.toBytes("col4");
  private final byte [] col5 = Bytes.toBytes("col5");

  
  public void testGet_SingleVersion(){
    if(PRINT){
      System.out.println("SingleVersion");
    }

    //Create tracker
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    //Looking for every other
    columns.add(col2);
    columns.add(col4);
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.DONE);
    int maxVersions = 1;
    
    ColumnTracker exp = new ExplicitColumnTracker(columns, maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col5);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    //"Match"
    for(byte [] col : scanner){
      result.add(exp.checkColumn(col, 0, col.length));
    }
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }
  
  public void testGet_MultiVersion(){
    if(PRINT){
      System.out.println("\nMultiVersion");
    }
    
    //Create tracker
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    //Looking for every other
    columns.add(col2);
    columns.add(col4);
    
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.SKIP);

    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.SKIP);

    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.SKIP);

    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.DONE);

    expected.add(MatchCode.DONE);
    expected.add(MatchCode.DONE);
    expected.add(MatchCode.DONE);
    int maxVersions = 2;
    
    ColumnTracker exp = new ExplicitColumnTracker(columns, maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col5);
    scanner.add(col5);
    scanner.add(col5);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    //"Match"
    for(byte [] col : scanner){
      result.add(exp.checkColumn(col, 0, col.length));
    }
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }


  /**
   * hbase-2259
   */
  public void testStackOverflow(){
    int maxVersions = 1;
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 100000; i++) {
      columns.add(Bytes.toBytes("col"+i));
    }

    ColumnTracker explicit = new ExplicitColumnTracker(columns, maxVersions);
    for (int i = 0; i < 100000; i+=2) {
      byte [] col = Bytes.toBytes("col"+i);
      explicit.checkColumn(col, 0, col.length);
    }
    explicit.update();

    for (int i = 1; i < 100000; i+=2) {
      byte [] col = Bytes.toBytes("col"+i);
      explicit.checkColumn(col, 0, col.length);
    }
  }

  
}
