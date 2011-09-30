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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;


public class TestExplicitColumnTracker extends HBaseTestCase {
  private boolean PRINT = false;

  private final byte[] col1 = Bytes.toBytes("col1");
  private final byte[] col2 = Bytes.toBytes("col2");
  private final byte[] col3 = Bytes.toBytes("col3");
  private final byte[] col4 = Bytes.toBytes("col4");
  private final byte[] col5 = Bytes.toBytes("col5");

  private void runTest(int maxVersions,
                       TreeSet<byte[]> trackColumns,
                       List<byte[]> scannerColumns,
                       List<MatchCode> expected) throws IOException {
    ColumnTracker exp = new ExplicitColumnTracker(
      trackColumns, 0, maxVersions, Long.MAX_VALUE);


    //Initialize result
    List<ScanQueryMatcher.MatchCode> result = new ArrayList<ScanQueryMatcher.MatchCode>();

    long timestamp = 0;
    //"Match"
    for(byte [] col : scannerColumns){
      result.add(exp.checkColumn(col, 0, col.length, ++timestamp));
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

  public void testGet_SingleVersion() throws IOException{
    if(PRINT){
      System.out.println("SingleVersion");
    }

    //Create tracker
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    //Looking for every other
    columns.add(col2);
    columns.add(col4);
    List<MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);             // col1
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL); // col2
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);             // col3    
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW); // col4
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);             // col5    
    int maxVersions = 1;

    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col5);

    runTest(maxVersions, columns, scanner, expected);
  }

  public void testGet_MultiVersion() throws IOException{
    if(PRINT){
      System.out.println("\nMultiVersion");
    }

    //Create tracker
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    //Looking for every other
    columns.add(col2);
    columns.add(col4);

    List<ScanQueryMatcher.MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);                   // col2; 1st version
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL); // col2; 2nd version
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);

    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);                   // col4; 1st version
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW); // col4; 2nd version
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);

    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW);
    int maxVersions = 2;

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
    runTest(maxVersions, columns, scanner, expected);
  }


  /**
   * hbase-2259
   */
  public void testStackOverflow() throws IOException{
    int maxVersions = 1;
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < 100000; i++) {
      columns.add(Bytes.toBytes("col"+i));
    }

    ColumnTracker explicit = new ExplicitColumnTracker(columns, 0, maxVersions,
        Long.MAX_VALUE);
    for (int i = 0; i < 100000; i+=2) {
      byte [] col = Bytes.toBytes("col"+i);
      explicit.checkColumn(col, 0, col.length, 1);
    }
    explicit.update();

    for (int i = 1; i < 100000; i+=2) {
      byte [] col = Bytes.toBytes("col"+i);
      explicit.checkColumn(col, 0, col.length, 1);
    }
  }

  /**
   * Regression test for HBASE-2545
   */
  public void testInfiniteLoop() throws IOException {
    TreeSet<byte[]> columns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    columns.addAll(Arrays.asList(new byte[][] {
      col2, col3, col5 }));
    List<byte[]> scanner = Arrays.<byte[]>asList(
      new byte[][] { col1, col4 });
    List<ScanQueryMatcher.MatchCode> expected = Arrays.<ScanQueryMatcher.MatchCode>asList(
      new ScanQueryMatcher.MatchCode[] {
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL });
    runTest(1, columns, scanner, expected);
  }
}
