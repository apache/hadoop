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

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;


public class TestQueryMatcher extends HBaseTestCase
implements HConstants {
  private final boolean PRINT = false;
  
  private byte [] row1;
  private byte [] row2;
  private byte [] fam1;
  private byte [] fam2;
  private byte [] col1;
  private byte [] col2;
  private byte [] col3;
  private byte [] col4;
  private byte [] col5;
  private byte [] col6;

  private byte [] data;

  private Get get;

  long ttl = Long.MAX_VALUE;
  KeyComparator rowComparator;

  public void setUp(){
    row1 = Bytes.toBytes("row1");
    row2 = Bytes.toBytes("row2");
    fam1 = Bytes.toBytes("fam1");
    fam2 = Bytes.toBytes("fam2");
    col1 = Bytes.toBytes("col1");
    col2 = Bytes.toBytes("col2");
    col3 = Bytes.toBytes("col3");
    col4 = Bytes.toBytes("col4");
    col5 = Bytes.toBytes("col5");
    col6 = Bytes.toBytes("col6");

    data = Bytes.toBytes("data");

    //Create Get
    get = new Get(row1);
    get.addFamily(fam1);
    get.addColumn(fam2, col2);
    get.addColumn(fam2, col4);
    get.addColumn(fam2, col5);

    rowComparator = KeyValue.KEY_COMPARATOR;

  }

  public void testMatch_ExplicitColumns() 
  throws IOException {
    //Moving up from the Tracker by using Gets and List<KeyValue> instead
    //of just byte [] 

    //Expected result
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.DONE);

    QueryMatcher qm = new QueryMatcher(get, fam2,
        get.getFamilyMap().get(fam2), ttl, rowComparator, 1);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, data));
    memstore.add(new KeyValue(row1, fam2, col2, data));
    memstore.add(new KeyValue(row1, fam2, col3, data));
    memstore.add(new KeyValue(row1, fam2, col4, data));
    memstore.add(new KeyValue(row1, fam2, col5, data));

    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<MatchCode> actual = new ArrayList<MatchCode>();

    for(KeyValue kv : memstore){
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected "+expected.get(i)+ 
            ", actual " +actual.get(i));
      }
    }
  }


  public void testMatch_Wildcard() 
  throws IOException {
    //Moving up from the Tracker by using Gets and List<KeyValue> instead
    //of just byte [] 

    //Expected result
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.NEXT);

    QueryMatcher qm = new QueryMatcher(get, fam2, null, ttl, rowComparator, 1);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, data));
    memstore.add(new KeyValue(row1, fam2, col2, data));
    memstore.add(new KeyValue(row1, fam2, col3, data));
    memstore.add(new KeyValue(row1, fam2, col4, data));
    memstore.add(new KeyValue(row1, fam2, col5, data));
    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<MatchCode> actual = new ArrayList<MatchCode>();

    for(KeyValue kv : memstore){
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected "+expected.get(i)+ 
            ", actual " +actual.get(i));
      }
    }
  }

}
