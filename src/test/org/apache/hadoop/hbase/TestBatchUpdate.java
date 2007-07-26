/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;

/**
 * Test batch updates
 */
public class TestBatchUpdate extends HBaseClusterTestCase {
  private static final String CONTENTS_STR = "contents:";
  private static final Text CONTENTS = new Text(CONTENTS_STR);
  private byte[] value;

  private HTableDescriptor desc = null;
  private HClient client = null;

  /** constructor */
  public TestBatchUpdate() {
    try {
      value = "abcd".getBytes(HConstants.UTF8_ENCODING);
      
    } catch (UnsupportedEncodingException e) {
      fail();
    }
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.client = new HClient(conf);
    this.desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS_STR));
    try {
      client.createTable(desc);
      client.openTable(desc.getName());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /** the test case */
  public void testBatchUpdate() {
    try {
      client.commitBatch(-1L);
      
    } catch (IllegalStateException e) {
      // expected
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    long lockid = client.startBatchUpdate(new Text("row1"));
    
    try {
      client.openTable(HConstants.META_TABLE_NAME);
      
    } catch (IllegalStateException e) {
      // expected
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    try {
      try {
        @SuppressWarnings("unused")
        long dummy = client.startUpdate(new Text("row2"));
      } catch (IllegalStateException e) {
        // expected
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
      client.put(lockid, CONTENTS, value);
      client.delete(lockid, CONTENTS);
      client.commitBatch(lockid);
      
      lockid = client.startBatchUpdate(new Text("row2"));
      client.put(lockid, CONTENTS, value);
      client.commit(lockid);
 
      Text[] columns = { CONTENTS };
      HScannerInterface scanner = client.obtainScanner(columns, new Text());
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      while(scanner.next(key, results)) {
        for(Map.Entry<Text, byte[]> e: results.entrySet()) {
          System.out.println(key + ": row: " + e.getKey() + " value: " + 
              new String(e.getValue()));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
