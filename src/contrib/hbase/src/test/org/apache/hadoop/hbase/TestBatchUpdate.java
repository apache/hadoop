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

import java.io.IOException;
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
  private HTable table = null;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestBatchUpdate() throws UnsupportedEncodingException {
    super();
    value = "abcd".getBytes(HConstants.UTF8_ENCODING);
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS_STR));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }

  /**
   * @throws IOException
   */
  public void testBatchUpdate() throws IOException {
    try {
      table.commit(-1L);
      
    } catch (IllegalStateException e) {
      // expected
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    long lockid = table.startUpdate(new Text("row1"));
    
    try {
      @SuppressWarnings("unused")
      long dummy = table.startUpdate(new Text("row2"));
    } catch (IllegalStateException e) {
      // expected
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    table.put(lockid, CONTENTS, value);
    table.delete(lockid, CONTENTS);
    table.commit(lockid);

    lockid = table.startUpdate(new Text("row2"));
    table.put(lockid, CONTENTS, value);
    table.commit(lockid);

    Text[] columns = { CONTENTS };
    HScannerInterface scanner = table.obtainScanner(columns, new Text());
    HStoreKey key = new HStoreKey();
    TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    while(scanner.next(key, results)) {
      for(Map.Entry<Text, byte[]> e: results.entrySet()) {
        System.out.println(key + ": row: " + e.getKey() + " value: " + 
            new String(e.getValue(), HConstants.UTF8_ENCODING));
      }
    }
  }
}
