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

import org.apache.hadoop.io.Text;

/**
 * Tests that HClient protects against multiple updates
 */
public class TestMultipleUpdates extends HBaseClusterTestCase {
  private static final String CONTENTS_STR = "contents:";
  private static final Text CONTENTS = new Text(CONTENTS_STR);
  private static final byte[] value = { 1, 2, 3, 4 };

  private HTableDescriptor desc = null;
  private HTable table = null;

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

  /** the test */
  public void testMultipleUpdates() {
    try {
      long lockid = table.startUpdate(new Text("row1"));
      
      try {
        long lockid2 = table.startUpdate(new Text("row2"));
        throw new Exception("second startUpdate returned lock id " + lockid2);
        
      } catch (IllegalStateException i) {
        // expected
      }
      
      long invalidid = 42;
      
      try {
        table.put(invalidid, CONTENTS, value);
        
      } catch (IllegalArgumentException i) {
        // expected
      }
      
      try {
        table.delete(invalidid, CONTENTS);
        
      } catch (IllegalArgumentException i) {
        // expected
      }
      
      try {
        table.abort(invalidid);
        
      } catch (IllegalArgumentException i) {
        // expected
      }
      
      try {
        table.commit(invalidid);
        
      } catch (IllegalArgumentException i) {
        // expected
      }
      
      table.abort(lockid);
      
    } catch (Exception e) {
      System.err.println("unexpected exception");
      e.printStackTrace();
      fail();
    }
  }
}
