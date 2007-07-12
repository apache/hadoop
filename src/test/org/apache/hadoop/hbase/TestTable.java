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

/** Tests table creation restrictions*/
public class TestTable extends HBaseClusterTestCase {
  public TestTable() {
    super(true);
  }

  public void testTable() {
    HClient client = new HClient(conf);
    
    try {
      client.createTable(HGlobals.rootTableDesc);
      
    } catch(IllegalArgumentException e) {
      // Expected - ignore it
      
    } catch(Exception e) {
      System.err.println("Unexpected exception");
      e.printStackTrace();
      fail();
    }
    
    try {
      client.createTable(HGlobals.metaTableDesc);
      
    } catch(IllegalArgumentException e) {
      // Expected - ignore it
      
    } catch(Exception e) {
      System.err.println("Unexpected exception");
      e.printStackTrace();
      fail();
    }

    HTableDescriptor desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY.toString()));

    try {
      client.createTable(desc);
      
    } catch(Exception e) {
      System.err.println("Unexpected exception");
      e.printStackTrace();
      fail();
    }

    try {
      client.createTable(desc);
      
    } catch(IOException e) {
      // Expected. Ignore it.
      
    } catch(Exception e) {
      System.err.println("Unexpected exception");
      e.printStackTrace();
      fail();
    }
  }
}
