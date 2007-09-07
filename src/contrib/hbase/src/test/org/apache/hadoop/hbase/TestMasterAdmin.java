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

/** tests administrative functions */
public class TestMasterAdmin extends HBaseClusterTestCase {
  private static final Text COLUMN_NAME = new Text("col1:");
  private static HTableDescriptor testDesc;
  static {
    testDesc = new HTableDescriptor("testadmin1");
    testDesc.addFamily(new HColumnDescriptor(COLUMN_NAME.toString()));
  }
  
  private HBaseAdmin admin;

  /** constructor */
  public TestMasterAdmin() {
    super(true);
    admin = null;
  }
  
  /** @throws Exception */
  public void testMasterAdmin() throws Exception {
    admin = new HBaseAdmin(conf);
    admin.createTable(testDesc);
    admin.disableTable(testDesc.getName());

    try {
      try {
        @SuppressWarnings("unused")
        HTable table = new HTable(conf, testDesc.getName());

      } catch(IllegalStateException e) {
        // Expected
      }

      admin.addColumn(testDesc.getName(), new HColumnDescriptor("col2:"));
      admin.enableTable(testDesc.getName());
      try {
        admin.deleteColumn(testDesc.getName(), new Text("col2:"));
        
      } catch(TableNotDisabledException e) {
        // Expected
      }

      admin.disableTable(testDesc.getName());
      admin.deleteColumn(testDesc.getName(), new Text("col2:"));
      
    } catch(Exception e) {
      e.printStackTrace();
      fail();
      
    } finally {
      admin.deleteTable(testDesc.getName());
    }
  }
}
