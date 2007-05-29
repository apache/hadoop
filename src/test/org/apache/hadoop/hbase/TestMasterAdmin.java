/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public class TestMasterAdmin extends HBaseClusterTestCase {
  private static final Text COLUMN_NAME = new Text("col1:");
  private static HTableDescriptor testDesc;
  static {
    testDesc = new HTableDescriptor("testadmin1");
    testDesc.addFamily(new HColumnDescriptor(COLUMN_NAME.toString()));
  }
  
  private HClient client;

  public TestMasterAdmin() {
    super(true);
    client = new HClient(conf);
  }
  
  public void testMasterAdmin() {
    try {
      client.createTable(testDesc);
      client.disableTable(testDesc.getName());
      
    } catch(Exception e) {
      e.printStackTrace();
      fail();
    }

    try {
      try {
        client.openTable(testDesc.getName());

      } catch(IllegalStateException e) {
        // Expected
      }

      client.addColumn(testDesc.getName(), new HColumnDescriptor("col2:"));
      client.enableTable(testDesc.getName());
      try {
        client.deleteColumn(testDesc.getName(), new Text("col2:"));
        
      } catch(TableNotDisabledException e) {
        // Expected
      }

      client.disableTable(testDesc.getName());
      client.deleteColumn(testDesc.getName(), new Text("col2:"));
      
    } catch(Exception e) {
      e.printStackTrace();
      fail();
      
    } finally {
      try {
        client.deleteTable(testDesc.getName());
        
      } catch(Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }
}
