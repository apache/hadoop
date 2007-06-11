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

import java.io.IOException;

/** Tests region server failover when a region server exits cleanly */
public class TestRegionServerAbort extends HBaseClusterTestCase {

  private HClient client;
  
  /** Constructor */
  public TestRegionServerAbort() {
    super(2);                                   // Start two region servers
    client = new HClient(conf);
  }
  
  /** The test */
  public void testRegionServerAbort() {
    try {
      // When the META table can be opened, the region servers are running
      
      client.openTable(HConstants.META_TABLE_NAME);
      
    } catch(IOException e) {
      e.printStackTrace();
      fail();
    }
    
    // Force a region server to exit "ungracefully"
    
    this.cluster.abortRegionServer(0);
    
    try {
      Thread.sleep(120000);              // Wait for cluster to adjust
      
    } catch(InterruptedException e) {
    }
  }

}
