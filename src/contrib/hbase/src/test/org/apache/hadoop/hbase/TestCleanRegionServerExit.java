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

/**
 * Tests region server failover when a region server exits.
 */
public class TestCleanRegionServerExit extends HBaseClusterTestCase {
  private HClient client;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.client = new HClient(conf);
  }
  
  public void testCleanRegionServerExit()
  throws IOException, InterruptedException {
    try {
      // When the META table can be opened, the region servers are running
      this.client.openTable(HConstants.META_TABLE_NAME);
      // Put something into the meta table.
      this.client.createTable(new HTableDescriptor(getName()));
      // Get current region server instance.
      HRegionServer hsr = this.cluster.regionServers.get(0);
      Thread hrst = this.cluster.regionThreads.get(0);
      // Start up a new one to take over serving of root and meta after we shut
      // down the current meta/root host.
      this.cluster.startRegionServer();
      // Now shutdown the region server and wait for it to go down.
      hsr.stop();
      hrst.join();
      // The recalibration of the client is not working properly.  FIX.
      // After above is fixed, add in assertions that we can get data from
      // newly located meta table.
    } catch(Exception e) {
      e.printStackTrace();
      fail();
    }
  }

/* Comment out till recalibration of client is working properly.

  public void testRegionServerAbort()
  throws IOException, InterruptedException {
    // When the META table can be opened, the region servers are running
    this.client.openTable(HConstants.META_TABLE_NAME);
    // Put something into the meta table.
    this.client.createTable(new HTableDescriptor(getName()));
    // Get current region server instance.
    HRegionServer hsr = this.cluster.regionServers.get(0);
    Thread hrst = this.cluster.regionThreads.get(0);
    // Start up a new one to take over serving of root and meta after we shut
    // down the current meta/root host.
    this.cluster.startRegionServer();
    // Force a region server to exit "ungracefully"
    hsr.abort();
    hrst.join();
    // The recalibration of the client is not working properly.  FIX.
    // After above is fixed, add in assertions that we can get data from
    // newly located meta table.
  }
*/
}
