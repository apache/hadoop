/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Additional scanner tests.
 * {@link TestScanner} does a custom setup/takedown not conducive
 * to addition of extra scanning tests.
 * @see TestScanner
 */
public class TestScanner2 extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  /**
   * Test scanning of META table around split.
   * There was a problem where only one of the splits showed in a scan.
   * Split deletes a row and then adds two new ones.
   * @throws IOException
   */
  public void testSplitDeleteOneAddTwoRegions() throws IOException {
    // First add a new table.  Its intial region will be added to META region.
    HClient client = new HClient(this.conf);
    client.createTable(new HTableDescriptor(getName()));
    List<HRegionInfo> regions = scan(client, HConstants.META_TABLE_NAME);
    assertEquals("Expected one region", regions.size(), 1);
    HRegionInfo region = regions.get(0);
    assertTrue("Expected region named for test",
      region.regionName.toString().startsWith(getName()));
    // Now do what happens at split time; remove old region and then add two
    // new ones in its place.
    HRegion.removeRegionFromMETA(client, HConstants.META_TABLE_NAME,
      region.regionName);
    HTableDescriptor desc = region.tableDesc;
    Path homedir = new Path(getName());
    List<HRegion> newRegions = new ArrayList<HRegion>(2);
    newRegions.add(HRegion.createHRegion(
      new HRegionInfo(2L, desc, null, new Text("midway")),
      homedir, this.conf, null));
    newRegions.add(HRegion.createHRegion(
      new HRegionInfo(3L, desc, new Text("midway"), null),
        homedir, this.conf, null));
    for (HRegion r: newRegions) {
      HRegion.addRegionToMETA(client, HConstants.META_TABLE_NAME, r,
        this.cluster.getHMasterAddress(), -1L);
    }
    regions = scan(client, HConstants.META_TABLE_NAME);
    assertEquals("Should be two regions only", 2, regions.size());
  }
  
  private List<HRegionInfo> scan(final HClient client, final Text table)
  throws IOException {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    HRegionInterface regionServer = null;
    long scannerId = -1L;
    try {
      client.openTable(table);
      HClient.RegionLocation rl = client.getRegionLocation(table);
      regionServer = client.getHRegionConnection(rl.serverAddress);
      scannerId = regionServer.openScanner(rl.regionInfo.regionName,
          HMaster.METACOLUMNS, new Text());
      while (true) {
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        KeyedData[] values = regionServer.next(scannerId);
        if (values.length == 0) {
          break;
        }

        for (int i = 0; i < values.length; i++) {
          results.put(values[i].getKey().getColumn(), values[i].getData());
        }

        HRegionInfo info = HRegion.getRegionInfo(results);
        String serverName = HRegion.getServerName(results);
        long startCode = HRegion.getStartCode(results);
        LOG.info(Thread.currentThread().getName() + " scanner: "
            + Long.valueOf(scannerId) + ": regioninfo: {" + info.toString()
            + "}, server: " + serverName + ", startCode: " + startCode);
        regions.add(info);
      }
    } finally {
      try {
        if (scannerId != -1L) {
          if (regionServer != null) {
            regionServer.close(scannerId);
          }
        }
      } catch (IOException e) {
        LOG.error(e);
      }
    }
    return regions;
  }
}