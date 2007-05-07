package org.apache.hadoop.hbase;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestToString extends TestCase {
  public void testServerInfo() throws Exception {
    final String hostport = "127.0.0.1:9999";
    HServerAddress address = new HServerAddress(hostport);
    assertEquals("HServerAddress toString", address.toString(), hostport);
    HServerInfo info = new HServerInfo(address, -1);
    assertEquals("HServerInfo", info.toString(),
        "address: " + hostport + ", startcode: " + -1);
  }
  
  public void testHRegionInfo() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("hank", 10);
    htd.addFamily(new Text("hankfamily:"));
    htd.addFamily(new Text("hankotherfamily:"));
    assertEquals("Table descriptor", htd.toString(),
     "name: hank, maxVersions: 10, families: [hankfamily:, hankotherfamily:]");
    HRegionInfo hri = new HRegionInfo(-1, htd, new Text(), new Text("10"));
    assertEquals("HRegionInfo", 
        "regionname: hank__-1, startKey: <>, tableDesc: {name: hank, " +
        "maxVersions: 10, families: [hankfamily:, hankotherfamily:]}",
        hri.toString());
  }
}
