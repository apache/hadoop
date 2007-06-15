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
    HTableDescriptor htd = new HTableDescriptor("hank");
    htd.addFamily(new HColumnDescriptor("hankfamily:"));
    htd.addFamily(new HColumnDescriptor(new Text("hankotherfamily:"), 10,
        HColumnDescriptor.CompressionType.BLOCK, true, 1000, null));
    assertEquals("Table descriptor", "name: hank, families: "
        + "{hankfamily:=(hankfamily:, max versions: 3, compression: none, "
        + "in memory: false, max value length: 2147483647, bloom filter: none), "
        + "hankotherfamily:=(hankotherfamily:, max versions: 10, "
        + "compression: block, in memory: true, max value length: 1000, "
        + "bloom filter: none)}", htd.toString());
    HRegionInfo hri = new HRegionInfo(-1, htd, new Text(), new Text("10"));
    System.out.println(hri.toString());
    assertEquals("HRegionInfo", 
        "regionname: hank__-1, startKey: <>, tableDesc: {" + "name: hank, "
        + "families: {hankfamily:=(hankfamily:, max versions: 3, "
        + "compression: none, in memory: false, max value length: 2147483647, "
        + "bloom filter: none), hankotherfamily:=(hankotherfamily:, "
        + "max versions: 10, compression: block, in memory: true, max value "
        + "length: 1000, bloom filter: none)}}",
        hri.toString());
  }
}
