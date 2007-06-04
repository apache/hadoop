package org.apache.hadoop.hbase;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/**
 * Test comparing HBase objects.
 */
public class TestCompare extends TestCase {
  public void testHRegionInfo() {
    HRegionInfo a = new HRegionInfo(1, new HTableDescriptor("a"), null, null);
    HRegionInfo b = new HRegionInfo(2, new HTableDescriptor("b"), null, null);
    assertTrue(a.compareTo(b) != 0);
    HTableDescriptor t = new HTableDescriptor("t");
    Text midway = new Text("midway");
    a = new HRegionInfo(1, t, null, midway);
    b = new HRegionInfo(2, t, midway, null);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertEquals(a, a);
    assertTrue(a.compareTo(a) == 0);
    a = new HRegionInfo(1, t, new Text("a"), new Text("d"));
    b = new HRegionInfo(2, t, new Text("e"), new Text("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(1, t, new Text("aaaa"), new Text("dddd"));
    b = new HRegionInfo(2, t, new Text("e"), new Text("g"));
    assertTrue(a.compareTo(b) < 0);
    a = new HRegionInfo(1, t, new Text("aaaa"), new Text("dddd"));
    b = new HRegionInfo(2, t, new Text("aaaa"), new Text("eeee"));
    assertTrue(a.compareTo(b) < 0);
  }
}
