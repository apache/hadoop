package org.apache.hadoop.hbase;

import java.util.TreeMap;
import org.apache.hadoop.io.Text;

/** Tests user specifyable time stamps */
public class TestTimestamp extends HBaseClusterTestCase {
  private static final long T0 = 10L;
  private static final long T1 = 100L;
  
  private static final String COLUMN_NAME = "contents:";
  private static final String TABLE_NAME = "test";
  private static final String VERSION1 = "version1";
  private static final String LATEST = "latest";
  
  private static final Text COLUMN = new Text(COLUMN_NAME);
  private static final Text[] COLUMNS = {
    COLUMN
  };
  private static final Text TABLE = new Text(TABLE_NAME);
  private static final Text ROW = new Text("row");
  
  private HClient client;

  /** constructor */
  public TestTimestamp() {
    super();
    client = new HClient(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));

    try {
      client.createTable(desc);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
  
  /** the test */
  public void testTimestamp() {
    try {
      client.openTable(TABLE);
      
      // store a value specifying an update time

      long lockid = client.startUpdate(ROW);
      client.put(lockid, COLUMN, VERSION1.getBytes(HConstants.UTF8_ENCODING));
      client.commit(lockid, T0);
      
      // store a value specifying 'now' as the update time
      
      lockid = client.startUpdate(ROW);
      client.put(lockid, COLUMN, LATEST.getBytes(HConstants.UTF8_ENCODING));
      client.commit(lockid);
      
      // delete values older than T1
      
      lockid = client.startUpdate(ROW);
      client.delete(lockid, COLUMN);
      client.commit(lockid, T1);
      
      // now retrieve...
      
      // the most recent version:
      
      byte[] bytes = client.get(ROW, COLUMN);
      assertTrue(bytes != null && bytes.length != 0);
      assertTrue(LATEST.equals(new String(bytes, HConstants.UTF8_ENCODING)));
      
      // any version <= time T1
      
      byte[][] values = client.get(ROW, COLUMN, T1, 3);
      assertNull(values);
      
      // the version from T0
      
      values = client.get(ROW, COLUMN, T0, 3);
      assertTrue(values.length == 1
          && VERSION1.equals(new String(values[0], HConstants.UTF8_ENCODING)));

      // flush everything out to disk
      
      HRegionServer s = cluster.regionServers.get(0);
      for(HRegion r: s.onlineRegions.values() ) {
        r.flushcache(false);
      }
      
      // now retrieve...
      
      // the most recent version:
      
      bytes = client.get(ROW, COLUMN);
      assertTrue(bytes != null && bytes.length != 0);
      assertTrue(LATEST.equals(new String(bytes, HConstants.UTF8_ENCODING)));
      
      // any version <= time T1
      
      values = client.get(ROW, COLUMN, T1, 3);
      assertNull(values);
      
      // the version from T0
      
      values = client.get(ROW, COLUMN, T0, 3);
      assertTrue(values.length == 1
          && VERSION1.equals(new String(values[0], HConstants.UTF8_ENCODING)));

      // three versions older than now
      
      values = client.get(ROW, COLUMN, 3);
      assertTrue(values.length == 1
          && LATEST.equals(new String(values[0], HConstants.UTF8_ENCODING)));
      
      // Test scanners
      
      HScannerInterface scanner =
        client.obtainScanner(COLUMNS, HClient.EMPTY_START_ROW);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        int count = 0;
        while(scanner.next(key, results)) {
          count++;
        }
        assertEquals(count, 1);
        assertEquals(results.size(), 1);
        
      } finally {
        scanner.close();
      }
      
      scanner = client.obtainScanner(COLUMNS, HClient.EMPTY_START_ROW, T1);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        int count = 0;
        while(scanner.next(key, results)) {
          count++;
        }
        assertEquals(count, 0);
        assertEquals(results.size(), 0);
        
      } finally {
        scanner.close();
      }
      
      scanner = client.obtainScanner(COLUMNS, HClient.EMPTY_START_ROW, T0);
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        int count = 0;
        while(scanner.next(key, results)) {
          count++;
        }
        assertEquals(count, 0);
        assertEquals(results.size(), 0);
        
      } finally {
        scanner.close();
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
