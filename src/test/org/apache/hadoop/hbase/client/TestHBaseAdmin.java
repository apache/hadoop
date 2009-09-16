package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;


public class TestHBaseAdmin extends HBaseClusterTestCase {
  static final Log LOG = LogFactory.getLog(TestHBaseAdmin.class.getName());
  
  private String TABLE_STR = "testTable";
  private byte [] TABLE = Bytes.toBytes(TABLE_STR);
  private byte [] ROW = Bytes.toBytes("testRow");
  private byte [] FAMILY = Bytes.toBytes("testFamily");
  private byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private byte [] VALUE = Bytes.toBytes("testValue");
  
  private HBaseAdmin admin = null;
  private HConnection connection = null;
  
  /**
   * Constructor does nothing special, start cluster.
   */
  public TestHBaseAdmin() throws Exception{
    super();
  }

  
  public void testCreateTable() throws IOException {
    init();
    
    HTableDescriptor [] tables = connection.listTables();
    int numTables = tables.length;
    
    createTable(TABLE, FAMILY);
    tables = connection.listTables();
    
    assertEquals(numTables + 1, tables.length);
  }
  
  
  public void testDisableAndEnableTable() throws IOException {
    init();
    
    HTable ht =  createTable(TABLE, FAMILY);
    
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    
    admin.disableTable(TABLE);
    
    //Test that table is disabled
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = true;
    }
    assertEquals(true, ok);
    
    admin.enableTable(TABLE);
    
    //Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertEquals(true, ok);
  }
  
  
  public void testTableExist() throws IOException {
    init();
    boolean exist = false;
    
    exist = admin.tableExists(TABLE);
    assertEquals(false, exist);
    
    createTable(TABLE, FAMILY);
    
    exist = admin.tableExists(TABLE);
    assertEquals(true, exist);    
  }
  

//  public void testMajorCompact() throws Exception {
//    init();
//    
//    int testTableCount = 0;
//    int flushSleep = 1000;
//    int majocCompactSleep = 7000;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//    byte [][] ROWS = makeN(ROW, 5);
//    
//    Put put = new Put(ROWS[0]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[1]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[2]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.flush(TABLE);
//    Thread.sleep(flushSleep);
//    
//    put = new Put(ROWS[3]);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    admin.majorCompact(TABLE);
//    Thread.sleep(majocCompactSleep);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//        System.out.println(output);
//      }
//    }
//    assertEquals(1, testTableCount);
//  }
//  
//
//
//  public void testFlush_TableName() throws Exception {
//    init();
//
//    int initTestTableCount = 0;
//    int testTableCount = 0;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//
//    Put put = new Put(ROW);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          initTestTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//    
//    //Flushing 
//    admin.flush(TABLE);
//    Thread.sleep(2000);
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//
//    assertEquals(initTestTableCount + 1, testTableCount);
//  }
// 
//
//  public void testFlush_RegionName() throws Exception{
//    init();
//    int initTestTableCount = 0;
//    int testTableCount = 0;
//    String regionName = null;
//    
//    HTable ht = createTable(TABLE, FAMILY);
//
//    Put put = new Put(ROW);
//    put.add(FAMILY, QUALIFIER, VALUE);
//    ht.put(put);
//    
//    HRegion [] regions = null;
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String reg = Bytes.toString(region.getRegionName());
//      String table = reg.split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        regionName = reg;
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          initTestTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//    
//    //Flushing 
//    admin.flush(regionName);
//    Thread.sleep(2000);
//    
//    regions = connection.getRegionServerWithRetries(
//        new ServerCallable<HRegion []>(connection, TABLE, ROW) {
//          public HRegion [] call() throws IOException {
//            return server.getOnlineRegionsAsArray();
//          }
//        }
//    );
//    for(HRegion region : regions) {
//      String table = Bytes.toString(region.getRegionName()).split(",")[0];
//      if(table.equals(TABLE_STR)) {
//        String output = "table: " + table;
//        int i = 0;
//        for(int j : region.getStoresSize()) {
//          output += ", files in store " + i++ + "(" + j + ")";
//          testTableCount = j; 
//        }
//        if (LOG.isDebugEnabled()) {
//          LOG.debug(output);
//        }
//      }
//    }
//
//    assertEquals(initTestTableCount + 1, testTableCount);
//  }

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  @SuppressWarnings("unused")
  private byte [][] makeN(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  private HTable createTable(byte [] tableName, byte [] ... families) 
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    return new HTable(conf, tableName);
  }
  
  private void init() throws IOException {
    connection = new HBaseAdmin(conf).connection;
    admin = new HBaseAdmin(conf);
  }
  
}