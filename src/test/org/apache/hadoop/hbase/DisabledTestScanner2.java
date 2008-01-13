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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.filter.RegExpRowFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Additional scanner tests.
 * {@link TestScanner} does a custom setup/takedown not conducive
 * to addition of extra scanning tests.
 *
 * <p>Temporarily disabled until hudson stabilizes again.
 * @see TestScanner
 */
public class DisabledTestScanner2 extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  final char FIRST_ROWKEY = 'a';
  final char FIRST_BAD_RANGE_ROWKEY = 'j';
  final char LAST_BAD_RANGE_ROWKEY = 'q';
  final char LAST_ROWKEY = 'z';
  final char FIRST_COLKEY = '0';
  final char LAST_COLKEY = '3';
  static byte[] GOOD_BYTES = null;
  static byte[] BAD_BYTES = null;

  static {
    try {
      GOOD_BYTES = "goodstuff".getBytes(HConstants.UTF8_ENCODING);
      BAD_BYTES = "badstuff".getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      fail();
    }
  }

  /**
   * Test for HADOOP-2467 fix.  If scanning more than one column family,
   * filters such as the {@line WhileMatchRowFilter} could prematurely
   * shutdown scanning if one of the stores ran started returned filter = true.
   * @throws MasterNotRunningException
   * @throws IOException
   */
  public void testScanningMultipleFamiliesOfDifferentVintage()
  throws MasterNotRunningException, IOException {
    Text tableName = new Text(getName());
    final Text [] families = createTable(new HBaseAdmin(this.conf), tableName);
    HTable table = new HTable(this.conf, tableName);
    HScannerInterface scanner = null;
    try {
      long time = System.currentTimeMillis();
      LOG.info("Current time " + time);
      for (int i = 0; i < families.length; i++) {
        final byte [] lastKey = new byte [] {'a', 'a', (byte)('b' + i)};
        Incommon inc = new HTableIncommon(table);
        addContent(inc, families[i].toString(),
          START_KEY_BYTES, new Text(lastKey), time + (1000 * i));
        // Add in to the first store a record that is in excess of the stop
        // row specified below setting up the scanner filter.  Add 'bbb'.
        // Use a stop filter of 'aad'.  The store scanner going to 'bbb' was
        // flipping the switch in StopRowFilter stopping us returning all
        // of the rest of the other store content.
        if (i == 0) {
          long id = inc.startBatchUpdate(new Text("bbb"));
          inc.put(id, families[0], "bbb".getBytes());
          inc.commit(id);
        }
      }
      RowFilterInterface f =
        new WhileMatchRowFilter(new StopRowFilter(new Text("aad")));
      scanner = table.obtainScanner(families, HConstants.EMPTY_START_ROW,
        HConstants.LATEST_TIMESTAMP, f);
      int count = 0;
      for (Map.Entry<HStoreKey, SortedMap<Text, byte []>> e: scanner) {
        count++;
      }
      // Should get back 3 rows: aaa, aab, and aac.
      assertEquals(count, 3);
    } finally {
      scanner.close();
      table.close();
    }
  }

  /**
   * @throws Exception
   */
  public void testStopRow() throws Exception {
    Text tableName = new Text(getName());
    createTable(new HBaseAdmin(this.conf), tableName);
    HTable table = new HTable(this.conf, tableName);
    try {
      final String lastKey = "aac";
      addContent(new HTableIncommon(table), FIRST_COLKEY + ":");
      HScannerInterface scanner =
        table.obtainScanner(new Text [] {new Text(FIRST_COLKEY + ":")},
            HConstants.EMPTY_START_ROW, new Text(lastKey));
      for (Map.Entry<HStoreKey, SortedMap<Text, byte []>> e: scanner) {
        if(e.getKey().getRow().toString().compareTo(lastKey) >= 0) {
          LOG.info(e.getKey());
          fail();
        }
      }
    } finally {
      table.close();
    }
  }
  
  /**
   * @throws Exception
   */
  public void testIterator() throws Exception {
    HTable table = new HTable(this.conf, HConstants.ROOT_TABLE_NAME);
    try {
      HScannerInterface scanner =
        table.obtainScanner(HConstants.COLUMN_FAMILY_ARRAY,
            HConstants.EMPTY_START_ROW);
      for (Map.Entry<HStoreKey, SortedMap<Text, byte []>> e: scanner) {
        assertNotNull(e.getKey());
        assertNotNull(e.getValue());
      }
    } finally {
      table.close();
    }
  }

  /**
   * Test getting scanners with regexes for column names.
   * @throws IOException 
   */
  public void testRegexForColumnName() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    
    // Setup colkeys to be inserted
    Text tableName = new Text(getName());
    createTable(admin, tableName);
    HTable table = new HTable(this.conf, tableName);
    try {
      // Add a row to columns without qualifiers and then two with.  Make one
      // numbers only so easy to find w/ a regex.
      long id = table.startUpdate(new Text(getName()));
      final String firstColkeyFamily = Character.toString(FIRST_COLKEY) + ":";
      table.put(id, new Text(firstColkeyFamily + getName()), GOOD_BYTES);
      table.put(id, new Text(firstColkeyFamily + "22222"), GOOD_BYTES);
      table.put(id, new Text(firstColkeyFamily), GOOD_BYTES);
      table.commit(id);
      // Now do a scan using a regex for a column name.
      checkRegexingScanner(table, firstColkeyFamily + "\\d+");
      // Do a new scan that only matches on column family.
      checkRegexingScanner(table, firstColkeyFamily + "$");
    } finally {
      table.close();
    }
  }
  
  /*
   * Create a scanner w/ passed in column name regex.  Assert we only get
   * back one column that matches.
   * @param table
   * @param regexColumnname
   * @throws IOException
   */
  private void checkRegexingScanner(final HTable table,
      final String regexColumnname) throws IOException {
    Text [] regexCol = new Text [] {new Text(regexColumnname)};
    HScannerInterface scanner =
      table.obtainScanner(regexCol, HConstants.EMPTY_START_ROW);
    HStoreKey key = new HStoreKey();
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    int count = 0;
    while (scanner.next(key, results)) {
      for (Text c: results.keySet()) {
        System.out.println(c);
        assertTrue(c.toString().matches(regexColumnname));
        count++;
      }
    }
    assertEquals(1, count);
    scanner.close();
  }

  /**
   * Test the scanner's handling of various filters.  
   * 
   * @throws Exception
   */
  public void testScannerFilter() throws Exception {
    // Setup HClient, ensure that it is running correctly
    HBaseAdmin admin = new HBaseAdmin(conf);
    
    // Setup colkeys to be inserted
    Text tableName = new Text(getName());
    Text [] colKeys = createTable(admin, tableName);
    assertTrue("Master is running.", admin.isMasterRunning());
    
    // Enter data
    HTable table = new HTable(conf, tableName);
    try {
      for (char i = FIRST_ROWKEY; i <= LAST_ROWKEY; i++) {
        Text rowKey = new Text(new String(new char[] { i }));
        long lockID = table.startUpdate(rowKey);
        for (char j = 0; j < colKeys.length; j++) {
          table.put(lockID, colKeys[j], (i >= FIRST_BAD_RANGE_ROWKEY && 
              i <= LAST_BAD_RANGE_ROWKEY)? BAD_BYTES : GOOD_BYTES);
        }
        table.commit(lockID);
      }

      regExpFilterTest(table, colKeys);
      rowFilterSetTest(table, colKeys);
    } finally {
      table.close();
    }
  }
  
  /**
   * @param admin
   * @param tableName
   * @return Returns column keys used making table.
   * @throws IOException
   */
  private Text [] createTable(final HBaseAdmin admin, final Text tableName)
  throws IOException {
    // Setup colkeys to be inserted
    HTableDescriptor htd = new HTableDescriptor(getName());
    Text[] colKeys = new Text[(LAST_COLKEY - FIRST_COLKEY) + 1];
    for (char i = 0; i < colKeys.length; i++) {
      colKeys[i] = new Text(new String(new char[] { 
        (char)(FIRST_COLKEY + i), ':' }));
      htd.addFamily(new HColumnDescriptor(colKeys[i].toString()));
    }
    admin.createTable(htd);
    assertTrue("Table with name " + tableName + " created successfully.", 
      admin.tableExists(tableName));
    return colKeys;
  }
  
  private void regExpFilterTest(HTable table, Text[] colKeys) 
    throws Exception {
    // Get the filter.  The RegExpRowFilter used should filter out vowels.
    Map<Text, byte[]> colCriteria = new TreeMap<Text, byte[]>();
    for (int i = 0; i < colKeys.length; i++) {
      colCriteria.put(colKeys[i], GOOD_BYTES);
    }
    RowFilterInterface filter = new RegExpRowFilter("[^aeiou]", colCriteria);

    // Create the scanner from the filter.
    HScannerInterface scanner = table.obtainScanner(colKeys, new Text(new 
      String(new char[] { FIRST_ROWKEY })), filter);

    // Iterate over the scanner, ensuring that results match the passed regex.
    iterateOnScanner(scanner, "[^aei-qu]");
  }
  
  private void rowFilterSetTest(HTable table, Text[] colKeys) 
    throws Exception {
    // Get the filter.  The RegExpRowFilter used should filter out vowels and 
    // the WhileMatchRowFilter(StopRowFilter) should filter out all rows 
    // greater than or equal to 'r'.
    Set<RowFilterInterface> filterSet = new HashSet<RowFilterInterface>();
    filterSet.add(new RegExpRowFilter("[^aeiou]"));
    filterSet.add(new WhileMatchRowFilter(new StopRowFilter(new Text("r"))));
    RowFilterInterface filter = 
      new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ALL, filterSet);
    
    // Create the scanner from the filter.
    HScannerInterface scanner = table.obtainScanner(colKeys, new Text(new 
        String(new char[] { FIRST_ROWKEY })), filter);
    
    // Iterate over the scanner, ensuring that results match the passed regex.
    iterateOnScanner(scanner, "[^aeior-z]");
  }
  
  private void iterateOnScanner(HScannerInterface scanner, String regexToMatch)
  throws Exception {
      // A pattern that will only match rows that should not have been filtered.
      Pattern p = Pattern.compile(regexToMatch);
      
      try {
        // Use the scanner to ensure all results match the above pattern.
        HStoreKey rowKey = new HStoreKey();
        TreeMap<Text, byte[]> columns = new TreeMap<Text, byte[]>();
        while (scanner.next(rowKey, columns)) {
          String key = rowKey.getRow().toString();
          assertTrue("Shouldn't have extracted '" + key + "'", 
            p.matcher(key).matches());
        }
      } finally {
        scanner.close();
      }
  }
  
  /**
   * Test scanning of META table around split.
   * There was a problem where only one of the splits showed in a scan.
   * Split deletes a row and then adds two new ones.
   * @throws IOException
   */
  public void testSplitDeleteOneAddTwoRegions() throws IOException {
    HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
    try {
      // First add a new table.  Its intial region will be added to META region.
      HBaseAdmin admin = new HBaseAdmin(conf);
      Text tableName = new Text(getName());
      admin.createTable(new HTableDescriptor(tableName.toString()));
      List<HRegionInfo> regions = scan(metaTable);
      assertEquals("Expected one region", 1, regions.size());
      HRegionInfo region = regions.get(0);
      assertTrue("Expected region named for test",
          region.getRegionName().toString().startsWith(getName()));
      // Now do what happens at split time; remove old region and then add two
      // new ones in its place.
      removeRegionFromMETA(metaTable, region.getRegionName());
      HTableDescriptor desc = region.getTableDesc();
      Path homedir = new Path(getName());
      List<HRegion> newRegions = new ArrayList<HRegion>(2);
      newRegions.add(HRegion.createHRegion(
          new HRegionInfo(desc, null, new Text("midway")),
          homedir, this.conf));
      newRegions.add(HRegion.createHRegion(
          new HRegionInfo(desc, new Text("midway"), null),
          homedir, this.conf));
      try {
        for (HRegion r : newRegions) {
          addRegionToMETA(metaTable, r, this.cluster.getHMasterAddress(),
              -1L);
        }
        regions = scan(metaTable);
        assertEquals("Should be two regions only", 2, regions.size());
      } finally {
        for (HRegion r : newRegions) {
          r.close();
          r.getLog().closeAndDelete();
        }
      }
    } finally {
      metaTable.close();
    }
  }
  
  private List<HRegionInfo> scan(final HTable t)
  throws IOException {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    HRegionInterface regionServer = null;
    long scannerId = -1L;
    try {
      HRegionLocation rl = t.getRegionLocation(t.getTableName());
      regionServer = t.getConnection().getHRegionConnection(rl.getServerAddress());
      scannerId = regionServer.openScanner(rl.getRegionInfo().getRegionName(),
          HConstants.COLUMN_FAMILY_ARRAY, new Text(),
          System.currentTimeMillis(), null);
      while (true) {
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        HbaseMapWritable values = regionServer.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        
        for (Map.Entry<Writable, Writable> e: values.entrySet()) {
          HStoreKey k = (HStoreKey) e.getKey();
          results.put(k.getColumn(),
              ((ImmutableBytesWritable) e.getValue()).get());
        }

        HRegionInfo info = (HRegionInfo) Writables.getWritable(
            results.get(HConstants.COL_REGIONINFO), new HRegionInfo());

        byte[] bytes = results.get(HConstants.COL_SERVER);
        String serverName = Writables.bytesToString(bytes);

        long startCode =
          Writables.bytesToLong(results.get(HConstants.COL_STARTCODE));

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
  
  private void addRegionToMETA(final HTable t, final HRegion region,
      final HServerAddress serverAddress,
      final long startCode)
  throws IOException {
    long lockid = t.startUpdate(region.getRegionName());
    t.put(lockid, HConstants.COL_REGIONINFO,
      Writables.getBytes(region.getRegionInfo()));
    t.put(lockid, HConstants.COL_SERVER,
      Writables.stringToBytes(serverAddress.toString()));
    t.put(lockid, HConstants.COL_STARTCODE, Writables.longToBytes(startCode));
    t.commit(lockid);
    // Assert added.
    byte [] bytes = t.get(region.getRegionName(), HConstants.COL_REGIONINFO);
    HRegionInfo hri = Writables.getHRegionInfo(bytes);
    assertEquals(region.getRegionId(), hri.getRegionId());
    if (LOG.isDebugEnabled()) {
      LOG.info("Added region " + region.getRegionName() + " to table " +
        t.getTableName());
    }
  }
  
  /*
   * Delete <code>region</code> from META <code>table</code>.
   * @param conf Configuration object
   * @param table META table we are to delete region from.
   * @param regionName Region to remove.
   * @throws IOException
   */
  private void removeRegionFromMETA(final HTable t, final Text regionName)
  throws IOException {
    long lockid = t.startUpdate(regionName);
    t.delete(lockid, HConstants.COL_REGIONINFO);
    t.delete(lockid, HConstants.COL_SERVER);
    t.delete(lockid, HConstants.COL_STARTCODE);
    t.commit(lockid);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed " + regionName + " from table " + t.getTableName());
    }
  }
}
