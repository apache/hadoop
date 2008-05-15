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
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.filter.RegExpRowFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Additional scanner tests.
 * {@link org.apache.hadoop.hbase.regionserver.TestScanner} does a custom
 * setup/takedown not conducive
 * to addition of extra scanning tests.
 *
 * <p>Temporarily disabled until hudson stabilizes again.
 * @see org.apache.hadoop.hbase.regionserver.TestScanner
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
    final byte [][] families = createTable(new HBaseAdmin(this.conf),
      getName());
    HTable table = new HTable(this.conf, getName());
    Scanner scanner = null;
    try {
      long time = System.currentTimeMillis();
      LOG.info("Current time " + time);
      for (int i = 0; i < families.length; i++) {
        final byte [] lastKey = new byte [] {'a', 'a', (byte)('b' + i)};
        Incommon inc = new HTableIncommon(table);
        addContent(inc, families[i].toString(),
          START_KEY_BYTES, lastKey, time + (1000 * i));
        // Add in to the first store a record that is in excess of the stop
        // row specified below setting up the scanner filter.  Add 'bbb'.
        // Use a stop filter of 'aad'.  The store scanner going to 'bbb' was
        // flipping the switch in StopRowFilter stopping us returning all
        // of the rest of the other store content.
        if (i == 0) {
          BatchUpdate batchUpdate = new BatchUpdate(Bytes.toBytes("bbb"));
          batchUpdate.put(families[0], "bbb".getBytes());
          inc.commit(batchUpdate);
        }
      }
      RowFilterInterface f =
        new WhileMatchRowFilter(new StopRowFilter(Bytes.toBytes("aad")));
      scanner = table.getScanner(families, HConstants.EMPTY_START_ROW,
        HConstants.LATEST_TIMESTAMP, f);
      int count = 0;
      for (RowResult e: scanner) {
        count++;
      }
      // Should get back 3 rows: aaa, aab, and aac.
      assertEquals(count, 3);
    } finally {
      scanner.close();
    }
  }

  /**
   * @throws Exception
   */
  public void testStopRow() throws Exception {
    createTable(new HBaseAdmin(this.conf), getName());
    HTable table = new HTable(this.conf, getName());
    final String lastKey = "aac";
    addContent(new HTableIncommon(table), FIRST_COLKEY + ":");
    byte [][] cols = new byte [1][];
    cols[0] = Bytes.toBytes(FIRST_COLKEY + ":");
    Scanner scanner = table.getScanner(cols,
      HConstants.EMPTY_START_ROW, Bytes.toBytes(lastKey));
    for (RowResult e: scanner) {
      if(e.getRow().toString().compareTo(lastKey) >= 0) {
        LOG.info(e.getRow());
        fail();
      }
    }
  }
  
  /**
   * @throws Exception
   */
  public void testIterator() throws Exception {
    HTable table = new HTable(this.conf, HConstants.ROOT_TABLE_NAME);
    Scanner scanner =
      table.getScanner(HConstants.COLUMN_FAMILY_ARRAY,
          HConstants.EMPTY_START_ROW);
    for (RowResult e: scanner) {
      assertNotNull(e);
      assertNotNull(e.getRow());
    }
  }

  /**
   * Test getting scanners with regexes for column names.
   * @throws IOException 
   */
  public void testRegexForColumnName() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    
    // Setup colkeys to be inserted
    createTable(admin, getName());
    HTable table = new HTable(this.conf, getName());
    // Add a row to columns without qualifiers and then two with.  Make one
    // numbers only so easy to find w/ a regex.
    BatchUpdate batchUpdate = new BatchUpdate(getName());
    final String firstColkeyFamily = Character.toString(FIRST_COLKEY) + ":";
    batchUpdate.put(firstColkeyFamily + getName(), GOOD_BYTES);
    batchUpdate.put(firstColkeyFamily + "22222", GOOD_BYTES);
    batchUpdate.put(firstColkeyFamily, GOOD_BYTES);
    table.commit(batchUpdate);
    // Now do a scan using a regex for a column name.
    checkRegexingScanner(table, firstColkeyFamily + "\\d+");
    // Do a new scan that only matches on column family.
    checkRegexingScanner(table, firstColkeyFamily + "$");
  }
  
  /*
   * Create a scanner w/ passed in column name regex.  Assert we only get
   * back one column that matches.
   * @param table
   * @param regexColumnname
   * @throws IOException
   */
  private void checkRegexingScanner(final HTable table, 
    final String regexColumnname) 
  throws IOException {
    byte [][] regexCols = new byte[1][];
    regexCols[0] = Bytes.toBytes(regexColumnname);
    Scanner scanner = table.getScanner(regexCols, HConstants.EMPTY_START_ROW);
    int count = 0;
    for (RowResult r : scanner) {
      for (byte [] c: r.keySet()) {
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
    byte [][] colKeys = createTable(admin, getName());
    assertTrue("Master is running.", admin.isMasterRunning());
    
    // Enter data
    HTable table = new HTable(conf, getName());
    for (char i = FIRST_ROWKEY; i <= LAST_ROWKEY; i++) {
      byte [] rowKey = new byte [] { (byte)i };
      BatchUpdate batchUpdate = new BatchUpdate(rowKey);
      for (char j = 0; j < colKeys.length; j++) {
        batchUpdate.put(colKeys[j], (i >= FIRST_BAD_RANGE_ROWKEY && 
            i <= LAST_BAD_RANGE_ROWKEY)? BAD_BYTES : GOOD_BYTES);
      }
      table.commit(batchUpdate);
    }

    regExpFilterTest(table, colKeys);
    rowFilterSetTest(table, colKeys);
  }
  
  /**
   * @param admin
   * @param tableName
   * @return Returns column keys used making table.
   * @throws IOException
   */
  private byte [][] createTable(final HBaseAdmin admin, final String tableName)
  throws IOException {
    // Setup colkeys to be inserted
    HTableDescriptor htd = new HTableDescriptor(getName());
    byte [][] colKeys = new byte[(LAST_COLKEY - FIRST_COLKEY) + 1][];
    for (char i = 0; i < colKeys.length; i++) {
      colKeys[i] = new byte [] {(byte)(FIRST_COLKEY + i), ':' };
      htd.addFamily(new HColumnDescriptor(colKeys[i].toString()));
    }
    admin.createTable(htd);
    assertTrue("Table with name " + tableName + " created successfully.", 
      admin.tableExists(tableName));
    return colKeys;
  }
  
  private void regExpFilterTest(HTable table, byte [][] colKeys) 
    throws Exception {
    // Get the filter.  The RegExpRowFilter used should filter out vowels.
    Map<byte [], byte[]> colCriteria =
      new TreeMap<byte [], byte[]>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < colKeys.length; i++) {
      colCriteria.put(colKeys[i], GOOD_BYTES);
    }
    RowFilterInterface filter = new RegExpRowFilter("[^aeiou]", colCriteria);

    // Create the scanner from the filter.
    Scanner scanner = table.getScanner(colKeys, new byte [] { FIRST_ROWKEY },
      filter);

    // Iterate over the scanner, ensuring that results match the passed regex.
    iterateOnScanner(scanner, "[^aei-qu]");
  }
  
  private void rowFilterSetTest(HTable table, byte [][] colKeys) 
  throws Exception {
    // Get the filter.  The RegExpRowFilter used should filter out vowels and 
    // the WhileMatchRowFilter(StopRowFilter) should filter out all rows 
    // greater than or equal to 'r'.
    Set<RowFilterInterface> filterSet = new HashSet<RowFilterInterface>();
    filterSet.add(new RegExpRowFilter("[^aeiou]"));
    filterSet.add(new WhileMatchRowFilter(new StopRowFilter(Bytes.toBytes("r"))));
    RowFilterInterface filter = 
      new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ALL, filterSet);
    
    // Create the scanner from the filter.
    Scanner scanner = table.getScanner(colKeys, new byte [] { FIRST_ROWKEY },
      filter);
    
    // Iterate over the scanner, ensuring that results match the passed regex.
    iterateOnScanner(scanner, "[^aeior-z]");
  }
  
  private void iterateOnScanner(Scanner scanner, String regexToMatch)
  throws Exception {
    // A pattern that will only match rows that should not have been filtered.
    Pattern p = Pattern.compile(regexToMatch);
    
    try {
      // Use the scanner to ensure all results match the above pattern.
      for (RowResult r : scanner) {
        String key = r.getRow().toString();
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
    // First add a new table.  Its intial region will be added to META region.
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(new HTableDescriptor(getName()));
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
        new HRegionInfo(desc, null, Bytes.toBytes("midway")),
        homedir, this.conf));
    newRegions.add(HRegion.createHRegion(
        new HRegionInfo(desc, Bytes.toBytes("midway"), null),
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
          HConstants.COLUMN_FAMILY_ARRAY, HConstants.EMPTY_START_ROW,
          HConstants.LATEST_TIMESTAMP, null);
      while (true) {
        RowResult values = regionServer.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        
        HRegionInfo info = (HRegionInfo) Writables.getWritable(
          values.get(HConstants.COL_REGIONINFO).getValue(), new HRegionInfo());

        String serverName = 
          Writables.cellToString(values.get(HConstants.COL_SERVER));

        long startCode =
          Writables.cellToLong(values.get(HConstants.COL_STARTCODE));

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
    BatchUpdate batchUpdate = new BatchUpdate(region.getRegionName());
    batchUpdate.put(HConstants.COL_REGIONINFO,
      Writables.getBytes(region.getRegionInfo()));
    batchUpdate.put(HConstants.COL_SERVER,
      Bytes.toBytes(serverAddress.toString()));
    batchUpdate.put(HConstants.COL_STARTCODE, Bytes.toBytes(startCode));
    t.commit(batchUpdate);
    // Assert added.
    byte [] bytes = 
      t.get(region.getRegionName(), HConstants.COL_REGIONINFO).getValue();
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
  private void removeRegionFromMETA(final HTable t, final byte [] regionName)
  throws IOException {
    BatchUpdate batchUpdate = new BatchUpdate(regionName);
    batchUpdate.delete(HConstants.COL_REGIONINFO);
    batchUpdate.delete(HConstants.COL_SERVER);
    batchUpdate.delete(HConstants.COL_STARTCODE);
    t.commit(batchUpdate);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed " + regionName + " from table " + t.getTableName());
    }
  }
}
