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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.hbase.filter.RegExpRowFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.MapWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * Additional scanner tests.
 * {@link TestScanner} does a custom setup/takedown not conducive
 * to addition of extra scanning tests.
 * @see TestScanner
 */
public class TestScanner2 extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  final char FIRST_ROWKEY = 'a';
  final char FIRST_BAD_RANGE_ROWKEY = 'j';
  final char LAST_BAD_RANGE_ROWKEY = 'q';
  final char LAST_ROWKEY = 'z';
  final char FIRST_COLKEY = '0';
  final char LAST_COLKEY = '3';
  final byte[] GOOD_BYTES = "goodstuff".getBytes();
  final byte[] BAD_BYTES = "badstuff".getBytes();

  /**
   * Test the scanner's handling of various filters.  
   * 
   * @throws Exception
   */
  public void testScannerFilter() throws Exception {
    // Setup HClient, ensure that it is running correctly
    HBaseAdmin admin = new HBaseAdmin(conf);
    
    // Setup colkeys to be inserted
    HTableDescriptor htd = new HTableDescriptor(getName());
    Text tableName = new Text(getName());
    Text[] colKeys = new Text[(LAST_COLKEY - FIRST_COLKEY) + 1];
    for (char i = 0; i < colKeys.length; i++) {
      colKeys[i] = new Text(new String(new char[] { 
        (char)(FIRST_COLKEY + i), ':' }));
      htd.addFamily(new HColumnDescriptor(colKeys[i].toString()));
    }
    admin.createTable(htd);
    assertTrue("Table with name " + tableName + " created successfully.", 
        admin.tableExists(tableName));
    assertTrue("Master is running.", admin.isMasterRunning());
    
    // Enter data
    HTable table = new HTable(conf, tableName);
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
    // First add a new table.  Its intial region will be added to META region.
    HBaseAdmin admin = new HBaseAdmin(conf);
    Text tableName = new Text(getName());
    admin.createTable(new HTableDescriptor(tableName.toString()));
    List<HRegionInfo> regions = scan(conf, HConstants.META_TABLE_NAME);
    assertEquals("Expected one region", regions.size(), 1);
    HRegionInfo region = regions.get(0);
    assertTrue("Expected region named for test",
      region.regionName.toString().startsWith(getName()));
    // Now do what happens at split time; remove old region and then add two
    // new ones in its place.
    removeRegionFromMETA(new HTable(conf, HConstants.META_TABLE_NAME),
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
    try {
      for (HRegion r : newRegions) {
        addRegionToMETA(conf, HConstants.META_TABLE_NAME, r,
            this.cluster.getHMasterAddress(), -1L);
      }
      regions = scan(conf, HConstants.META_TABLE_NAME);
      assertEquals("Should be two regions only", 2, regions.size());
    } finally {
      for (HRegion r : newRegions) {
        r.close();
        r.getLog().closeAndDelete();
      }
    }
  }
  
  private List<HRegionInfo> scan(final Configuration conf, final Text table)
  throws IOException {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    HRegionInterface regionServer = null;
    long scannerId = -1L;
    try {
      HTable t = new HTable(conf, table);
      HRegionLocation rl = t.getRegionLocation(table);
      regionServer = t.getConnection().getHRegionConnection(rl.getServerAddress());
      scannerId = regionServer.openScanner(rl.getRegionInfo().getRegionName(),
          HConstants.COLUMN_FAMILY_ARRAY, new Text(),
          System.currentTimeMillis(), null);
      while (true) {
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        MapWritable values = regionServer.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        
        for (Map.Entry<WritableComparable, Writable> e: values.entrySet()) {
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
  
  private void addRegionToMETA(final Configuration conf,
      final Text table, final HRegion region,
      final HServerAddress serverAddress,
      final long startCode)
  throws IOException {
    HTable t = new HTable(conf, table);
    try {
      long lockid = t.startUpdate(region.getRegionName());
      t.put(lockid, HConstants.COL_REGIONINFO, Writables.getBytes(region.getRegionInfo()));
      t.put(lockid, HConstants.COL_SERVER,
        Writables.stringToBytes(serverAddress.toString()));
      t.put(lockid, HConstants.COL_STARTCODE, Writables.longToBytes(startCode));
      t.commit(lockid);
      if (LOG.isDebugEnabled()) {
        LOG.info("Added region " + region.getRegionName() + " to table " +
            table);
      }
    } finally {
      t.close();
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
    try {
      long lockid = t.startUpdate(regionName);
      t.delete(lockid, HConstants.COL_REGIONINFO);
      t.delete(lockid, HConstants.COL_SERVER);
      t.delete(lockid, HConstants.COL_STARTCODE);
      t.commit(lockid);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removed " + regionName + " from table " + t.getTableName());
      }
    } finally {
      t.close();
    }
  }
}