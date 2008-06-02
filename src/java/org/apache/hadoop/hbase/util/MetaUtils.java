/**
 * Copyright 2008 The Apache Software Foundation
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

package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

/**
 * Contains utility methods for manipulating HBase meta tables.
 * Be sure to call {@link #shutdown()} when done with this class so it closes
 * resources opened during meta processing (ROOT, META, etc.).
 */
public class MetaUtils {
  private static final Log LOG = LogFactory.getLog(MetaUtils.class);
  
  private final HBaseConfiguration conf;
  boolean initialized;
  private FileSystem fs;
  private Path rootdir;
  private HLog log;
  private HRegion rootRegion;
  private Map<byte [], HRegion> metaRegions = Collections.synchronizedSortedMap(
    new TreeMap<byte [], HRegion>(Bytes.BYTES_COMPARATOR));
  
  /** Default constructor */
  public MetaUtils() {
    this(new HBaseConfiguration());
  }
  
  /** @param conf HBaseConfiguration */
  public MetaUtils(HBaseConfiguration conf) {
    this.conf = conf;
    conf.setInt("hbase.client.retries.number", 1);
    this.initialized = false;
    this.rootRegion = null;
  }

  /**
   * Verifies that DFS is available and that HBase is off-line.
   * 
   * @return Path of root directory of HBase installation
   * @throws IOException
   */
  public Path initialize() throws IOException {
    if (!initialized) {
      this.fs = FileSystem.get(this.conf);              // get DFS handle

      // Get root directory of HBase installation

      this.rootdir =
        fs.makeQualified(new Path(this.conf.get(HConstants.HBASE_DIR)));

      if (!fs.exists(rootdir)) {
        String message = "HBase root directory " + rootdir.toString() +
        " does not exist.";
        LOG.error(message);
        throw new FileNotFoundException(message);
      }

      this.log = new HLog(this.fs, 
          new Path(this.fs.getHomeDirectory(),
              HConstants.HREGION_LOGDIR_NAME + "_" + System.currentTimeMillis()
          ),
          this.conf, null
      );

      this.initialized = true;
    }
    return this.rootdir;
  }
  
  /** @return true if initialization completed successfully */
  public boolean isInitialized() {
    return this.initialized;
  }
  
  /** @return the HLog */
  public HLog getLog() {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    return this.log;
  }
  
  /**
   * @return HRegion for root region
   * @throws IOException
   */
  public HRegion getRootRegion() throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    if (this.rootRegion == null) {
      openRootRegion();
    }
    return this.rootRegion;
  }
  
  /**
   * Open or return cached opened meta region
   * 
   * @param metaInfo HRegionInfo for meta region
   * @return meta HRegion
   * @throws IOException
   */
  public HRegion getMetaRegion(HRegionInfo metaInfo) throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    HRegion meta = metaRegions.get(metaInfo.getRegionName());
    if (meta == null) {
      meta = openMetaRegion(metaInfo);
      this.metaRegions.put(metaInfo.getRegionName(), meta);
    }
    return meta;
  }
  
  /**
   * Closes catalog regions if open. Also closes and deletes the HLog. You
   * must call this method if you want to persist changes made during a
   * MetaUtils edit session.
   */
  public void shutdown() {
    if (this.rootRegion != null) {
      try {
        this.rootRegion.close();
      } catch (IOException e) {
        LOG.error("closing root region", e);
      } finally {
        this.rootRegion = null;
      }
    }
    try {
      for (HRegion r: metaRegions.values()) {
        r.close();
      }
    } catch (IOException e) {
      LOG.error("closing meta region", e);
    } finally {
      metaRegions.clear();
    }
    try {
      this.log.rollWriter();
      this.log.closeAndDelete();
    } catch (IOException e) {
      LOG.error("closing HLog", e);
    } finally {
      this.log = null;
    }
    this.initialized = false;
  }

  /**
   * Used by scanRootRegion and scanMetaRegion to call back the caller so it
   * can process the data for a row.
   */
  public interface ScannerListener {
    /**
     * Callback so client of scanner can process row contents
     * 
     * @param info HRegionInfo for row
     * @return false to terminate the scan
     * @throws IOException
     */
    public boolean processRow(HRegionInfo info) throws IOException;
  }
  
  /**
   * Scans the root region. For every meta region found, calls the listener with
   * the HRegionInfo of the meta region.
   * 
   * @param listener method to be called for each meta region found
   * @throws IOException
   */
  public void scanRootRegion(ScannerListener listener) throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    
    // Open root region so we can scan it
    if (this.rootRegion == null) {
      openRootRegion();
    }

    InternalScanner rootScanner = rootRegion.getScanner(
        HConstants.COL_REGIONINFO_ARRAY, HConstants.EMPTY_START_ROW,
        HConstants.LATEST_TIMESTAMP, null);

    try {
      HStoreKey key = new HStoreKey();
      SortedMap<byte [], byte[]> results =
        new TreeMap<byte [], byte[]>(Bytes.BYTES_COMPARATOR);
      while (rootScanner.next(key, results)) {
        HRegionInfo info = Writables.getHRegionInfoOrNull(
            results.get(HConstants.COL_REGIONINFO));
        if (info == null) {
          LOG.warn("region info is null for row " + key.getRow() +
              " in table " + HConstants.ROOT_TABLE_NAME);
          continue;
        }
        if (!listener.processRow(info)) {
          break;
        }
        results.clear();
      }
    } finally {
      rootScanner.close();
    }
  }

  /**
   * Scans a meta region. For every region found, calls the listener with
   * the HRegionInfo of the region.
   * TODO: Use Visitor rather than Listener pattern.  Allow multiple Visitors.
   * Use this everywhere we scan meta regions: e.g. in metascanners, in close
   * handling, etc.  Have it pass in the whole row, not just HRegionInfo.
   * 
   * @param metaRegionInfo HRegionInfo for meta region
   * @param listener method to be called for each meta region found
   * @throws IOException
   */
  public void scanMetaRegion(HRegionInfo metaRegionInfo,
    ScannerListener listener)
  throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    // Open meta region so we can scan it
    HRegion metaRegion = openMetaRegion(metaRegionInfo);
    scanMetaRegion(metaRegion, listener);
  }

  /**
   * Scan the passed in metaregion <code>m</code> invoking the passed
   * <code>listener</code> per row found.
   * @param m
   * @param listener
   * @throws IOException
   */
  public void scanMetaRegion(final HRegion m, final ScannerListener listener)
  throws IOException {
    InternalScanner metaScanner = m.getScanner(HConstants.COL_REGIONINFO_ARRAY,
      HConstants.EMPTY_START_ROW, HConstants.LATEST_TIMESTAMP, null);
    try {
      HStoreKey key = new HStoreKey();
      SortedMap<byte[], byte[]> results =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      while (metaScanner.next(key, results)) {
        HRegionInfo info =
          Writables.getHRegionInfoOrNull(results.get(HConstants.COL_REGIONINFO));
        if (info == null) {
          LOG.warn("regioninfo null for row " + key.getRow() + " in table " +
            Bytes.toString(m.getTableDesc().getName()));
          continue;
        }
        if (!listener.processRow(info)) {
          break;
        }
        results.clear();
      }
    } finally {
      metaScanner.close();
    }
  }

  private void openRootRegion() throws IOException {
    this.rootRegion = HRegion.openHRegion(HRegionInfo.ROOT_REGIONINFO,
        this.rootdir, this.log, this.conf);
    this.rootRegion.compactStores();
  }

  private HRegion openMetaRegion(HRegionInfo metaInfo) throws IOException {
    HRegion meta =
      HRegion.openHRegion(metaInfo, this.rootdir, this.log, this.conf);
    meta.compactStores();
    return meta;
  }
 
  /**
   * Set a single region on/offline.
   * This is a tool to repair tables that have offlined tables in their midst.
   * Can happen on occasion.  Use at your own risk.  Call from a bit of java
   * or jython script.  This method is 'expensive' in that it creates a
   * {@link HTable} instance per invocation to go against <code>.META.</code>
   * @param c A configuration that has its <code>hbase.master</code>
   * properly set.
   * @param row Row in the catalog .META. table whose HRegionInfo's offline
   * status we want to change.
   * @param onlineOffline Pass <code>true</code> to online the region.
   * @throws IOException
   */
  public static void changeOnlineStatus (final HBaseConfiguration c,
      final byte [] row, final boolean onlineOffline)
  throws IOException {
    HTable t = new HTable(c, HConstants.META_TABLE_NAME);
    Cell cell = t.get(row, HConstants.COL_REGIONINFO);
    if (cell == null) {
      throw new IOException("no information for row " + row);
    }
    // Throws exception if null.
    HRegionInfo info = Writables.getHRegionInfo(cell);
    BatchUpdate b = new BatchUpdate(row);
    info.setOffline(onlineOffline);
    b.put(HConstants.COL_REGIONINFO, Writables.getBytes(info));
    b.delete(HConstants.COL_SERVER);
    b.delete(HConstants.COL_STARTCODE);
    t.commit(b);
  }
  
  /**
   * Offline version of the online TableOperation,
   * org.apache.hadoop.hbase.master.AddColumn.
   * @param tableName
   * @param hcd Add this column to <code>tableName</code>
   * @throws IOException 
   */
  public void addColumn(final byte [] tableName,
      final HColumnDescriptor hcd)
  throws IOException {
    List<HRegionInfo> metas = getMETARowsInROOT();
    for (HRegionInfo hri: metas) {
      final HRegion m = getMetaRegion(hri);
      scanMetaRegion(m, new ScannerListener() {
        private boolean inTable = true;
        
        @SuppressWarnings("synthetic-access")
        public boolean processRow(HRegionInfo info) throws IOException {
          LOG.debug("Testing " + Bytes.toString(tableName) + " against " +
            Bytes.toString(info.getTableDesc().getName()));
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            this.inTable = false;
            info.getTableDesc().addFamily(hcd);
            updateMETARegionInfo(m, info);
            return true;
          }
          // If we got here and we have not yet encountered the table yet,
          // inTable will be false.  Otherwise, we've passed out the table.
          // Stop the scanner.
          return this.inTable;
        }});
    }
  }
  
  /**
   * Offline version of the online TableOperation,
   * org.apache.hadoop.hbase.master.DeleteColumn.
   * @param tableName
   * @param columnFamily Name of column name to remove.
   * @throws IOException
   */
  public void deleteColumn(final byte [] tableName,
      final byte [] columnFamily) throws IOException {
    List<HRegionInfo> metas = getMETARowsInROOT();
    final Path tabledir = new Path(rootdir, Bytes.toString(tableName));
    for (HRegionInfo hri: metas) {
      final HRegion m = getMetaRegion(hri);
      scanMetaRegion(m, new ScannerListener() {
        private boolean inTable = true;
        
        @SuppressWarnings("synthetic-access")
        public boolean processRow(HRegionInfo info) throws IOException {
          if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
            this.inTable = false;
            info.getTableDesc().removeFamily(columnFamily);
            updateMETARegionInfo(m, info);
            FSUtils.deleteColumnFamily(fs, tabledir, info.getEncodedName(),
              HStoreKey.getFamily(columnFamily));
            return false;
          }
          // If we got here and we have not yet encountered the table yet,
          // inTable will be false.  Otherwise, we've passed out the table.
          // Stop the scanner.
          return this.inTable;
        }});
    }
  }
  
  private void updateMETARegionInfo(HRegion r, final HRegionInfo hri) 
  throws IOException {
    if (LOG.isDebugEnabled()) {
      HRegionInfo h = Writables.getHRegionInfoOrNull(
        r.get(hri.getRegionName(), HConstants.COL_REGIONINFO).getValue());
      LOG.debug("Old " + Bytes.toString(HConstants.COL_REGIONINFO) +
        " for " + hri.toString() + " in " + r.toString() + " is: " +
        h.toString());
    }
    BatchUpdate b = new BatchUpdate(hri.getRegionName());
    b.put(HConstants.COL_REGIONINFO, Writables.getBytes(hri));
    r.batchUpdate(b);
    if (LOG.isDebugEnabled()) {
      HRegionInfo h = Writables.getHRegionInfoOrNull(
          r.get(hri.getRegionName(), HConstants.COL_REGIONINFO).getValue());
        LOG.debug("New " + Bytes.toString(HConstants.COL_REGIONINFO) +
          " for " + hri.toString() + " in " + r.toString() + " is: " +
          h.toString());
    }
  }

  /**
   * @return List of <code>.META.<code> {@link HRegionInfo} found in the
   * <code>-ROOT-</code> table.
   * @throws IOException
   * @see #getMetaRegion(HRegionInfo)
   */
  public List<HRegionInfo> getMETARowsInROOT() throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Must call initialize method first.");
    }
    final List<HRegionInfo> result = new ArrayList<HRegionInfo>();
    scanRootRegion(new ScannerListener() {
      @SuppressWarnings("unused")
      public boolean processRow(HRegionInfo info) throws IOException {
        if (Bytes.equals(info.getTableDesc().getName(),
            HConstants.META_TABLE_NAME)) {
          result.add(info);
          return false;
        }
        return true;
      }});
    return result;
  }
}