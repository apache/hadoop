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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.io.BatchUpdate;

/** 
 * A non-instantiable class that has a static method capable of compacting
 * a table by merging adjacent regions that have grown too small.
 */
class HMerge implements HConstants {
  static final Log LOG = LogFactory.getLog(HMerge.class);
  static final Random rand = new Random();
  
  private HMerge() {
    // Not instantiable
  }
  
  /**
   * Scans the table and merges two adjacent regions if they are small. This
   * only happens when a lot of rows are deleted.
   * 
   * When merging the META region, the HBase instance must be offline.
   * When merging a normal table, the HBase instance must be online, but the
   * table must be disabled. 
   * 
   * @param conf        - configuration object for HBase
   * @param fs          - FileSystem where regions reside
   * @param tableName   - Table to be compacted
   * @throws IOException
   */
  public static void merge(HBaseConfiguration conf, FileSystem fs,
      Text tableName) throws IOException {
    
    HConnection connection = HConnectionManager.getConnection(conf);
    boolean masterIsRunning = connection.isMasterRunning();
    HConnectionManager.deleteConnection(conf);
    if(tableName.equals(META_TABLE_NAME)) {
      if(masterIsRunning) {
        throw new IllegalStateException(
            "Can not compact META table if instance is on-line");
      }
      new OfflineMerger(conf, fs).process();

    } else {
      if(!masterIsRunning) {
        throw new IllegalStateException(
            "HBase instance must be running to merge a normal table");
      }
      new OnlineMerger(conf, fs, tableName).process();
    }
  }

  private static abstract class Merger {
    protected final HBaseConfiguration conf;
    protected final FileSystem fs;
    protected final Path tabledir;
    protected final HLog hlog;
    private final long maxFilesize;

    
    protected Merger(HBaseConfiguration conf, FileSystem fs, Text tableName)
        throws IOException {
      
      this.conf = conf;
      this.fs = fs;
      this.maxFilesize =
        conf.getLong("hbase.hregion.max.filesize", DEFAULT_MAX_FILE_SIZE);

      this.tabledir = new Path(
          fs.makeQualified(new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR))),
          tableName.toString()
      );
      Path logdir = new Path(tabledir, "merge_" + System.currentTimeMillis() +
          HREGION_LOGDIR_NAME);
      this.hlog =
        new HLog(fs, logdir, conf, null);
    }
    
    void process() throws IOException {
      try {
        for(HRegionInfo[] regionsToMerge = next();
        regionsToMerge != null;
        regionsToMerge = next()) {

          if (!merge(regionsToMerge)) {
            return;
          }
        }
      } finally {
        try {
          hlog.closeAndDelete();
          
        } catch(IOException e) {
          LOG.error(e);
        }
      }
    }
    
    private boolean merge(HRegionInfo[] info) throws IOException {
      if(info.length < 2) {
        LOG.info("only one region - nothing to merge");
        return false;
      }
      
      HRegion currentRegion = null;
      long currentSize = 0;
      HRegion nextRegion = null;
      long nextSize = 0;
      Text midKey = new Text();
      for (int i = 0; i < info.length - 1; i++) {
        if (currentRegion == null) {
          currentRegion =
            new HRegion(tabledir, hlog, fs, conf, info[i], null, null);
          currentSize = currentRegion.largestHStore(midKey).getAggregate();
        }
        nextRegion =
          new HRegion(tabledir, hlog, fs, conf, info[i + 1], null, null);

        nextSize = nextRegion.largestHStore(midKey).getAggregate();

        if ((currentSize + nextSize) <= (maxFilesize / 2)) {
          // We merge two adjacent regions if their total size is less than
          // one half of the desired maximum size

          LOG.info("merging regions " + currentRegion.getRegionName()
              + " and " + nextRegion.getRegionName());

          HRegion mergedRegion = HRegion.closeAndMerge(currentRegion, nextRegion);

          updateMeta(currentRegion.getRegionName(), nextRegion.getRegionName(),
              mergedRegion);

          break;
        }
        LOG.info("not merging regions " + currentRegion.getRegionName()
            + " and " + nextRegion.getRegionName());

        currentRegion.close();
        currentRegion = nextRegion;
        currentSize = nextSize;
      }
      if(currentRegion != null) {
        currentRegion.close();
      }
      return true;
    }
    
    protected abstract HRegionInfo[] next() throws IOException;
    
    protected abstract void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException;
    
  }

  /** Instantiated to compact a normal user table */
  private static class OnlineMerger extends Merger {
    private final Text tableName;
    private final HTable table;
    private final HScannerInterface metaScanner;
    private HRegionInfo latestRegion;
    
    OnlineMerger(HBaseConfiguration conf, FileSystem fs, Text tableName)
    throws IOException {
      
      super(conf, fs, tableName);
      this.tableName = tableName;
      this.table = new HTable(conf, META_TABLE_NAME);
      this.metaScanner = table.obtainScanner(COL_REGIONINFO_ARRAY, tableName);
      this.latestRegion = null;
    }
    
    private HRegionInfo nextRegion() throws IOException {
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        if (! metaScanner.next(key, results)) {
          return null;
        }
        byte[] bytes = results.get(COL_REGIONINFO);
        if (bytes == null || bytes.length == 0) {
          throw new NoSuchElementException("meta region entry missing "
              + COL_REGIONINFO);
        }
        HRegionInfo region =
          (HRegionInfo) Writables.getWritable(bytes, new HRegionInfo());

        if (!region.getTableDesc().getName().equals(tableName)) {
          return null;
        }
        
        if (!region.isOffline()) {
          throw new TableNotDisabledException("region " + region.getRegionName()
              + " is not disabled");
        }
        return region;
        
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        LOG.error("meta scanner error", e);
        try {
          metaScanner.close();
          
        } catch (IOException ex) {
          ex = RemoteExceptionHandler.checkIOException(ex);
          LOG.error("error closing scanner", ex);
        }
        throw e;
      }
    }

    @Override
    protected HRegionInfo[] next() throws IOException {
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      if(latestRegion == null) {
        latestRegion = nextRegion();
      }
      if(latestRegion != null) {
        regions.add(latestRegion);
      }
      latestRegion = nextRegion();
      if(latestRegion != null) {
        regions.add(latestRegion);
      }
      return regions.toArray(new HRegionInfo[regions.size()]);
    }

    @Override
    protected void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException {
      Text[] regionsToDelete = {
          oldRegion1,
          oldRegion2
      };
      for(int r = 0; r < regionsToDelete.length; r++) {
        if(regionsToDelete[r].equals(latestRegion.getRegionName())) {
          latestRegion = null;
        }
        long lockid = -1L;
        try {
          lockid = table.startUpdate(regionsToDelete[r]);
          table.delete(lockid, COL_REGIONINFO);
          table.delete(lockid, COL_SERVER);
          table.delete(lockid, COL_STARTCODE);
          table.delete(lockid, COL_SPLITA);
          table.delete(lockid, COL_SPLITB);
          table.commit(lockid);
          lockid = -1L;

          if(LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + regionsToDelete[r]);
          }
        } finally {
          if(lockid != -1L) {
            table.abort(lockid);
          }
        }
      }
      newRegion.getRegionInfo().setOffline(true);
      long lockid = -1L;
      try {
        lockid = table.startUpdate(newRegion.getRegionName());
        table.put(lockid, COL_REGIONINFO,
            Writables.getBytes(newRegion.getRegionInfo()));
        table.commit(lockid);
        lockid = -1L;

        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: "
              + newRegion.getRegionName());
        }
      } finally {
        if(lockid != -1L) {
          table.abort(lockid);
        }
      }
    }
  }

  /** Instantiated to compact the meta region */
  private static class OfflineMerger extends Merger {
    private final List<HRegionInfo> metaRegions = new ArrayList<HRegionInfo>();
    private final HRegion root;
    
    OfflineMerger(HBaseConfiguration conf, FileSystem fs)
        throws IOException {
      
      super(conf, fs, META_TABLE_NAME);

      Path rootTableDir = HTableDescriptor.getTableDir(
          fs.makeQualified(new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR))),
          ROOT_TABLE_NAME);

      // Scan root region to find all the meta regions
      
      root = new HRegion(rootTableDir, hlog, fs, conf,
          HRegionInfo.rootRegionInfo, null, null);

      HScannerInterface rootScanner = root.getScanner(COL_REGIONINFO_ARRAY,
          new Text(), System.currentTimeMillis(), null);
      
      try {
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while(rootScanner.next(key, results)) {
          for(byte [] b: results.values()) {
            HRegionInfo info = Writables.getHRegionInfoOrNull(b);
            if (info != null) {
              metaRegions.add(info);
            }
          }
        }
      } finally {
        rootScanner.close();
        try {
          root.close();
          
        } catch(IOException e) {
          LOG.error(e);
        }
      }
    }

    @Override
    protected HRegionInfo[] next() {
      HRegionInfo[] results = null;
      if (metaRegions.size() > 0) {
        results = metaRegions.toArray(new HRegionInfo[metaRegions.size()]);
        metaRegions.clear();
      }
      return results;
    }

    @Override
    protected void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException {
      
      Text[] regionsToDelete = {
          oldRegion1,
          oldRegion2
      };
      for(int r = 0; r < regionsToDelete.length; r++) {
        long lockid = Math.abs(rand.nextLong());
        BatchUpdate b = new BatchUpdate(lockid);
        lockid = b.startUpdate(regionsToDelete[r]);
        b.delete(lockid, COL_REGIONINFO);
        b.delete(lockid, COL_SERVER);
        b.delete(lockid, COL_STARTCODE);
        b.delete(lockid, COL_SPLITA);
        b.delete(lockid, COL_SPLITB);
        root.batchUpdate(System.currentTimeMillis(), b);
        lockid = -1L;

        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + regionsToDelete[r]);
        }
      }
      HRegionInfo newInfo = newRegion.getRegionInfo();
      newInfo.setOffline(true);
      long lockid = Math.abs(rand.nextLong());
      BatchUpdate b = new BatchUpdate(lockid);
      lockid = b.startUpdate(newRegion.getRegionName());
      b.put(lockid, COL_REGIONINFO, Writables.getBytes(newInfo));
      root.batchUpdate(System.currentTimeMillis(), b);
      if(LOG.isDebugEnabled()) {
        LOG.debug("updated columns in row: " + newRegion.getRegionName());
      }
    }
  }
}
