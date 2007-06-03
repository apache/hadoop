/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;

public class HMerge implements HConstants {
  private static final Log LOG = LogFactory.getLog(HMerge.class);
  private static final Text[] META_COLS = {COL_REGIONINFO};
  
  private HMerge() {}                           // Not instantiable
  
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
  public static void merge(Configuration conf, FileSystem fs, Text tableName)
      throws IOException {
    
    HClient client = new HClient(conf);
    boolean masterIsRunning = client.isMasterRunning();
    if(tableName.equals(META_TABLE_NAME)) {
        if(masterIsRunning) {
          throw new IllegalStateException(
              "Can not compact META table if instance is on-line");
        }
        new OfflineMerger(conf, fs, META_TABLE_NAME).process();
      
    } else {
      if(!masterIsRunning) {
        throw new IllegalStateException(
            "HBase instance must be running to merge a normal table");
      }
      new OnlineMerger(conf, fs, client, tableName).process();
    }
  }

  private static abstract class Merger {
    protected Configuration conf;
    protected FileSystem fs;
    protected Text tableName;
    protected Path dir;
    protected Path basedir;
    protected HLog hlog;
    protected DataInputBuffer in;
    protected boolean more;
    protected HStoreKey key;
    protected HRegionInfo info;
    
    protected Merger(Configuration conf, FileSystem fs, Text tableName)
        throws IOException {
      
      this.conf = conf;
      this.fs = fs;
      this.tableName = tableName;
      this.in = new DataInputBuffer();
      this.more = true;
      this.key = new HStoreKey();
      this.info = new HRegionInfo();
      this.dir = new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR));
      this.basedir = new Path(dir, "merge_" + System.currentTimeMillis());
      fs.mkdirs(basedir);
      this.hlog = new HLog(fs, new Path(basedir, HREGION_LOGDIR_NAME), conf);
    }
    
    public void process() throws IOException {
      try {
        while(more) {
          TreeSet<HRegionInfo> regionsToMerge = next();
          if(regionsToMerge == null) {
            break;
          }
          merge(regionsToMerge.toArray(new HRegionInfo[regionsToMerge.size()]));
        }
      } finally {
        try {
          hlog.close();
          
        } catch(IOException e) {
          LOG.error(e);
        }
        try {
          fs.delete(basedir);
          
        } catch(IOException e) {
          LOG.error(e);
        }
      }
    }
    
    private void merge(HRegionInfo[] regions) throws IOException {
      if(regions.length < 2) {
        LOG.info("only one region - nothing to merge");
        return;
      }
      
      HRegion currentRegion = null;
      long currentSize = 0;
      HRegion nextRegion = null;
      long nextSize = 0;
      for(int i = 0; i < regions.length - 1; i++) {
        if(currentRegion == null) {
          currentRegion =
            new HRegion(dir, hlog, fs, conf, regions[i], null, null);

          currentSize = currentRegion.largestHStore();
        }
        nextRegion =
          new HRegion(dir, hlog, fs, conf, regions[i + 1], null, null);

        nextSize = nextRegion.largestHStore();

        if((currentSize + nextSize) <= (DESIRED_MAX_FILE_SIZE / 2)) {
          // We merge two adjacent regions if their total size is less than
          // one half of the desired maximum size

          LOG.info("merging regions " + currentRegion.getRegionName()
              + " and " + nextRegion.getRegionName());

          HRegion mergedRegion = HRegion.closeAndMerge(currentRegion, nextRegion);

          updateMeta(currentRegion.getRegionName(), nextRegion.getRegionName(),
              mergedRegion);

          currentRegion = null;
          i++;
          continue;
          
        } else {
          LOG.info("not merging regions " + currentRegion.getRegionName()
              + " and " + nextRegion.getRegionName());
        }

        currentRegion.close();
        currentRegion = nextRegion;
        currentSize = nextSize;
      }
      if(currentRegion != null) {
        currentRegion.close();
      }
    }
    
    protected abstract TreeSet<HRegionInfo> next() throws IOException;
    
    protected abstract void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException;
    
  }
  
  private static class OnlineMerger extends Merger {
    private HClient client;
    private HScannerInterface metaScanner;
    private HRegionInfo latestRegion;
    
    public OnlineMerger(Configuration conf, FileSystem fs, HClient client,
        Text tableName) throws IOException {
      
      super(conf, fs, tableName);
      this.client = client;
      client.openTable(META_TABLE_NAME);
      this.metaScanner = client.obtainScanner(META_COLS, new Text());
      this.latestRegion = null;
    }
    
    private HRegionInfo nextRegion() throws IOException {
      try {
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        if(! metaScanner.next(key, results)) {
          more = false;
          return null;
        }
        byte[] bytes = results.get(COL_REGIONINFO);
        if(bytes == null || bytes.length == 0) {
          throw new NoSuchElementException("meta region entry missing "
              + COL_REGIONINFO);
        }
        HRegionInfo region = new HRegionInfo(bytes);
        if(!region.offLine) {
          throw new TableNotDisabledException("region " + region.regionName
              + " is not disabled");
        }
        return region;
        
      } catch(IOException e) {
        try {
          metaScanner.close();
          
        } catch(IOException ex) {
          LOG.error(ex);
        }
        more = false;
        throw e;
      }
    }

    protected TreeSet<HRegionInfo> next() throws IOException {
      TreeSet<HRegionInfo> regions = new TreeSet<HRegionInfo>();
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
      return regions;
    }
    
    protected void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException {
      Text[] regionsToDelete = {
          oldRegion1,
          oldRegion2
      };
      for(int r = 0; r < regionsToDelete.length; r++) {
        if(regionsToDelete[r].equals(latestRegion.regionName)) {
          latestRegion = null;
        }
        long lockid = -1L;
        try {
          lockid = client.startUpdate(regionsToDelete[r]);
          client.delete(lockid, COL_REGIONINFO);
          client.delete(lockid, COL_SERVER);
          client.delete(lockid, COL_STARTCODE);
          client.commit(lockid);
          lockid = -1L;

          if(LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + regionsToDelete[r]);
          }
        } finally {
          try {
            if(lockid != -1L) {
              client.abort(lockid);
            }

          } catch(IOException iex) {
            LOG.error(iex);
          }
        }
      }
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      newRegion.getRegionInfo().offLine = true;
      newRegion.getRegionInfo().write(s);
      long lockid = -1L;
      try {
        lockid = client.startUpdate(newRegion.getRegionName());
        client.put(lockid, COL_REGIONINFO, byteValue.toByteArray());
        client.commit(lockid);
        lockid = -1L;

        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: "
              + newRegion.getRegionName());
        }
      } finally {
        try {
          if(lockid != -1L) {
            client.abort(lockid);
          }

        } catch(IOException iex) {
          LOG.error(iex);
        }
      }
    }
  }

  private static class OfflineMerger extends Merger {
    private Path dir;
    private TreeSet<HRegionInfo> metaRegions;
    private TreeMap<Text, BytesWritable> results;
    
    public OfflineMerger(Configuration conf, FileSystem fs, Text tableName)
        throws IOException {
      
      super(conf, fs, tableName);
      this.dir = new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR));
      this.metaRegions = new TreeSet<HRegionInfo>();
      this.results = new TreeMap<Text, BytesWritable>();

      // Scan root region to find all the meta regions
      
      HRegion root = new HRegion(dir, hlog,fs, conf, HGlobals.rootRegionInfo,
          null, null);

      HInternalScannerInterface rootScanner =
        root.getScanner(META_COLS, new Text());
      
      try {
        while(rootScanner.next(key, results)) {
          for(BytesWritable b: results.values()) {
            byte[] bytes = new byte[b.getSize()];
            System.arraycopy(b.get(), 0, bytes, 0, bytes.length);
            in.reset(bytes, bytes.length);
            info.readFields(in);
            metaRegions.add(info);
            results.clear();
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

    protected TreeSet<HRegionInfo> next() throws IOException {
      more = false;
      return metaRegions;
    }
    
    protected void updateMeta(Text oldRegion1, Text oldRegion2,
        HRegion newRegion) throws IOException {
      
      HRegion root =
        new HRegion(dir, hlog, fs, conf, HGlobals.rootRegionInfo, null, null);

      Text[] regionsToDelete = {
          oldRegion1,
          oldRegion2
      };
      for(int r = 0; r < regionsToDelete.length; r++) {
        long lockid = -1L;
        try {
          lockid = root.startUpdate(regionsToDelete[r]);
          root.delete(lockid, COL_REGIONINFO);
          root.delete(lockid, COL_SERVER);
          root.delete(lockid, COL_STARTCODE);
          root.commit(lockid);
          lockid = -1L;

          if(LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + regionsToDelete[r]);
          }
        } finally {
          try {
            if(lockid != -1L) {
              root.abort(lockid);
            }

          } catch(IOException iex) {
            LOG.error(iex);
          }
        }
      }
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      newRegion.getRegionInfo().offLine = true;
      newRegion.getRegionInfo().write(s);
      long lockid = -1L;
      try {
        lockid = root.startUpdate(newRegion.getRegionName());
        root.put(lockid, COL_REGIONINFO,
            new BytesWritable(byteValue.toByteArray()));
        root.commit(lockid);
        lockid = -1L;

        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: "
              + newRegion.getRegionName());
        }
      } finally {
        try {
          if(lockid != -1L) {
            root.abort(lockid);
          }

        } catch(IOException iex) {
          LOG.error(iex);
        }
      }
    }
  }
}
