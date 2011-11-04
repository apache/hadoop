/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * Utility that can merge any two regions in the same table: adjacent,
 * overlapping or disjoint.
 */
public class Merge extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(Merge.class);
  private Path rootdir;
  private volatile MetaUtils utils;
  private byte [] tableName;               // Name of table
  private volatile byte [] region1;        // Name of region 1
  private volatile byte [] region2;        // Name of region 2
  private volatile boolean isMetaTable;
  private volatile HRegionInfo mergeInfo;

  /** default constructor */
  public Merge() {
    super();
  }

  /**
   * @param conf configuration
   */
  public Merge(Configuration conf) {
    this.mergeInfo = null;
    setConf(conf);
  }

  public int run(String[] args) throws Exception {
    if (parseArgs(args) != 0) {
      return -1;
    }

    // Verify file system is up.
    FileSystem fs = FileSystem.get(getConf());              // get DFS handle
    LOG.info("Verifying that file system is available...");
    try {
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return -1;
    }

    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...");
    try {
      HBaseAdmin.checkHBaseAvailable(getConf());
      LOG.fatal("HBase cluster must be off-line.");
      return -1;
    } catch (ZooKeeperConnectionException zkce) {
      // If no zk, presume no master.
    } catch (MasterNotRunningException e) {
      // Expected. Ignore.
    }

    // Initialize MetaUtils and and get the root of the HBase installation

    this.utils = new MetaUtils(getConf());
    this.rootdir = FSUtils.getRootDir(getConf());
    try {
      if (isMetaTable) {
        mergeTwoMetaRegions();
      } else {
        mergeTwoRegions();
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Merge failed", e);
      utils.scanMetaRegion(HRegionInfo.FIRST_META_REGIONINFO,
          new MetaUtils.ScannerListener() {
            public boolean processRow(HRegionInfo info) {
              System.err.println(info.toString());
              return true;
            }
          }
      );

      return -1;

    } finally {
      if (this.utils != null) {
        this.utils.shutdown();
      }
    }
  }

  /** @return HRegionInfo for merge result */
  HRegionInfo getMergedHRegionInfo() {
    return this.mergeInfo;
  }

  /*
   * Merge two meta regions. This is unlikely to be needed soon as we have only
   * seend the meta table split once and that was with 64MB regions. With 256MB
   * regions, it will be some time before someone has enough data in HBase to
   * split the meta region and even less likely that a merge of two meta
   * regions will be needed, but it is included for completeness.
   */
  private void mergeTwoMetaRegions() throws IOException {
    HRegion rootRegion = utils.getRootRegion();
    Get get = new Get(region1);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    List<KeyValue> cells1 =  rootRegion.get(get, null).list();
    HRegionInfo info1 = Writables.getHRegionInfo((cells1 == null)? null: cells1.get(0).getValue());

    get = new Get(region2);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    List<KeyValue> cells2 =  rootRegion.get(get, null).list();
    HRegionInfo info2 = Writables.getHRegionInfo((cells2 == null)? null: cells2.get(0).getValue());
    HRegion merged = merge(HTableDescriptor.META_TABLEDESC, info1, rootRegion, info2, rootRegion);
    LOG.info("Adding " + merged.getRegionInfo() + " to " +
        rootRegion.getRegionInfo());
    HRegion.addRegionToMETA(rootRegion, merged);
    merged.close();
  }

  private static class MetaScannerListener
  implements MetaUtils.ScannerListener {
    private final byte [] region1;
    private final byte [] region2;
    private HRegionInfo meta1 = null;
    private HRegionInfo meta2 = null;

    MetaScannerListener(final byte [] region1, final byte [] region2) {
      this.region1 = region1;
      this.region2 = region2;
    }

    public boolean processRow(HRegionInfo info) {
      if (meta1 == null && HRegion.rowIsInRange(info, region1)) {
        meta1 = info;
      }
      if (region2 != null && meta2 == null &&
          HRegion.rowIsInRange(info, region2)) {
        meta2 = info;
      }
      return meta1 == null || (region2 != null && meta2 == null);
    }

    HRegionInfo getMeta1() {
      return meta1;
    }

    HRegionInfo getMeta2() {
      return meta2;
    }
  }

  /*
   * Merges two regions from a user table.
   */
  private void mergeTwoRegions() throws IOException {
    LOG.info("Merging regions " + Bytes.toStringBinary(this.region1) + " and " +
        Bytes.toStringBinary(this.region2) + " in table " + Bytes.toString(this.tableName));
    // Scan the root region for all the meta regions that contain the regions
    // we're merging.
    MetaScannerListener listener = new MetaScannerListener(region1, region2);
    this.utils.scanRootRegion(listener);
    HRegionInfo meta1 = listener.getMeta1();
    if (meta1 == null) {
      throw new IOException("Could not find meta region for " + Bytes.toStringBinary(region1));
    }
    HRegionInfo meta2 = listener.getMeta2();
    if (meta2 == null) {
      throw new IOException("Could not find meta region for " + Bytes.toStringBinary(region2));
    }
    LOG.info("Found meta for region1 " + Bytes.toStringBinary(meta1.getRegionName()) +
      ", meta for region2 " + Bytes.toStringBinary(meta2.getRegionName()));
    HRegion metaRegion1 = this.utils.getMetaRegion(meta1);
    Get get = new Get(region1);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    List<KeyValue> cells1 =  metaRegion1.get(get, null).list();
    HRegionInfo info1 =
      Writables.getHRegionInfo((cells1 == null)? null: cells1.get(0).getValue());
    if (info1 == null) {
      throw new NullPointerException("info1 is null using key " +
          Bytes.toStringBinary(region1) + " in " + meta1);
    }

    HRegion metaRegion2;
    if (Bytes.equals(meta1.getRegionName(), meta2.getRegionName())) {
      metaRegion2 = metaRegion1;
    } else {
      metaRegion2 = utils.getMetaRegion(meta2);
    }
    get = new Get(region2);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    List<KeyValue> cells2 =  metaRegion2.get(get, null).list();
    HRegionInfo info2 = Writables.getHRegionInfo((cells2 == null)? null: cells2.get(0).getValue());
    if (info2 == null) {
      throw new NullPointerException("info2 is null using key " + meta2);
    }
    HTableDescriptor htd = FSTableDescriptors.getTableDescriptor(FileSystem.get(getConf()),
      this.rootdir, this.tableName);
    HRegion merged = merge(htd, info1, metaRegion1, info2, metaRegion2);

    // Now find the meta region which will contain the newly merged region

    listener = new MetaScannerListener(merged.getRegionName(), null);
    utils.scanRootRegion(listener);
    HRegionInfo mergedInfo = listener.getMeta1();
    if (mergedInfo == null) {
      throw new IOException("Could not find meta region for " +
          Bytes.toStringBinary(merged.getRegionName()));
    }
    HRegion mergeMeta;
    if (Bytes.equals(mergedInfo.getRegionName(), meta1.getRegionName())) {
      mergeMeta = metaRegion1;
    } else if (Bytes.equals(mergedInfo.getRegionName(), meta2.getRegionName())) {
      mergeMeta = metaRegion2;
    } else {
      mergeMeta = utils.getMetaRegion(mergedInfo);
    }
    LOG.info("Adding " + merged.getRegionInfo() + " to " +
        mergeMeta.getRegionInfo());

    HRegion.addRegionToMETA(mergeMeta, merged);
    merged.close();
  }

  /*
   * Actually merge two regions and update their info in the meta region(s)
   * If the meta is split, meta1 may be different from meta2. (and we may have
   * to scan the meta if the resulting merged region does not go in either)
   * Returns HRegion object for newly merged region
   */
  private HRegion merge(final HTableDescriptor htd, HRegionInfo info1,
      HRegion meta1, HRegionInfo info2, HRegion meta2)
  throws IOException {
    if (info1 == null) {
      throw new IOException("Could not find " + Bytes.toStringBinary(region1) + " in " +
          Bytes.toStringBinary(meta1.getRegionName()));
    }
    if (info2 == null) {
      throw new IOException("Cound not find " + Bytes.toStringBinary(region2) + " in " +
          Bytes.toStringBinary(meta2.getRegionName()));
    }
    HRegion merged = null;
    HLog log = utils.getLog();
    HRegion r1 = HRegion.openHRegion(info1, htd, log, getConf());
    try {
      HRegion r2 = HRegion.openHRegion(info2, htd, log, getConf());
      try {
        merged = HRegion.merge(r1, r2);
      } finally {
        if (!r2.isClosed()) {
          r2.close();
        }
      }
    } finally {
      if (!r1.isClosed()) {
        r1.close();
      }
    }

    // Remove the old regions from meta.
    // HRegion.merge has already deleted their files

    removeRegionFromMeta(meta1, info1);
    removeRegionFromMeta(meta2, info2);

    this.mergeInfo = merged.getRegionInfo();
    return merged;
  }

  /*
   * Removes a region's meta information from the passed <code>meta</code>
   * region.
   *
   * @param meta META HRegion to be updated
   * @param regioninfo HRegionInfo of region to remove from <code>meta</code>
   *
   * @throws IOException
   */
  private void removeRegionFromMeta(HRegion meta, HRegionInfo regioninfo)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing region: " + regioninfo + " from " + meta);
    }

    Delete delete  = new Delete(regioninfo.getRegionName(),
        System.currentTimeMillis(), null);
    meta.delete(delete, null, true);
  }

  /*
   * Adds a region's meta information from the passed <code>meta</code>
   * region.
   *
   * @param metainfo META HRegionInfo to be updated
   * @param region HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  private int parseArgs(String[] args) throws IOException {
    GenericOptionsParser parser =
      new GenericOptionsParser(getConf(), args);

    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 3) {
      usage();
      return -1;
    }
    tableName = Bytes.toBytes(remainingArgs[0]);
    isMetaTable = Bytes.compareTo(tableName, HConstants.META_TABLE_NAME) == 0;

    region1 = Bytes.toBytesBinary(remainingArgs[1]);
    region2 = Bytes.toBytesBinary(remainingArgs[2]);
    int status = 0;
    if (notInTable(tableName, region1) || notInTable(tableName, region2)) {
      status = -1;
    } else if (Bytes.equals(region1, region2)) {
      LOG.error("Can't merge a region with itself");
      status = -1;
    }
    return status;
  }

  private boolean notInTable(final byte [] tn, final byte [] rn) {
    if (WritableComparator.compareBytes(tn, 0, tn.length, rn, 0, tn.length) != 0) {
      LOG.error("Region " + Bytes.toStringBinary(rn) + " does not belong to table " +
        Bytes.toString(tn));
      return true;
    }
    return false;
  }

  private void usage() {
    System.err.println(
        "Usage: bin/hbase merge <table-name> <region-1> <region-2>\n");
  }

  public static void main(String[] args) {
    int status;
    try {
      status = ToolRunner.run(HBaseConfiguration.create(), new Merge(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
