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

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Perform a file system upgrade to convert older file layouts.
 * HBase keeps a file in hdfs named hbase.version just under the hbase.rootdir.
 * This file holds the version of the hbase data in the Filesystem.  When the
 * software changes in a manner incompatible with the data in the Filesystem,
 * it updates its internal version number,
 * {@link HConstants#FILE_SYSTEM_VERSION}.  This wrapper script manages moving
 * the filesystem across versions until there's a match with current software's
 * version number.
 * 
 * <p>This wrapper script comprises a set of migration steps.  Which steps
 * are run depends on the span between the version of the hbase data in the
 * Filesystem and the version of the current softare.
 * 
 * <p>A migration script must accompany any patch that changes data formats.
 * 
 * <p>This script has a 'check' and 'execute' mode.  Adding migration steps,
 * its important to keep this in mind.  Testing if migration needs to be run,
 * be careful not to make presumptions about the current state of the data in
 * the filesystem.  It may have a format from many versions previous with
 * layout not as expected or keys and values of a format not expected.  Tools
 * such as {@link MetaUtils} may not work as expected when running against
 * old formats -- or, worse, fail in ways that are hard to figure (One such is
 * edits made by previous migration steps not being apparent in later migration
 * steps).  The upshot is always verify presumptions migrating.
 * 
 * <p>This script will migrate an hbase 0.1 install to a 0.2 install only.
 * 
 * @see <a href="http://wiki.apache.org/hadoop/Hbase/HowToMigrate">How To Migration</a>
 */
public class Migrate extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Migrate.class);

  private final HBaseConfiguration conf;
  private FileSystem fs;
  
  // Gets set by migration methods if we are in readOnly mode.
  boolean migrationNeeded = false;

  boolean readOnly = false;

  // Filesystem version of hbase 0.1.x.
  private static final float HBASE_0_1_VERSION = 0.1f;
  
  /** default constructor */
  public Migrate() {
    this(new HBaseConfiguration());
  }
  
  /**
   * @param conf
   */
  public Migrate(HBaseConfiguration conf) {
    super(conf);
    this.conf = conf;
  }
  
  /*
   * Sets the hbase rootdir as fs.default.name.
   * @return True if succeeded.
   */
  private boolean setFsDefaultName() {
    // Validate root directory path
    Path rd = new Path(conf.get(HConstants.HBASE_DIR));
    try {
      // Validate root directory path
      FSUtils.validateRootPath(rd);
    } catch (IOException e) {
      LOG.fatal("Not starting migration because the root directory path '" +
          rd.toString() + "' is not valid. Check the setting of the" +
          " configuration parameter '" + HConstants.HBASE_DIR + "'", e);
      return false;
    }
    this.conf.set("fs.default.name", rd.toString());
    return true;
  }

  /*
   * @return True if succeeded verifying filesystem.
   */
  private boolean verifyFilesystem() {
    try {
      // Verify file system is up.
      fs = FileSystem.get(conf);                        // get DFS handle
      LOG.info("Verifying that file system is available...");
      FSUtils.checkFileSystemAvailable(fs);
      return true;
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return false;
    }
  }
  
  private boolean notRunning() {
    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...");
    try {
      HBaseAdmin.checkHBaseAvailable(conf);
      LOG.fatal("HBase cluster must be off-line.");
      return false;
    } catch (MasterNotRunningException e) {
      return true;
    }
  }
  
  /** {@inheritDoc} */
  public int run(String[] args) {
    if (parseArgs(args) != 0) {
      return -1;
    }
    if (!setFsDefaultName()) {
      return -2;
    }
    if (!verifyFilesystem()) {
      return -3;
    }
    if (!notRunning()) {
      return -4;
    }

    try {
      LOG.info("Starting upgrade" + (readOnly ? " check" : ""));

      // See if there is a file system version file
      String versionStr = FSUtils.getVersion(fs, FSUtils.getRootDir(this.conf));
      if (versionStr != null &&
          versionStr.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) {
        LOG.info("No upgrade necessary.");
        return 0;
      }
      if (versionStr == null ||
          Float.parseFloat(versionStr) < HBASE_0_1_VERSION) {
        throw new IOException("Install 0.1.x of hbase and run its " +
          "migration first");
      }
      float version = Float.parseFloat(versionStr);
      if (version == HBASE_0_1_VERSION) {
        checkForUnrecoveredLogFiles(getRootDirFiles());
        migrateToV5();
      } else {
        throw new IOException("Unrecognized or non-migratable version: " +
          version);
      }

      if (!readOnly) {
        // Set file system version
        LOG.info("Setting file system version.");
        FSUtils.setVersion(fs, FSUtils.getRootDir(this.conf));
        LOG.info("Upgrade successful.");
      } else if (this.migrationNeeded) {
        LOG.info("Upgrade needed.");
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Upgrade" +  (readOnly ? " check" : "") + " failed", e);
      return -1;
    }
  }
  
  private void migrateToV5() throws IOException {
    rewriteMetaHRegionInfo();
    addHistorianFamilyToMeta();
    updateBloomFilters();
  }
  
  /**
   * Rewrite the meta tables so that HRI is versioned and so we move to new
   * HCD and HCD.
   * @throws IOException 
   */
  private void rewriteMetaHRegionInfo() throws IOException {
    if (this.readOnly && this.migrationNeeded) {
      return;
    }
    // Read using old classes.
    final org.apache.hadoop.hbase.util.migration.v5.MetaUtils utils =
      new org.apache.hadoop.hbase.util.migration.v5.MetaUtils(this.conf);
    try {
      // Scan the root region
      utils.scanRootRegion(new org.apache.hadoop.hbase.util.migration.v5.MetaUtils.ScannerListener() {
        public boolean processRow(org.apache.hadoop.hbase.util.migration.v5.HRegionInfo info)
        throws IOException {
          // Scan every meta region
          final org.apache.hadoop.hbase.util.migration.v5.HRegion metaRegion =
            utils.getMetaRegion(info);
          // If here, we were able to read with old classes.  If readOnly, then
          // needs migration.
          if (readOnly && !migrationNeeded) {
            migrationNeeded = true;
            return false;
          }
          updateHRegionInfo(utils.getRootRegion(), info);
          utils.scanMetaRegion(info, new org.apache.hadoop.hbase.util.migration.v5.MetaUtils.ScannerListener() {
            public boolean processRow(org.apache.hadoop.hbase.util.migration.v5.HRegionInfo hri)
            throws IOException {
              updateHRegionInfo(metaRegion, hri);
              return true;
            }
          });
          return true;
        }
      });
    } finally {
      utils.shutdown();
    }
  }
  
  /*
   * Move from old pre-v5 hregioninfo to current HRegionInfo
   * Persist back into <code>r</code>
   * @param mr
   * @param oldHri
   */
  void updateHRegionInfo(org.apache.hadoop.hbase.util.migration.v5.HRegion mr,
    org.apache.hadoop.hbase.util.migration.v5.HRegionInfo oldHri)
  throws IOException {
    byte [] oldHriTableName = oldHri.getTableDesc().getName();
    HTableDescriptor newHtd =
      Bytes.equals(HConstants.ROOT_TABLE_NAME, oldHriTableName)?
        HTableDescriptor.ROOT_TABLEDESC:
        Bytes.equals(HConstants.META_TABLE_NAME, oldHriTableName)?
          HTableDescriptor.META_TABLEDESC:
          new HTableDescriptor(oldHri.getTableDesc().getName());
    for (org.apache.hadoop.hbase.util.migration.v5.HColumnDescriptor oldHcd:
        oldHri.getTableDesc().getFamilies()) {
      HColumnDescriptor newHcd = new HColumnDescriptor(
        HStoreKey.addDelimiter(oldHcd.getName()),
        oldHcd.getMaxValueLength(),
        HColumnDescriptor.CompressionType.valueOf(oldHcd.getCompressionType().toString()),
        oldHcd.isInMemory(), oldHcd.isBlockCacheEnabled(),
        oldHcd.getMaxValueLength(), oldHcd.getTimeToLive(),
        oldHcd.isBloomFilterEnabled());
      newHtd.addFamily(newHcd);
    }
    HRegionInfo newHri = new HRegionInfo(newHtd, oldHri.getStartKey(),
      oldHri.getEndKey(), oldHri.isSplit(), oldHri.getRegionId());
    BatchUpdate b = new BatchUpdate(newHri.getRegionName());
    b.put(HConstants.COL_REGIONINFO, Writables.getBytes(newHri));
    mr.batchUpdate(b);
    if (LOG.isDebugEnabled()) {
        LOG.debug("New " + Bytes.toString(HConstants.COL_REGIONINFO) +
          " for " + oldHri.toString() + " in " + mr.toString() + " is: " +
          newHri.toString());
    }
  }

  private FileStatus[] getRootDirFiles() throws IOException {
    FileStatus[] stats = fs.listStatus(FSUtils.getRootDir(this.conf));
    if (stats == null || stats.length == 0) {
      throw new IOException("No files found under root directory " +
        FSUtils.getRootDir(this.conf).toString());
    }
    return stats;
  }

  private void checkForUnrecoveredLogFiles(FileStatus[] rootFiles)
  throws IOException {
    List<String> unrecoveredLogs = new ArrayList<String>();
    for (int i = 0; i < rootFiles.length; i++) {
      String name = rootFiles[i].getPath().getName();
      if (name.startsWith("log_")) {
        unrecoveredLogs.add(name);
      }
    }
    if (unrecoveredLogs.size() != 0) {
      throw new IOException("There are " + unrecoveredLogs.size() +
          " unrecovered region server logs. Please uninstall this version of " +
          "HBase, re-install the previous version, start your cluster and " +
          "shut it down cleanly, so that all region server logs are recovered" +
          " and deleted.  Or, if you are sure logs are vestige of old " +
          "failures in hbase, remove them and then rerun the migration.  " +
          "See 'Redo Logs' in http://wiki.apache.org/hadoop/Hbase/HowToMigrate. " + 
          "Here are the problem log files: " + unrecoveredLogs);
    }
  }

  private void addHistorianFamilyToMeta() throws IOException {
    if (this.migrationNeeded) {
      // Be careful. We cannot use MetAutils if current hbase in the
      // Filesystem has not been migrated.
      return;
    }
    boolean needed = false;
    MetaUtils utils = new MetaUtils(this.conf);
    try {
      List<HRegionInfo> metas = utils.getMETARows(HConstants.META_TABLE_NAME);
      for (HRegionInfo meta : metas) {
        if (meta.getTableDesc().
            getFamily(HConstants.COLUMN_FAMILY_HISTORIAN) == null) {
          needed = true;
          break;
        }
      }
      if (needed && this.readOnly) {
        this.migrationNeeded = true;
      } else {
        utils.addColumn(HConstants.META_TABLE_NAME,
          new HColumnDescriptor(HConstants.COLUMN_FAMILY_HISTORIAN,
            HConstants.ALL_VERSIONS, HColumnDescriptor.CompressionType.NONE,
            false, false, Integer.MAX_VALUE, HConstants.FOREVER, false));
        LOG.info("Historian family added to .META.");
        // Flush out the meta edits.
      }
    } finally {
      utils.shutdown();
    }
  }
  
  private void updateBloomFilters() throws IOException {
    if (this.migrationNeeded && this.readOnly) {
      return;
    }
    final Path rootDir = FSUtils.getRootDir(conf);
    final MetaUtils utils = new MetaUtils(this.conf);
    try {
      // Scan the root region
      utils.scanRootRegion(new MetaUtils.ScannerListener() {
        public boolean processRow(HRegionInfo info) throws IOException {
          // Scan every meta region
          final HRegion metaRegion = utils.getMetaRegion(info);
          utils.scanMetaRegion(info, new MetaUtils.ScannerListener() {
            public boolean processRow(HRegionInfo hri) throws IOException {
              HTableDescriptor desc = hri.getTableDesc();
              Path tableDir =
                HTableDescriptor.getTableDir(rootDir, desc.getName()); 
              for (HColumnDescriptor column: desc.getFamilies()) {
                if (column.isBloomfilter()) {
                  // Column has a bloom filter
                  migrationNeeded = true;
                  Path filterDir = HStoreFile.getFilterDir(tableDir,
                      hri.getEncodedName(), column.getName());
                  if (fs.exists(filterDir)) {
                    // Filter dir exists
                    if (readOnly) {
                      // And if we are only checking to see if a migration is
                      // needed - it is. We're done.
                      return false;
                    }
                    // Delete the filter
                    fs.delete(filterDir, true);
                    // Update the HRegionInfo in meta setting the bloomfilter
                    // to be disabled.
                    column.setBloomfilter(false);
                    utils.updateMETARegionInfo(metaRegion, hri);
                  }
                }
              }
              return true;
            }
          });
          // Stop scanning if only doing a check and we've determined that a
          // migration is needed. Otherwise continue by returning true
          return readOnly && migrationNeeded ? false : true;
        }
      });
    } finally {
      utils.shutdown();
    }
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options opts = new Options();
    GenericOptionsParser parser =
      new GenericOptionsParser(this.getConf(), opts, args);
    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 1) {
      usage();
      return -1;
    }
    if (remainingArgs[0].compareTo("check") == 0) {
      this.readOnly = true;
    } else if (remainingArgs[0].compareTo("upgrade") != 0) {
      usage();
      return -1;
    }
    return 0;
  }
  
  private void usage() {
    System.err.println("Usage: bin/hbase migrate { check | upgrade } [options]\n");
    System.err.println("  check                            perform upgrade checks only.");
    System.err.println("  upgrade                          perform upgrade checks and modify hbase.\n");
    System.err.println("  Options are:");
    System.err.println("    -conf <configuration file>     specify an application configuration file");
    System.err.println("    -D <property=value>            use value for given property");
    System.err.println("    -fs <local|namenode:port>      specify a namenode");
  }

  /**
   * Main program
   * 
   * @param args command line arguments
   */
  public static void main(String[] args) {
    int status = 0;
    try {
      status = ToolRunner.run(new Migrate(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
