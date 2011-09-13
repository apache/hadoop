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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import static org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;

/**
 * Check consistency among the in-memory states of the master and the
 * region server(s) and the state of data in HDFS.
 */
public class HBaseFsck {
  public static final long DEFAULT_TIME_LAG = 60000; // default value of 1 minute
  public static final long DEFAULT_SLEEP_BEFORE_RERUN = 10000;

  private static final int MAX_NUM_THREADS = 50; // #threads to contact regions
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;

  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;

  private ClusterStatus status;
  private HConnection connection;

  private TreeMap<String, HbckInfo> regionInfo = new TreeMap<String, HbckInfo>();
  private TreeMap<String, TInfo> tablesInfo = new TreeMap<String, TInfo>();
  private TreeSet<byte[]> disabledTables =
    new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  ErrorReporter errors = new PrintingErrorReporter();

  private static boolean details = false; // do we display the full report
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older
  private boolean fix = false; // do we want to try fixing the errors?
  private boolean rerun = false; // if we tried to fix something rerun hbck
  private static boolean summary = false; // if we want to print less output
  private boolean checkMetaOnly = false;
  
  // Empty regioninfo qualifiers in .META.
  private Set<Result> emptyRegionInfoQualifiers = new HashSet<Result>();
  private int numThreads = MAX_NUM_THREADS;
  private final HBaseAdmin admin;

  ThreadPoolExecutor executor; // threads to retrieve data from regionservers

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HBaseFsck(Configuration conf)
    throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    this.conf = conf;

    admin = new HBaseAdmin(conf);
    status = admin.getMaster().getClusterStatus();
    connection = admin.getConnection();

    numThreads = conf.getInt("hbasefsck.numthreads", numThreads);
    executor = new ThreadPoolExecutor(0, numThreads,
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>());
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @throws IOException if a remote or network exception occurs
   * @return 0 on success, non-zero on failure
   * @throws KeeperException
   * @throws InterruptedException
   */
  int doWork() throws IOException, KeeperException, InterruptedException {
    // print hbase server version
    errors.print("Version: " + status.getHBaseVersion());

    // Make sure regionInfo is empty before starting
    regionInfo.clear();
    tablesInfo.clear();
    emptyRegionInfoQualifiers.clear();
    disabledTables.clear();
    errors.clear();

    // get a list of all regions from the master. This involves
    // scanning the META table
    if (!recordRootRegion()) {
      // Will remove later if we can fix it
      errors.reportError("Encountered fatal error. Exiting...");
      return -1;
    }
    
    getMetaEntries();

    // Check if .META. is found only once and in the right place
    if (!checkMetaEntries()) {
      // Will remove later if we can fix it
      errors.reportError("Encountered fatal error. Exiting...");
      return -1;
    }

    // get a list of all tables that have not changed recently.
    if (!checkMetaOnly) {
      AtomicInteger numSkipped = new AtomicInteger(0);
      HTableDescriptor[] allTables = getTables(numSkipped);
      errors.print("Number of Tables: " +
          (allTables == null ? 0 : allTables.length));
      if (details) {
        if (numSkipped.get() > 0) {
          errors.detail("Number of Tables in flux: " + numSkipped.get());
        }
        if (allTables != null && allTables.length > 0) {
          for (HTableDescriptor td : allTables) {
          String tableName = td.getNameAsString();
          errors.detail("  Table: " + tableName + "\t" +
                             (td.isReadOnly() ? "ro" : "rw") + "\t" +
                             (td.isRootRegion() ? "ROOT" :
                              (td.isMetaRegion() ? "META" : "    ")) + "\t" +
                             " families: " + td.getFamilies().size());
          }
        }
      }
    }
    
    // From the master, get a list of all known live region servers
    Collection<ServerName> regionServers = status.getServers();
    errors.print("Number of live region servers: " +
                       regionServers.size());
    if (details) {
      for (ServerName rsinfo: regionServers) {
        errors.print("  " + rsinfo);
      }
    }

    // From the master, get a list of all dead region servers
    Collection<ServerName> deadRegionServers = status.getDeadServerNames();
    errors.print("Number of dead region servers: " +
                       deadRegionServers.size());
    if (details) {
      for (ServerName name: deadRegionServers) {
        errors.print("  " + name);
      }
    }

    // Determine what's deployed
    processRegionServers(regionServers);

    // Determine what's on HDFS
    checkHdfs();

    // Empty cells in .META.?
    errors.print("Number of empty REGIONINFO_QUALIFIER rows in .META.: " +
      emptyRegionInfoQualifiers.size());
    if (details) {
      for (Result r: emptyRegionInfoQualifiers) {
        errors.print("  " + r);
      }
    }

    // Get disabled tables from ZooKeeper
    loadDisabledTables();

    // Check consistency
    checkConsistency();

    // Check integrity
    checkIntegrity();

    // Print table summary
    printTableSummary();

    return errors.summarize();
  }

  public ErrorReporter getErrors() {
    return errors;
  }

  /**
   * Load the list of disabled tables in ZK into local set.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  private void loadDisabledTables()
  throws ZooKeeperConnectionException, IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        ZooKeeperWatcher zkw = connection.getZooKeeperWatcher();
        try {
          for (String tableName : ZKTable.getDisabledOrDisablingTables(zkw)) {
            disabledTables.add(Bytes.toBytes(tableName));
          }
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
        return null;
      }
    });
  }

  /**
   * Check if the specified region's table is disabled.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   * @throws KeeperException
   */
  private boolean isTableDisabled(HRegionInfo regionInfo) {
    return disabledTables.contains(regionInfo.getTableName());
  }

  /**
   * Scan HDFS for all regions, recording their information into
   * regionInfo
   */
  void checkHdfs() throws IOException, InterruptedException {
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    // list all tables from HDFS
    List<FileStatus> tableDirs = Lists.newArrayList();

    boolean foundVersionFile = false;
    FileStatus[] files = fs.listStatus(rootDir);
    for (FileStatus file : files) {
      String dirName = file.getPath().getName();
      if (dirName.equals(HConstants.VERSION_FILE_NAME)) {
        foundVersionFile = true;
      } else {
        if (!checkMetaOnly ||
            dirName.equals("-ROOT-") ||
            dirName.equals(".META.")) {
          tableDirs.add(file);
        }
      }
    }

    // verify that version file exists
    if (!foundVersionFile) {
      errors.reportError(ERROR_CODE.NO_VERSION_FILE,
          "Version file does not exist in root dir " + rootDir);
    }

    // level 1:  <HBASE_DIR>/*
    WorkItemHdfsDir[] dirs = new WorkItemHdfsDir[tableDirs.size()];  
    int num = 0;
    for (FileStatus tableDir : tableDirs) {
      dirs[num] = new WorkItemHdfsDir(this, fs, errors, tableDir); 
      executor.execute(dirs[num]);
      num++;
    }

    // wait for all directories to be done
    for (int i = 0; i < num; i++) {
      synchronized (dirs[i]) {
        while (!dirs[i].isDone()) {
          dirs[i].wait();
        }
      }
    }
  }

  /**
   * Record the location of the ROOT region as found in ZooKeeper,
   * as if it were in a META table. This is so that we can check
   * deployment of ROOT.
   */
  boolean recordRootRegion() throws IOException {
    HRegionLocation rootLocation = connection.locateRegion(
      HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);

    // Check if Root region is valid and existing
    if (rootLocation == null || rootLocation.getRegionInfo() == null ||
        rootLocation.getHostname() == null) {
      errors.reportError(ERROR_CODE.NULL_ROOT_REGION,
        "Root Region or some of its attributes are null.");
      return false;
    }
    ServerName sn;
    try {
      sn = getRootRegionServerName();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted", e);
    }
    MetaEntry m =
      new MetaEntry(rootLocation.getRegionInfo(), sn, System.currentTimeMillis());
    HbckInfo hbInfo = new HbckInfo(m);
    regionInfo.put(rootLocation.getRegionInfo().getEncodedName(), hbInfo);
    return true;
  }

  private ServerName getRootRegionServerName()
  throws IOException, InterruptedException {
    RootRegionTracker rootRegionTracker =
      new RootRegionTracker(this.connection.getZooKeeperWatcher(), new Abortable() {
        @Override
        public void abort(String why, Throwable e) {
          LOG.error(why, e);
          System.exit(1);
        }
      });
    rootRegionTracker.start();
    ServerName sn = null;
    try {
      sn = rootRegionTracker.getRootRegionLocation();
    } finally {
      rootRegionTracker.stop();
    }
    return sn;
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   */
  void processRegionServers(Collection<ServerName> regionServerList)
  throws IOException, InterruptedException {
    WorkItemRegion[] work = new WorkItemRegion[regionServerList.size()];
    int num = 0;

    // loop to contact each region server in parallel
    for (ServerName rsinfo: regionServerList) {
      work[num] = new WorkItemRegion(this, rsinfo, errors, connection);
      executor.execute(work[num]);
      num++;
    }
    
    // wait for all submitted tasks to be done
    for (int i = 0; i < num; i++) {
      synchronized (work[i]) {
        while (!work[i].isDone()) {
          work[i].wait();
        }
      }
    }
  }

  /**
   * Check consistency of all regions that have been found in previous phases.
   * @throws KeeperException
   * @throws InterruptedException
   */
  void checkConsistency()
  throws IOException, KeeperException, InterruptedException {
    for (java.util.Map.Entry<String, HbckInfo> e: regionInfo.entrySet()) {
      doConsistencyCheck(e.getKey(), e.getValue());
    }
  }

  /**
   * Check a single region for consistency and correct deployment.
   * @throws KeeperException
   * @throws InterruptedException
   */
  void doConsistencyCheck(final String key, final HbckInfo hbi)
  throws IOException, KeeperException, InterruptedException {
    String descriptiveName = hbi.toString();

    boolean inMeta = hbi.metaEntry != null;
    boolean inHdfs = hbi.foundRegionDir != null;
    boolean hasMetaAssignment = inMeta && hbi.metaEntry.regionServer != null;
    boolean isDeployed = !hbi.deployedOn.isEmpty();
    boolean isMultiplyDeployed = hbi.deployedOn.size() > 1;
    boolean deploymentMatchesMeta =
      hasMetaAssignment && isDeployed && !isMultiplyDeployed &&
      hbi.metaEntry.regionServer.equals(hbi.deployedOn.get(0));
    boolean splitParent =
      (hbi.metaEntry == null)? false: hbi.metaEntry.isSplit() && hbi.metaEntry.isOffline();
    boolean shouldBeDeployed = inMeta && !isTableDisabled(hbi.metaEntry);
    boolean recentlyModified = hbi.foundRegionDir != null &&
      hbi.foundRegionDir.getModificationTime() + timelag > System.currentTimeMillis();

    // ========== First the healthy cases =============
    if (hbi.onlyEdits) {
      return;
    }
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
      return;
    } else if (inMeta && !isDeployed && splitParent) {
      return;
    } else if (inMeta && !shouldBeDeployed && !isDeployed) {
      return;
    } else if (recentlyModified) {
      LOG.warn("Region " + descriptiveName + " was recently modified -- skipping");
      return;
    }
    // ========== Cases where the region is not in META =============
    else if (!inMeta && !inHdfs && !isDeployed) {
      // We shouldn't have record of this region at all then!
      assert false : "Entry for region with no data";
    } else if (!inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META_HDFS, "Region "
          + descriptiveName + ", key=" + key + ", not on HDFS or in META but " +
          "deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (!inMeta && inHdfs && !isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META_OR_DEPLOYED, "Region "
          + descriptiveName + " on HDFS, but not listed in META " +
          "or deployed on any region server");
    } else if (!inMeta && inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META, "Region " + descriptiveName
          + " not in META, but deployed on " + Joiner.on(", ").join(hbi.deployedOn));

    // ========== Cases where the region is in META =============
    } else if (inMeta && !inHdfs && !isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS_OR_DEPLOYED, "Region "
          + descriptiveName + " found in META, but not in HDFS "
          + "or deployed on any region server.");
    } else if (inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS, "Region " + descriptiveName
          + " found in META, but not in HDFS, " +
          "and deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (inMeta && inHdfs && !isDeployed && shouldBeDeployed) {
      errors.reportError(ERROR_CODE.NOT_DEPLOYED, "Region " + descriptiveName
          + " not deployed on any region server.");
      // If we are trying to fix the errors
      if (shouldFix()) {
        errors.print("Trying to fix unassigned region...");
        setShouldRerun();
        HBaseFsckRepair.fixUnassigned(this.admin, hbi.metaEntry);
      }
    } else if (inMeta && inHdfs && isDeployed && !shouldBeDeployed) {
      errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED, "Region "
          + descriptiveName + " should not be deployed according " +
          "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (inMeta && inHdfs && isMultiplyDeployed) {
      errors.reportError(ERROR_CODE.MULTI_DEPLOYED, "Region " + descriptiveName
          + " is listed in META on region server " + hbi.metaEntry.regionServer
          + " but is multiply assigned to region servers " +
          Joiner.on(", ").join(hbi.deployedOn));
      // If we are trying to fix the errors
      if (shouldFix()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixDupeAssignment(this.admin, hbi.metaEntry, hbi.deployedOn);
      }
    } else if (inMeta && inHdfs && isDeployed && !deploymentMatchesMeta) {
      errors.reportError(ERROR_CODE.SERVER_DOES_NOT_MATCH_META, "Region "
          + descriptiveName + " listed in META on region server " +
          hbi.metaEntry.regionServer + " but found on region server " +
          hbi.deployedOn.get(0));
      // If we are trying to fix the errors
      if (shouldFix()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixDupeAssignment(this.admin, hbi.metaEntry, hbi.deployedOn);
      }
    } else {
      errors.reportError(ERROR_CODE.UNKNOWN, "Region " + descriptiveName +
          " is in an unforeseen state:" +
          " inMeta=" + inMeta +
          " inHdfs=" + inHdfs +
          " isDeployed=" + isDeployed +
          " isMultiplyDeployed=" + isMultiplyDeployed +
          " deploymentMatchesMeta=" + deploymentMatchesMeta +
          " shouldBeDeployed=" + shouldBeDeployed);
    }
  }

  /**
   * Checks tables integrity. Goes over all regions and scans the tables.
   * Collects all the pieces for each table and checks if there are missing,
   * repeated or overlapping ones.
   */
  void checkIntegrity() {
    for (HbckInfo hbi : regionInfo.values()) {
      // Check only valid, working regions
      if (hbi.metaEntry == null) continue;
      if (hbi.metaEntry.regionServer == null) continue;
      if (hbi.onlyEdits) continue;

      // Missing regionDir or over-deployment is checked elsewhere. Include
      // these cases in modTInfo, so we can evaluate those regions as part of
      // the region chain in META
      //if (hbi.foundRegionDir == null) continue;
      //if (hbi.deployedOn.size() != 1) continue;
      if (hbi.deployedOn.size() == 0) continue;

      // We should be safe here
      String tableName = hbi.metaEntry.getTableNameAsString();
      TInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        modTInfo = new TInfo(tableName);
      }
      for (ServerName server : hbi.deployedOn) {
        modTInfo.addServer(server);
      }

      //modTInfo.addEdge(hbi.metaEntry.getStartKey(), hbi.metaEntry.getEndKey());
      modTInfo.addRegionInfo(hbi);

      tablesInfo.put(tableName, modTInfo);
    }

    for (TInfo tInfo : tablesInfo.values()) {
      if (!tInfo.checkRegionChain()) {
        errors.report("Found inconsistency in table " + tInfo.getName());
      }
    }
  }

  /**
   * Maintain information about a particular table.
   */
  private class TInfo {
    String tableName;
    TreeSet <ServerName> deployedOn;
    List<HbckInfo> regions = new ArrayList<HbckInfo>();

    TInfo(String name) {
      this.tableName = name;
      deployedOn = new TreeSet <ServerName>();
    }

    public void addRegionInfo (HbckInfo r) {
      regions.add(r);
    }

    public void addServer(ServerName server) {
      this.deployedOn.add(server);
    }

    public String getName() {
      return tableName;
    }

    public int getNumRegions() {
      return regions.size();
    }

    /**
     * Check the region chain (from META) of this table.  We are looking for
     * holes, overlaps, and cycles.
     * @return false if there are errors
     */
    public boolean checkRegionChain() {
      Collections.sort(regions);
      HbckInfo last = null;
      int originalErrorsCount = errors.getErrorList().size();

      for (HbckInfo r : regions) {
        if (last == null) {
          // This is the first region, check that the start key is empty
          if (! Bytes.equals(r.metaEntry.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
            errors.reportError(ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY,
                "First region should start with an empty key.",
                this, r);
          }
        } else {

          // Check if endKey < startKey
          // Previous implementation of this code checked for a cycle in the
          // region chain.  A cycle would imply that the endKey comes before
          // the startKey (i.e. endKey < startKey).
          if (! Bytes.equals(r.metaEntry.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)) {
            // continue with this check if this is not the last region
            int cmpRegionKeys = Bytes.compareTo(r.metaEntry.getStartKey(),
                r.metaEntry.getEndKey());
            if (cmpRegionKeys > 0) {
              errors.reportError(ERROR_CODE.REGION_CYCLE,
                  String.format("The endkey for this region comes before the "
                      + "startkey, startkey=%s, endkey=%s",
                      Bytes.toStringBinary(r.metaEntry.getStartKey()),
                      Bytes.toStringBinary(r.metaEntry.getEndKey())),
                  this, r, last);
            }
          }

          // Check if the startkeys are different
          if (Bytes.equals(r.metaEntry.getStartKey(), last.metaEntry.getStartKey())) {
            errors.reportError(ERROR_CODE.DUPE_STARTKEYS,
                "Two regions have the same startkey: "
                    + Bytes.toStringBinary(r.metaEntry.getStartKey()),
                this, r, last);
          } else {
            // Check that the startkey is the same as the previous end key
            int cmp = Bytes.compareTo(r.metaEntry.getStartKey(),
                last.metaEntry.getEndKey());
            if (cmp > 0) {
              // hole
              errors.reportError(ERROR_CODE.HOLE_IN_REGION_CHAIN,
                  "There is a hole in the region chain.",
                  this, r, last);
            } else if (cmp < 0) {
              // overlap
              errors.reportError(ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
                  "There is an overlap in the region chain.",
                  this, r, last);
            }
          }

        }

        last = r;
      }

      return errors.getErrorList().size() == originalErrorsCount;
    }

  }

  /**
   * Return a list of user-space table names whose metadata have not been
   * modified in the last few milliseconds specified by timelag
   * if any of the REGIONINFO_QUALIFIER, SERVER_QUALIFIER, STARTCODE_QUALIFIER,
   * SPLITA_QUALIFIER, SPLITB_QUALIFIER have not changed in the last
   * milliseconds specified by timelag, then the table is a candidate to be returned.
   * @return tables that have not been modified recently
   * @throws IOException if an error is encountered
   */
   HTableDescriptor[] getTables(AtomicInteger numSkipped) {
    List<String> tableNames = new ArrayList<String>();
    long now = System.currentTimeMillis();

    for (HbckInfo hbi : regionInfo.values()) {
      MetaEntry info = hbi.metaEntry;

      // if the start key is zero, then we have found the first region of a table.
      // pick only those tables that were not modified in the last few milliseconds.
      if (info != null && info.getStartKey().length == 0 && !info.isMetaRegion()) {
        if (info.modTime + timelag < now) {
          tableNames.add(info.getTableNameAsString());
        } else {
          numSkipped.incrementAndGet(); // one more in-flux table
        }
      }
    }
    return getHTableDescriptors(tableNames);
  }

   HTableDescriptor[] getHTableDescriptors(List<String> tableNames) {
    HTableDescriptor[] htd = null;
     try {
       LOG.info("getHTableDescriptors == tableNames => " + tableNames);
       htd = new HBaseAdmin(conf).getTableDescriptors(tableNames);
     } catch (IOException e) {
       LOG.debug("Exception getting table descriptors", e);
     }
     return htd;
  }


  /**
   * Gets the entry in regionInfo corresponding to the the given encoded
   * region name. If the region has not been seen yet, a new entry is added
   * and returned.
   */
  private synchronized HbckInfo getOrCreateInfo(String name) {
    HbckInfo hbi = regionInfo.get(name);
    if (hbi == null) {
      hbi = new HbckInfo(null);
      regionInfo.put(name, hbi);
    }
    return hbi;
  }

  /**
    * Check values in regionInfo for .META.
    * Check if zero or more than one regions with META are found.
    * If there are inconsistencies (i.e. zero or more than one regions
    * pretend to be holding the .META.) try to fix that and report an error.
    * @throws IOException from HBaseFsckRepair functions
   * @throws KeeperException
   * @throws InterruptedException
    */
  boolean checkMetaEntries()
  throws IOException, KeeperException, InterruptedException {
    List <HbckInfo> metaRegions = Lists.newArrayList();
    for (HbckInfo value : regionInfo.values()) {
      if (value.metaEntry.isMetaTable()) {
        metaRegions.add(value);
      }
    }

    // If something is wrong
    if (metaRegions.size() != 1) {
      HRegionLocation rootLocation = connection.locateRegion(
        HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);
      HbckInfo root =
          regionInfo.get(rootLocation.getRegionInfo().getEncodedName());

      // If there is no region holding .META.
      if (metaRegions.size() == 0) {
        errors.reportError(ERROR_CODE.NO_META_REGION, ".META. is not found on any region.");
        if (shouldFix()) {
          errors.print("Trying to fix a problem with .META...");
          setShouldRerun();
          // try to fix it (treat it as unassigned region)
          HBaseFsckRepair.fixUnassigned(this.admin, root.metaEntry);
        }
      }
      // If there are more than one regions pretending to hold the .META.
      else if (metaRegions.size() > 1) {
        errors.reportError(ERROR_CODE.MULTI_META_REGION, ".META. is found on more than one region.");
        if (shouldFix()) {
          errors.print("Trying to fix a problem with .META...");
          setShouldRerun();
          // try fix it (treat is a dupe assignment)
          List <ServerName> deployedOn = Lists.newArrayList();
          for (HbckInfo mRegion : metaRegions) {
            deployedOn.add(mRegion.metaEntry.regionServer);
          }
          HBaseFsckRepair.fixDupeAssignment(this.admin, root.metaEntry, deployedOn);
        }
      }
      // rerun hbck with hopefully fixed META
      return false;
    }
    // no errors, so continue normally
    return true;
  }

  /**
   * Scan .META. and -ROOT-, adding all regions found to the regionInfo map.
   * @throws IOException if an error is encountered
   */
  void getMetaEntries() throws IOException {
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      int countRecord = 1;

      // comparator to sort KeyValues with latest modtime
      final Comparator<KeyValue> comp = new Comparator<KeyValue>() {
        public int compare(KeyValue k1, KeyValue k2) {
          return (int)(k1.getTimestamp() - k2.getTimestamp());
        }
      };

      public boolean processRow(Result result) throws IOException {
        try {

          // record the latest modification of this META record
          long ts =  Collections.max(result.list(), comp).getTimestamp();
          Pair<HRegionInfo, ServerName> pair =
            MetaReader.metaRowToRegionPair(result);
          if (pair == null || pair.getFirst() == null) {
            emptyRegionInfoQualifiers.add(result);
            return true;
          }
          ServerName sn = null;
          if (pair.getSecond() != null) {
            sn = pair.getSecond();
          }
          MetaEntry m = new MetaEntry(pair.getFirst(), sn, ts);
          HbckInfo hbInfo = new HbckInfo(m);
          HbckInfo previous = regionInfo.put(pair.getFirst().getEncodedName(), hbInfo);
          if (previous != null) {
            throw new IOException("Two entries in META are same " + previous);
          }

          // show proof of progress to the user, once for every 100 records.
          if (countRecord % 100 == 0) {
            errors.progress();
          }
          countRecord++;
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };

    // Scan -ROOT- to pick up META regions
    MetaScanner.metaScan(conf, visitor, null, null,
      Integer.MAX_VALUE, HConstants.ROOT_TABLE_NAME);

    if (!checkMetaOnly) {
      // Scan .META. to pick up user regions
      MetaScanner.metaScan(conf, visitor);
    }
    
    errors.print("");
  }

  /**
   * Stores the entries scanned from META
   */
  private static class MetaEntry extends HRegionInfo {
    ServerName regionServer;   // server hosting this region
    long modTime;          // timestamp of most recent modification metadata

    public MetaEntry(HRegionInfo rinfo, ServerName regionServer, long modTime) {
      super(rinfo);
      this.regionServer = regionServer;
      this.modTime = modTime;
    }
  }

  /**
   * Maintain information about a particular region.
   */
  static class HbckInfo implements Comparable {
    boolean onlyEdits = false;
    MetaEntry metaEntry = null;
    FileStatus foundRegionDir = null;
    List<ServerName> deployedOn = Lists.newArrayList();

    HbckInfo(MetaEntry metaEntry) {
      this.metaEntry = metaEntry;
    }

    public synchronized void addServer(ServerName server) {
      this.deployedOn.add(server);
    }

    public synchronized String toString() {
      if (metaEntry != null) {
        return metaEntry.getRegionNameAsString();
      } else if (foundRegionDir != null) {
        return foundRegionDir.getPath().toString();
      } else {
        return "UNKNOWN_REGION on " + Joiner.on(", ").join(deployedOn);
      }
    }

    @Override
    public int compareTo(Object o) {
      HbckInfo other = (HbckInfo) o;
      int startComparison = Bytes.compareTo(this.metaEntry.getStartKey(), other.metaEntry.getStartKey());
      if (startComparison != 0)
        return startComparison;
      else
        return Bytes.compareTo(this.metaEntry.getEndKey(), other.metaEntry.getEndKey());
    }
  }

  /**
   * Prints summary of all tables found on the system.
   */
  private void printTableSummary() {
    System.out.println("Summary:");
    for (TInfo tInfo : tablesInfo.values()) {
      if (errors.tableHasErrors(tInfo)) {
        System.out.println("Table " + tInfo.getName() + " is inconsistent.");
      } else {
        System.out.println("  " + tInfo.getName() + " is okay.");
      }
      System.out.println("    Number of regions: " + tInfo.getNumRegions());
      System.out.print("    Deployed on: ");
      for (ServerName server : tInfo.deployedOn) {
        System.out.print(" " + server.toString());
      }
      System.out.println();
    }
  }

  interface ErrorReporter {
    public static enum ERROR_CODE {
      UNKNOWN, NO_META_REGION, NULL_ROOT_REGION, NO_VERSION_FILE, NOT_IN_META_HDFS, NOT_IN_META,
      NOT_IN_META_OR_DEPLOYED, NOT_IN_HDFS_OR_DEPLOYED, NOT_IN_HDFS, SERVER_DOES_NOT_MATCH_META, NOT_DEPLOYED,
      MULTI_DEPLOYED, SHOULD_NOT_BE_DEPLOYED, MULTI_META_REGION, RS_CONNECT_FAILURE,
      FIRST_REGION_STARTKEY_NOT_EMPTY, DUPE_STARTKEYS,
      HOLE_IN_REGION_CHAIN, OVERLAP_IN_REGION_CHAIN, REGION_CYCLE
    }
    public void clear();
    public void report(String message);
    public void reportError(String message);
    public void reportError(ERROR_CODE errorCode, String message);
    public void reportError(ERROR_CODE errorCode, String message, TInfo table, HbckInfo info);
    public void reportError(ERROR_CODE errorCode, String message, TInfo table, HbckInfo info1, HbckInfo info2);
    public int summarize();
    public void detail(String details);
    public ArrayList<ERROR_CODE> getErrorList();
    public void progress();
    public void print(String message);
    public void resetErrors();
    public boolean tableHasErrors(TInfo table);
  }

  private static class PrintingErrorReporter implements ErrorReporter {
    public int errorCount = 0;
    private int showProgress;

    Set<TInfo> errorTables = new HashSet<TInfo>();

    // for use by unit tests to verify which errors were discovered
    private ArrayList<ERROR_CODE> errorList = new ArrayList<ERROR_CODE>();

    public void clear() {
      errorTables.clear();
      errorList.clear();
      errorCount = 0;
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message) {
      errorList.add(errorCode);
      if (!summary) {
        System.out.println("ERROR: " + message);
      }
      errorCount++;
      showProgress = 0;
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message, TInfo table,
                                         HbckInfo info) {
      errorTables.add(table);
      String reference = "(region " + info.metaEntry.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message, TInfo table,
                                         HbckInfo info1, HbckInfo info2) {
      errorTables.add(table);
      String reference = "(regions " + info1.metaEntry.getRegionNameAsString()
          + " and " + info2.metaEntry.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    public synchronized void reportError(String message) {
      reportError(ERROR_CODE.UNKNOWN, message);
    }

    /**
     * Report error information, but do not increment the error count.  Intended for cases
     * where the actual error would have been reported previously.
     * @param message
     */
    public synchronized void report(String message) {
      if (! summary) {
        System.out.println("ERROR: " + message);
      }
      showProgress = 0;
    }

    public synchronized int summarize() {
      System.out.println(Integer.toString(errorCount) +
                         " inconsistencies detected.");
      if (errorCount == 0) {
        System.out.println("Status: OK");
        return 0;
      } else {
        System.out.println("Status: INCONSISTENT");
        return -1;
      }
    }

    public ArrayList<ERROR_CODE> getErrorList() {
      return errorList;
    }

    public synchronized void print(String message) {
      if (!summary) {
        System.out.println(message);
      }
    }

    @Override
    public boolean tableHasErrors(TInfo table) {
      return errorTables.contains(table);
    }

    @Override
    public void resetErrors() {
      errorCount = 0;
    }

    public synchronized void detail(String message) {
      if (details) {
        System.out.println(message);
      }
      showProgress = 0;
    }

    public synchronized void progress() {
      if (showProgress++ == 10) {
        if (!summary) {
          System.out.print(".");
        }
        showProgress = 0;
      }
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class WorkItemRegion implements Runnable {
    private HBaseFsck hbck;
    private ServerName rsinfo;
    private ErrorReporter errors;
    private HConnection connection;
    private boolean done;

    WorkItemRegion(HBaseFsck hbck, ServerName info,
                   ErrorReporter errors, HConnection connection) {
      this.hbck = hbck;
      this.rsinfo = info;
      this.errors = errors;
      this.connection = connection;
      this.done = false;
    }

    // is this task done?
    synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized void run() {
      errors.progress();
      try {
        HRegionInterface server = connection.getHRegionConnection(new HServerAddress(rsinfo.getHostname(), rsinfo.getPort()));

        // list all online regions from this region server
        List<HRegionInfo> regions = server.getOnlineRegions();
        if (hbck.checkMetaOnly) {
          regions = filterOnlyMetaRegions(regions);
        }
        if (details) {
          errors.detail("RegionServer: " + rsinfo.getServerName() +
                           " number of regions: " + regions.size());
          for (HRegionInfo rinfo: regions) {
            errors.detail("  " + rinfo.getRegionNameAsString() +
                             " id: " + rinfo.getRegionId() +
                             " encoded_name: " + rinfo.getEncodedName() +
                             " start: " + Bytes.toStringBinary(rinfo.getStartKey()) +
                             " end: " + Bytes.toStringBinary(rinfo.getEndKey()));
          }
        }

        // check to see if the existence of this region matches the region in META
        for (HRegionInfo r:regions) {
          HbckInfo hbi = hbck.getOrCreateInfo(r.getEncodedName());
          hbi.addServer(rsinfo);
        }
      } catch (IOException e) {          // unable to connect to the region server. 
        errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE, "RegionServer: " + rsinfo.getServerName() +
          " Unable to fetch region information. " + e);
      } finally {
        done = true;
        notifyAll(); // wakeup anybody waiting for this item to be done
      }
    }

    private List<HRegionInfo> filterOnlyMetaRegions(List<HRegionInfo> regions) {
      List<HRegionInfo> ret = Lists.newArrayList();
      for (HRegionInfo hri : regions) {
        if (hri.isMetaRegion() || hri.isRootRegion()) {
          ret.add(hri);
        }
      }
      return ret;
    }
  }

  /**
   * Contact hdfs and get all information about spcified table directory.
   */
  static class WorkItemHdfsDir implements Runnable {
    private HBaseFsck hbck;
    private FileStatus tableDir;
    private ErrorReporter errors;
    private FileSystem fs;
    private boolean done;

    WorkItemHdfsDir(HBaseFsck hbck, FileSystem fs, ErrorReporter errors, 
                    FileStatus status) {
      this.hbck = hbck;
      this.fs = fs;
      this.tableDir = status;
      this.errors = errors;
      this.done = false;
    }

    synchronized boolean isDone() {
      return done;
    } 

    @Override
    public synchronized void run() {
      try {
        String tableName = tableDir.getPath().getName();
        // ignore hidden files
        if (tableName.startsWith(".") &&
            !tableName.equals( Bytes.toString(HConstants.META_TABLE_NAME)))
          return;
        // level 2: <HBASE_DIR>/<table>/*
        FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
        for (FileStatus regionDir : regionDirs) {
          String encodedName = regionDir.getPath().getName();

          // ignore directories that aren't hexadecimal
          if (!encodedName.toLowerCase().matches("[0-9a-f]+")) continue;
  
          HbckInfo hbi = hbck.getOrCreateInfo(encodedName);
          synchronized (hbi) {
            if (hbi.foundRegionDir != null) {
              errors.print("Directory " + encodedName + " duplicate??" +
                           hbi.foundRegionDir);
            }
            hbi.foundRegionDir = regionDir;
        
            // Set a flag if this region contains only edits
            // This is special case if a region is left after split
            hbi.onlyEdits = true;
            FileStatus[] subDirs = fs.listStatus(regionDir.getPath());
            Path ePath = HLog.getRegionDirRecoveredEditsDir(regionDir.getPath());
            for (FileStatus subDir : subDirs) {
              String sdName = subDir.getPath().getName();
              if (!sdName.startsWith(".") && !sdName.equals(ePath.getName())) {
                hbi.onlyEdits = false;
                break;
              }
            }
          }
        }
      } catch (IOException e) {          // unable to connect to the region server. 
        errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE, "Table Directory: " + tableDir.getPath().getName() +
                      " Unable to fetch region information. " + e);
      } finally {
        done = true;
        notifyAll();
      }
    }
  }

  /**
   * Display the full report from fsck.
   * This displays all live and dead region servers, and all known regions.
   */
  void displayFullReport() {
    details = true;
  }

  /**
   * Set summary mode.
   * Print only summary of the tables and status (OK or INCONSISTENT)
   */
  void setSummary() {
    summary = true;
  }

  /**
   * Set META check mode.
   * Print only info about META table deployment/state
   */
  void setCheckMetaOnly() {
    checkMetaOnly = true;
  }

  /**
   * Check if we should rerun fsck again. This checks if we've tried to
   * fix something and we should rerun fsck tool again.
   * Display the full report from fsck. This displays all live and dead
   * region servers, and all known regions.
   */
  void setShouldRerun() {
    rerun = true;
  }

  boolean shouldRerun() {
    return rerun;
  }

  /**
   * Fix inconsistencies found by fsck. This should try to fix errors (if any)
   * found by fsck utility.
   */
  void setFixErrors(boolean shouldFix) {
    fix = shouldFix;
  }

  boolean shouldFix() {
    return fix;
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * META during the last few seconds specified by hbase.admin.fsck.timelag
   * @param seconds - the time in seconds
   */
  void setTimeLag(long seconds) {
    timelag = seconds * 1000; // convert to milliseconds
  }

  protected static void printUsageAndExit() {
    System.err.println("Usage: fsck [opts] ");
    System.err.println(" where [opts] are:");
    System.err.println("   -details Display full report of all regions.");
    System.err.println("   -timelag {timeInSeconds}  Process only regions that " +
                       " have not experienced any metadata updates in the last " +
                       " {{timeInSeconds} seconds.");
    System.err.println("   -fix Try to fix some of the errors.");
    System.err.println("   -sleepBeforeRerun {timeInSeconds} Sleep this many seconds" +
                       " before checking if the fix worked if run with -fix");
    System.err.println("   -summary Print only summary of the tables and status.");
    System.err.println("   -metaonly Only check the state of ROOT and META tables.");
    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get("hbase.rootdir"));
    HBaseFsck fsck = new HBaseFsck(conf);
    long sleepBeforeRerun = DEFAULT_SLEEP_BEFORE_RERUN;

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-details")) {
        fsck.displayFullReport();
      } else if (cmd.equals("-timelag")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -timelag needs a value.");
          printUsageAndExit();
        }
        try {
          long timelag = Long.parseLong(args[i+1]);
          fsck.setTimeLag(timelag);
        } catch (NumberFormatException e) {
          System.err.println("-timelag needs a numeric value.");
          printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-sleepBeforeRerun")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -sleepBeforeRerun needs a value.");
          printUsageAndExit();
        }
        try {
          sleepBeforeRerun = Long.parseLong(args[i+1]);
        } catch (NumberFormatException e) {
          System.err.println("-sleepBeforeRerun needs a numeric value.");
          printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-fix")) {
        fsck.setFixErrors(true);
      } else if (cmd.equals("-summary")) {
        fsck.setSummary();
      } else if (cmd.equals("-metaonly")) {
        fsck.setCheckMetaOnly();
      } else {
        String str = "Unknown command line option : " + cmd;
        LOG.info(str);
        System.out.println(str);
        printUsageAndExit();
      }
    }
    // do the real work of fsck
    int code = fsck.doWork();
    // If we have changed the HBase state it is better to run fsck again
    // to see if we haven't broken something else in the process.
    // We run it only once more because otherwise we can easily fall into
    // an infinite loop.
    if (fsck.shouldRerun()) {
      try {
        LOG.info("Sleeping " + sleepBeforeRerun + "ms before re-checking after fix...");
        Thread.sleep(sleepBeforeRerun);
      } catch (InterruptedException ie) {
        Runtime.getRuntime().exit(code);
      }
      // Just report
      fsck.setFixErrors(false);
      fsck.errors.resetErrors();
      code = fsck.doWork();
    }

    Runtime.getRuntime().exit(code);
  }
}

