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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Check consistency among the in-memory states of the master and the 
 * region server(s) and the state of data in HDFS.
 */
public class HBaseFsck extends HBaseAdmin {
  public static final long DEFAULT_TIME_LAG = 60000; // default value of 1 minute

  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;

  private ClusterStatus status;
  private HMasterInterface master;
  private HConnection connection;
  private TreeMap<HRegionInfo, MetaEntry> metaEntries;

  private boolean details = false; // do we display the full report?
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   */
  public HBaseFsck(Configuration conf) 
    throws MasterNotRunningException, IOException {
    super(conf);
    this.conf = conf;

    // setup filesystem properties
    this.rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    this.fs = rootDir.getFileSystem(conf);


    // fetch information from master
    master = getMaster();
    status = master.getClusterStatus();
    connection = getConnection();
    this.metaEntries = new TreeMap<HRegionInfo, MetaEntry>();
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @throws IOException if a remote or network exception occurs
   * @return 0 on success, non-zero on failure
   */
  int doWork() throws IOException {
    // print hbase server version
    System.out.println("Version: " + status.getHBaseVersion());

    // get a list of all regions from the master. This involves
    // scanning the META table
    getMetaEntries(metaEntries);

    // get a list of all tables that have not changed recently.
    AtomicInteger numSkipped = new AtomicInteger(0);
    HTableDescriptor[] allTables = getTables(metaEntries, numSkipped);
    System.out.println("Number of Tables: " + allTables.length);
    if (details) {
      if (numSkipped.get() > 0) {
        System.out.println("\n Number of Tables in flux: " + numSkipped.get());
      }
      for (HTableDescriptor td : allTables) {
        String tableName = td.getNameAsString();
        System.out.println("\t Table: " + tableName + "\t" +
                           (td.isReadOnly() ? "ro" : "rw") + "\t" +
                           (td.isRootRegion() ? "ROOT" :
                            (td.isMetaRegion() ? "META" : "    ")) + "\t" +
                           " families:" + td.getFamilies().size());
      }
    }

    // From the master, get a list of all known live region servers
    Collection<HServerInfo> regionServers = status.getServerInfo();
    System.out.println("Number of live region servers:" + 
                       regionServers.size());
    if (details) {
      for (HServerInfo rsinfo: regionServers) {
        System.out.println("\t RegionServer:" + rsinfo.getServerName());
      }
    }

    // From the master, get a list of all dead region servers
    Collection<String> deadRegionServers = status.getDeadServerNames();
    System.out.println("Number of dead region servers:" + 
                       deadRegionServers.size());
    if (details) {
      for (String name: deadRegionServers) {
        System.out.println("\t RegionServer(dead):" + name);
      }
    }

    // process information from all region servers
    boolean status1 = processRegionServers(regionServers);

    // match HDFS with META
    boolean status2 = checkHdfs();

    if (status1 == true && status2 == true) {
      System.out.println("\nRest easy, buddy! HBase is clean. ");
      return 0;
    } else {
      System.out.println("\nInconsistencies detected.");
      return -1;
    }
  }

  /**
   * Checks HDFS and META
   * @return true if there were no errors, otherwise return false
   */
  boolean checkHdfs() throws IOException {

    boolean status = true; // success

    // make a copy of all tables in META
    TreeMap<String, MetaEntry> regions = new TreeMap<String, MetaEntry>();
    for (MetaEntry meta: metaEntries.values()) {
      regions.put(meta.getTableDesc().getNameAsString(), meta);
    }

    // list all tables from HDFS
    TreeMap<Path, FileStatus> allTableDirs = new TreeMap<Path, FileStatus>();
    FileStatus[] files = fs.listStatus(rootDir);
    for (int i = 0; files != null && i < files.length; i++) {
      allTableDirs.put(files[i].getPath(), files[i]);
    }

    // verify that -ROOT-,  .META directories exists.
    Path rdir = new Path(rootDir, Bytes.toString(HConstants.ROOT_TABLE_NAME));
    FileStatus ignore = allTableDirs.remove(rdir);
    if (ignore == null) {
      status = false;
      System.out.print("\nERROR: Path " + rdir + " for ROOT table does not exist.");
    }
    Path mdir = new Path(rootDir, Bytes.toString(HConstants.META_TABLE_NAME));
    ignore = allTableDirs.remove(mdir);
    if (ignore == null) {
      status = false;
      System.out.print("\nERROR: Path " + mdir + " for META table does not exist.");
    }

    // verify that version file exists
    Path vfile = new Path(rootDir, HConstants.VERSION_FILE_NAME);
    ignore = allTableDirs.remove(vfile);
    if (ignore == null) {
      status = false;
      System.out.print("\nERROR: Version file " + vfile + " does not exist.");
    }

    // filter out all valid regions found in the META
    for (HRegionInfo rinfo: metaEntries.values()) {
      Path tableDir = HTableDescriptor.getTableDir(rootDir, 
                        rinfo.getTableDesc().getName());
      // Path regionDir = HRegion.getRegionDir(tableDir, rinfo.getEncodedName());
      // if the entry exists in allTableDirs, then remove it from allTableDirs as well
      // as from the META tmp list
      FileStatus found = allTableDirs.remove(tableDir);
      if (found != null) {
        regions.remove(tableDir.getName());
      }
    }

    // The remaining entries in allTableDirs do not have entries in .META
    // However, if the path name was modified in the last few milliseconds
    // as specified by timelag, then do not flag it as an inconsistency.
    long now = System.currentTimeMillis();
    for (FileStatus region: allTableDirs.values()) {
      if (region.getModificationTime() + timelag < now) {
        String finalComponent = region.getPath().getName();
        if (!finalComponent.startsWith(".")) {
          // ignore .logs and .oldlogs directories
          System.out.print("\nERROR: Path " + region.getPath() + 
                           " does not have a corresponding entry in META.");
          status = false;
        }
      }
    }

    // the remaining entries in tmp do not have entries in HDFS
    for (HRegionInfo rinfo: regions.values()) {
      System.out.println("\nERROR: Region " + rinfo.getRegionNameAsString() +
                         " does not have a corresponding entry in HDFS.");
      status = false;
    }
    return status;
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   * @return true if there were no errors, otherwise return false
   */
  boolean processRegionServers(Collection<HServerInfo> regionServerList)
    throws IOException {

    // make a copy of all entries in META
    TreeMap<HRegionInfo, MetaEntry> tmp =
      new TreeMap<HRegionInfo, MetaEntry>(metaEntries);
    long errorCount = 0; // number of inconsistencies detected
    int showProgress = 0;

    // loop to contact each region server
    for (HServerInfo rsinfo: regionServerList) {
      showProgress++;                   // one more server.
      try {
        HRegionInterface server = connection.getHRegionConnection(
                                    rsinfo.getServerAddress());

        // list all online regions from this region server
        HRegionInfo[] regions = server.getRegionsAssignment();
        if (details) {
          System.out.print("\nRegionServer:" + rsinfo.getServerName() +
                           " number of regions:" + regions.length);
          for (HRegionInfo rinfo: regions) {
            System.out.print("\n\t name:" + rinfo.getRegionNameAsString() +
                             " id:" + rinfo.getRegionId() +
                             " encoded name:" + rinfo.getEncodedName() +
                             " start :" + Bytes.toStringBinary(rinfo.getStartKey()) +
                             " end :" + Bytes.toStringBinary(rinfo.getEndKey()));
          }
          showProgress = 0;
        }

        // check to see if the existance of this region matches the region in META
        for (HRegionInfo r: regions) {
          MetaEntry metaEntry = metaEntries.get(r);

          // this entry exists in the region server but is not in the META
          if (metaEntry == null) {
            if (r.isMetaRegion()) {
              continue;           // this is ROOT or META region
            }
            System.out.print("\nERROR: Region " + r.getRegionNameAsString() +
                             " found on server " + rsinfo.getServerAddress() +
                             " but is not listed in META.");
            errorCount++;
            showProgress = 0;
            continue;
          }
          if (!metaEntry.regionServer.equals(rsinfo.getServerAddress())) {
            System.out.print("\nERROR: Region " + r.getRegionNameAsString() +
                             " found on server " + rsinfo.getServerAddress() +
                             " but is listed in META to be on server " + 
                             metaEntry.regionServer);
            errorCount++;
            showProgress = 0;
          }

          // The region server is indeed serving a valid region. Remove it from tmp
          tmp.remove(r);
        }
      } catch (IOException e) {          // unable to connect to the region server. 
        if (details) {
          System.out.print("\nRegionServer:" + rsinfo.getServerName() +
                           " Unable to fetch region information. " + e);
        }
      }
      if (showProgress % 10 == 0) {
        System.out.print("."); // show progress to user
        showProgress = 0;
      }
    }

    // all the region left in tmp are not found on any region server
    for (MetaEntry metaEntry: tmp.values()) {
      // An offlined region will not be present out on a regionserver.  A region
      // is offlined if table is offlined -- will still have an entry in .META.
      // of a region is offlined because its a parent region and its daughters
      // still have references.
      if (metaEntry.isOffline()) continue;
      System.out.print("\nERROR: Region " + metaEntry.getRegionNameAsString() +
                         " is not served by any region server " +
                         " but is listed in META to be on server " + 
                         metaEntry.regionServer);
      errorCount++;
    }

    if (errorCount > 0) {
      System.out.println("\nDetected " + errorCount + " inconsistencies. " +
                         "This might not indicate a real problem because these regions " +
                         "could be in the midst of a split. Consider re-running with a " +
                         "larger value of -timelag.");
      return false;
    }
    return true;    // no errors
  }

  /**
   * Return a list of table names whose metadata have not been modified in the
   * last few milliseconds specified by timelag
   * if any of the REGIONINFO_QUALIFIER, SERVER_QUALIFIER, STARTCODE_QUALIFIER, 
   * SPLITA_QUALIFIER, SPLITB_QUALIFIER have not changed in the last 
   * milliseconds specified by timelag, then the table is a candidate to be returned.
   * @param regionList - all entries found in .META
   * @return tables that have not been modified recently
   * @throws IOException if an error is encountered
   */
  HTableDescriptor[] getTables(final TreeMap<HRegionInfo, MetaEntry> regionList,
                               AtomicInteger numSkipped) {
    TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    long now = System.currentTimeMillis();

    for (MetaEntry m: regionList.values()) {
      HRegionInfo info = m;

      // if the start key is zero, then we have found the first region of a table.
      // pick only those tables that were not modified in the last few milliseconds.
      if (info != null && info.getStartKey().length == 0) {
        if (m.modTime + timelag < now) {
          uniqueTables.add(info.getTableDesc());
        } else {
          numSkipped.incrementAndGet(); // one more in-flux table
        }
      }
    }
    return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  /**
   * Scan META. Returns a list of all regions of all known tables. 
   * @param regionList - fill up all entries found in .META
   * @throws IOException if an error is encountered
   */
  void getMetaEntries(final TreeMap<HRegionInfo,MetaEntry> regionList) throws IOException {
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

            // record region details
            byte[] value = result.getValue(HConstants.CATALOG_FAMILY, 
                                           HConstants.REGIONINFO_QUALIFIER);
            HRegionInfo info = null;
            HServerAddress server = null;
            byte[] startCode = null;
            if (value != null) {
              info = Writables.getHRegionInfo(value);
            }

            // record assigned region server
            value = result.getValue(HConstants.CATALOG_FAMILY,
                                       HConstants.SERVER_QUALIFIER);
            if (value != null && value.length > 0) {
              String address = Bytes.toString(value);
              server = new HServerAddress(address);
            }

            // record region's start key
            value = result.getValue(HConstants.CATALOG_FAMILY, 
                                    HConstants.STARTCODE_QUALIFIER);
            if (value != null) {
              startCode = value;
            }
            MetaEntry m = new MetaEntry(info, server, startCode, ts);
            m = regionList.put(m ,m);
            if (m != null) {
              throw new IOException("Two entries in META are same " + m);
            }

            // show proof of progress to the user, once for every 100 records. 
            if (countRecord % 100 == 0) {
              System.out.print(".");
            }
            countRecord++;
            return true;
          } catch (RuntimeException e) {
            LOG.error("Result=" + result);
            throw e;
          }
        }
      };
      MetaScanner.metaScan(conf, visitor);
      System.out.println("");
  }

  /**
   * Stores the entries scanned from META
   */
  private static class MetaEntry extends HRegionInfo {
    HServerAddress regionServer;   // server hosting this region
    byte[] startCode;        // start value of region
    long modTime;          // timestamp of most recent modification metadata

    public MetaEntry(HRegionInfo rinfo, HServerAddress regionServer, 
                     byte[] startCode, long modTime) {
      super(rinfo);
      this.regionServer = regionServer;
      this.startCode = startCode;
      this.modTime = modTime;
    }
  }

  /**
   * Display the full report from fsck. This displays all live and dead region servers ,
   * and all known regions.
   */
  void displayFullReport() {
    details = true;
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
    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) 
    throws IOException, MasterNotRunningException {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get("hbase.rootdir"));
    HBaseFsck fsck = new HBaseFsck(conf);

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
      } else {
        String str = "Unknown command line option : " + cmd;
        LOG.info(str);
        System.out.println(str);
        printUsageAndExit();
      }
    }
    // do the real work of fsck
    int code = fsck.doWork();
    Runtime.getRuntime().exit(code);
  }
}

