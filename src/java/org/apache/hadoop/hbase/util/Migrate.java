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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
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
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
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
 * <p>This script has a 'check' and 'excecute' mode.  Adding migration steps,
 * its important to keep this in mind.  Testing if migration needs to be run,
 * be careful not to make presumptions about the current state of the data in
 * the filesystem.  It may have a format from many versions previous with
 * layout not as expected or keys and values of a format not expected.  Tools
 * such as {@link MetaUtils} may not work as expected when running against
 * old formats -- or, worse, fail in ways that are hard to figure (One such is
 * edits made by previous migration steps not being apparent in later migration
 * steps).  The upshot is always verify presumptions migrating.
 */
public class Migrate extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Migrate.class);

  private static final String OLD_PREFIX = "hregion_";

  private final HBaseConfiguration conf;
  private FileSystem fs;
  
  // Gets set by migration methods if we are in readOnly mode.
  private boolean migrationNeeded = false;

  /** Action to take when an extra file or unrecoverd log file is found */
  private static String ACTIONS = "abort|ignore|delete|prompt";
  private static enum ACTION  {
    /** Stop conversion */
    ABORT,
    /** print a warning message, but otherwise ignore */
    IGNORE,
    /** delete extra files */
    DELETE,
    /** prompt for disposition of extra files */
    PROMPT
  }
  
  private static final Map<String, ACTION> options =
    new HashMap<String, ACTION>();
  
  static {
   options.put("abort", ACTION.ABORT);
   options.put("ignore", ACTION.IGNORE);
   options.put("delete", ACTION.DELETE);
   options.put("prompt", ACTION.PROMPT);
  }

  private boolean readOnly = false;
  private ACTION otherFiles = ACTION.IGNORE;

  private BufferedReader reader = null;
  
  private final Set<String> references = new HashSet<String>();
  
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
      String version = FSUtils.getVersion(fs, FSUtils.getRootDir(this.conf));
      if (version != null && 
          version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) {
        LOG.info("No upgrade necessary.");
        return 0;
      }

      // Dependent on which which version hbase is at, run appropriate
      // migrations.  Be consious that scripts can be run in readOnly -- i.e.
      // check if migration is needed -- and then in actual migrate mode.  Be
      // careful when writing your scripts that you do not make presumption
      // about state of the FileSystem.  For example, in script that migrates
      // between 2 and 3, it should not presume the layout is that of v2.  If
      // readOnly mode, the pre-v2 scripts may not have been run yet.
      if (version == null) {
        FileStatus[] rootFiles = getRootDirFiles();
        migrateFromNoVersion(rootFiles);
        migrateToV2(rootFiles);
        migrateToV3();
      } else if (version.compareTo("0.1") == 0) {
        migrateToV2(getRootDirFiles());
        migrateToV3();
      } else if (version.compareTo("2") == 0) {
        migrateToV3();
      } else if (version.compareTo("3") == 0) {
        // Nothing to do.
      } else {
        throw new IOException("Unrecognized version: " + version);
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

  private void migrateFromNoVersion(FileStatus[] rootFiles) throws IOException {
    LOG.info("No file system version found. Checking to see if hbase in " +
      "Filesystem is at revision 0.1");

    // Check to see if new root region dir exists
    boolean newRootRegion = checkNewRootRegionDirExists();
    if (this.readOnly &&  !newRootRegion) {
      this.migrationNeeded = true;
      return;
    }

    // Check for unrecovered region server log files
    checkForUnrecoveredLogFiles(rootFiles);

    // Check for "extra" files and for old upgradable regions
    extraFiles(rootFiles);

    if (!newRootRegion) {
      // find root region
      String rootRegion = OLD_PREFIX +
        HRegionInfo.ROOT_REGIONINFO.getEncodedName();
      if (!fs.exists(new Path(FSUtils.getRootDir(this.conf), rootRegion))) {
        throw new IOException("Cannot find root region " + rootRegion);
      } else if (readOnly) {
        migrationNeeded = true;
      } else {
        migrateRegionDir(HConstants.ROOT_TABLE_NAME, rootRegion);
        scanRootRegion();

        // scan for left over regions
        extraRegions();
      }
    }
  }
  
  private void migrateToV2(FileStatus[] rootFiles) throws IOException {
    LOG.info("Checking to see if hbase in Filesystem is at version 2.");
    checkForUnrecoveredLogFiles(rootFiles);
  }

  private void migrateToV3() throws IOException {
    LOG.info("Checking to see if hbase in Filesystem is at version 3.");
    addHistorianFamilyToMeta();
  }

  private FileStatus[] getRootDirFiles() throws IOException {
    FileStatus[] stats = fs.listStatus(FSUtils.getRootDir(this.conf));
    if (stats == null || stats.length == 0) {
      throw new IOException("No files found under root directory " +
        FSUtils.getRootDir(this.conf).toString());
    }
    return stats;
  }
  
  private boolean checkNewRootRegionDirExists() throws IOException {
    Path rootRegionDir =  HRegion.getRegionDir(FSUtils.getRootDir(this.conf),
      HRegionInfo.ROOT_REGIONINFO);
    boolean newRootRegion = fs.exists(rootRegionDir);
    this.migrationNeeded = !newRootRegion;
    return newRootRegion;
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
          " and deleted.");
    }
  }
  
  // Check for files that should not be there or should be migrated
  private void extraFiles(FileStatus[] stats) throws IOException {
    for (int i = 0; i < stats.length; i++) {
      String name = stats[i].getPath().getName();
      boolean newRootRegion = checkNewRootRegionDirExists();
      if (name.startsWith(OLD_PREFIX)) {
        if (!newRootRegion) {
          // We need to migrate if the new root region directory doesn't exist
          migrationNeeded = true;
          String regionName = name.substring(OLD_PREFIX.length());
          try {
            Integer.parseInt(regionName);
          } catch (NumberFormatException e) {
            extraFile(otherFiles, "Old region format can not be upgraded: " +
                name, stats[i].getPath());
          }
        } else {
          // Since the new root region directory exists, we assume that this
          // directory is not necessary
          extraFile(otherFiles, "Old region directory found: " + name,
              stats[i].getPath());
        }
      } else {
        // File name does not start with "hregion_"
        if (!newRootRegion) {
          // new root region directory does not exist. This is an extra file
          String message = "Unrecognized file " + name;
          extraFile(otherFiles, message, stats[i].getPath());
        }
      }
    }
  }

  private void extraFile(ACTION action, String message, Path p)
  throws IOException {
    
    if (action == ACTION.ABORT) {
      throw new IOException(message + " aborting");
    } else if (action == ACTION.IGNORE) {
      LOG.info(message + " ignoring");
    } else if (action == ACTION.DELETE) {
      LOG.info(message + " deleting");
      fs.delete(p, true);
    } else {
      // ACTION.PROMPT
      String response = prompt(message + " delete? [y/n]");
      if (response.startsWith("Y") || response.startsWith("y")) {
        LOG.info(message + " deleting");
        fs.delete(p, true);
      }
    }
  }
  
  void migrateRegionDir(final byte [] tableName, String oldPath)
  throws IOException {
    // Create directory where table will live
    Path rootdir = FSUtils.getRootDir(this.conf);
    Path tableDir = new Path(rootdir, Bytes.toString(tableName));
    fs.mkdirs(tableDir);

    // Move the old region directory under the table directory

    Path newPath = new Path(tableDir,
        oldPath.substring(OLD_PREFIX.length()));
    fs.rename(new Path(rootdir, oldPath), newPath);

    processRegionSubDirs(fs, newPath);
  }
  
  private void processRegionSubDirs(FileSystem fs, Path newPath)
  throws IOException {
    String newName = newPath.getName();
    FileStatus[] children = fs.listStatus(newPath);
    for (int i = 0; i < children.length; i++) {
      String child = children[i].getPath().getName();
      if (children[i].isDir()) {
        processRegionSubDirs(fs, children[i].getPath());

        // Rename old compaction directories

        if (child.startsWith(OLD_PREFIX)) {
          fs.rename(children[i].getPath(),
              new Path(newPath, child.substring(OLD_PREFIX.length())));
        }
      } else {
        if (newName.compareTo("mapfiles") == 0) {
          // Check to see if this mapfile is a reference

          if (HStore.isReference(children[i].getPath())) {
            // Keep track of references in case we come across a region
            // that we can't otherwise account for.
            references.add(child.substring(child.indexOf(".") + 1));
          }
        }
      }
    }
  }
  
  private void scanRootRegion() throws IOException {
    final MetaUtils utils = new MetaUtils(this.conf);
    try {
      utils.scanRootRegion(new MetaUtils.ScannerListener() {
        public boolean processRow(HRegionInfo info) throws IOException {
          // First move the meta region to where it should be and rename
          // subdirectories as necessary
          migrateRegionDir(HConstants.META_TABLE_NAME, OLD_PREFIX
              + info.getEncodedName());
          utils.scanMetaRegion(info, new MetaUtils.ScannerListener() {
            public boolean processRow(HRegionInfo tableInfo) throws IOException {
              // Move the region to where it should be and rename
              // subdirectories as necessary
              migrateRegionDir(tableInfo.getTableDesc().getName(), OLD_PREFIX
                  + tableInfo.getEncodedName());
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

  private void extraRegions() throws IOException {
    Path rootdir = FSUtils.getRootDir(this.conf);
    FileStatus[] stats = fs.listStatus(rootdir);
    if (stats == null || stats.length == 0) {
      throw new IOException("No files found under root directory " +
          rootdir.toString());
    }
    for (int i = 0; i < stats.length; i++) {
      String name = stats[i].getPath().getName();
      if (name.startsWith(OLD_PREFIX)) {
        String encodedName = name.substring(OLD_PREFIX.length());
        String message;
        if (references.contains(encodedName)) {
          message =
            "Region not in meta table but other regions reference it " + name;
        } else {
          message = 
            "Region not in meta table and no other regions reference it " + name;
        }
        extraFile(otherFiles, message, stats[i].getPath());
      }
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
            false, false, Integer.MAX_VALUE, HConstants.FOREVER, null));
        LOG.info("Historian family added to .META.");
        // Flush out the meta edits.
      }
    } finally {
      utils.shutdown();
    }
  }

  @SuppressWarnings("static-access")
  private int parseArgs(String[] args) {
    Options opts = new Options();
    Option extraFiles = OptionBuilder.withArgName(ACTIONS)
    .hasArg()
    .withDescription("disposition of 'extra' files: {abort|ignore|delete|prompt}")
    .create("extrafiles");
    
    opts.addOption(extraFiles);
    
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

    if (readOnly) {
      this.otherFiles = ACTION.IGNORE;

    } else {
      CommandLine commandLine = parser.getCommandLine();

      ACTION action = null;
      if (commandLine.hasOption("extrafiles")) {
        action = options.get(commandLine.getOptionValue("extrafiles"));
        if (action == null) {
          usage();
          return -1;
        }
        this.otherFiles = action;
      }
    }
    return 0;
  }
  
  private void usage() {
    System.err.println("Usage: bin/hbase migrate { check | upgrade } [options]\n");
    System.err.println("  check                            perform upgrade checks only.");
    System.err.println("  upgrade                          perform upgrade checks and modify hbase.\n");
    System.err.println("  Options are:");
    System.err.println("    -extrafiles={abort|ignore|delete|prompt}");
    System.err.println("                                   action to take if \"extra\" files are found.\n");
    System.err.println("    -conf <configuration file>     specify an application configuration file");
    System.err.println("    -D <property=value>            use value for given property");
    System.err.println("    -fs <local|namenode:port>      specify a namenode");
  }
  
  private synchronized String prompt(String prompt) {
    System.out.print(prompt + " > ");
    System.out.flush();
    if (reader == null) {
      reader = new BufferedReader(new InputStreamReader(System.in));
    }
    try {
      return reader.readLine();
      
    } catch (IOException e) {
      return null;
    }
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
