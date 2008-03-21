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
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
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

import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;

/**
 * Perform a file system upgrade to convert older file layouts to that
 * supported by HADOOP-2478, and then to the form supported by HBASE-69
 */
public class Migrate extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(Migrate.class);

  private static final String OLD_PREFIX = "hregion_";

  private final HBaseConfiguration conf;
  FileSystem fs;
  Path rootdir;
  MetaUtils utils;

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
  private boolean migrationNeeded = false;
  private boolean newRootRegion = false;
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
  
  /** {@inheritDoc} */
  public int run(String[] args) {
    if (parseArgs(args) != 0) {
      return -1;
    }

    try {
      // Verify file system is up.
      fs = FileSystem.get(conf);                        // get DFS handle
      LOG.info("Verifying that file system is available...");
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return -1;
    }
    
    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...");
    try {
      HBaseAdmin.checkHBaseAvailable(conf);
      LOG.fatal("HBase cluster must be off-line.");
      return -1;
    } catch (MasterNotRunningException e) {
      // Expected. Ignore.
    }
    
    try {
       
      // Initialize MetaUtils and and get the root of the HBase installation
      
      this.utils = new MetaUtils(conf);
      this.rootdir = utils.initialize();

      LOG.info("Starting upgrade" + (readOnly ? " check" : ""));

      // See if there is a file system version file

      String version = FSUtils.getVersion(fs, rootdir);
      if (version != null && 
          version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) {
        LOG.info("No upgrade necessary.");
        return 0;
      }

      // Get contents of root directory
      
      FileStatus[] rootFiles = getRootDirFiles();

      if (version == null) {
        migrateFromNoVersion(rootFiles);
        migrateToV2(rootFiles);
      } else if (version.compareTo("0.1") == 0) {
        migrateToV2(rootFiles);
      } else if (version.compareTo("2") == 0) {
        // Nothing to do (yet)
      } else {
        throw new IOException("Unrecognized version: " + version);
      }
      
      if (!readOnly) {
        // set file system version
        LOG.info("Setting file system version.");
        FSUtils.setVersion(fs, rootdir);
        LOG.info("Upgrade successful.");
      } else if (migrationNeeded) {
        LOG.info("Upgrade needed.");
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Upgrade" +  (readOnly ? " check" : "") + " failed", e);
      return -1;
      
    } finally {
      if (utils != null && utils.isInitialized()) {
        utils.shutdown();
      }
    }
  }

  private void migrateFromNoVersion(FileStatus[] rootFiles) throws IOException {
    LOG.info("No file system version found. Checking to see if file system " +
        "is at revision 0.1");
    
    // check to see if new root region dir exists

    checkNewRootRegionDirExists();
    
    // check for unrecovered region server log files

    checkForUnrecoveredLogFiles(rootFiles);

    // check for "extra" files and for old upgradable regions

    extraFiles(rootFiles);

    if (!newRootRegion) {
      // find root region

      String rootRegion = OLD_PREFIX +
        HRegionInfo.rootRegionInfo.getEncodedName();

      if (!fs.exists(new Path(rootdir, rootRegion))) {
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
    LOG.info("Checking to see if file system is at revision 2.");
    checkForUnrecoveredLogFiles(rootFiles);
  }

  private FileStatus[] getRootDirFiles() throws IOException {
    FileStatus[] stats = fs.listStatus(rootdir);
    if (stats == null || stats.length == 0) {
      throw new IOException("No files found under root directory " +
          rootdir.toString());
    }
    return stats;
  }
  
  private void checkNewRootRegionDirExists() throws IOException {
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.rootRegionInfo);
    newRootRegion = fs.exists(rootRegionDir);
    migrationNeeded = !newRootRegion;
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
      fs.delete(p);
    } else {
      // ACTION.PROMPT
      String response = prompt(message + " delete? [y/n]");
      if (response.startsWith("Y") || response.startsWith("y")) {
        LOG.info(message + " deleting");
        fs.delete(p);
      }
    }
  }
  
  void migrateRegionDir(Text tableName, String oldPath)throws IOException {

    // Create directory where table will live

    Path tableDir = new Path(rootdir, tableName.toString());
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
    utils.scanRootRegion(
      new MetaUtils.ScannerListener() {
        public boolean processRow(HRegionInfo info) throws IOException {
          // First move the meta region to where it should be and rename
          // subdirectories as necessary

          migrateRegionDir(HConstants.META_TABLE_NAME,
              OLD_PREFIX + info.getEncodedName());

          utils.scanMetaRegion(info,
            new MetaUtils.ScannerListener() {
              public boolean processRow(HRegionInfo tableInfo)
              throws IOException {
                // Move the region to where it should be and rename
                // subdirectories as necessary

                migrateRegionDir(tableInfo.getTableDesc().getName(),
                    OLD_PREFIX + tableInfo.getEncodedName());
                return true;
              }
            }
          );
          return true;
        }
      }
    );
  }
  
  private void extraRegions() throws IOException {
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
