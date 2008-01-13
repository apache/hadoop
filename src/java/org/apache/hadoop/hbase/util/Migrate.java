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

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HLog;
import org.apache.hadoop.hbase.HRegion;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStore;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.MasterNotRunningException;

/**
 * Perform a file system upgrade to convert older file layouts to that
 * supported by HADOOP-2478
 */
public class Migrate extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(Migrate.class);

  private static final String OLD_PREFIX = "hregion_";

  private final HBaseConfiguration conf;

  /** Action to take when an extra file is found */
  private static enum EXTRA_FILES  {
    /** Stop conversion */
    ABORT,
    /** print a warning message, but otherwise ignore */
    IGNORE,
    /** delete extra files */
    DELETE,
    /** prompt for disposition of extra files */
    PROMPT
  }
  
  private static final Map<String, EXTRA_FILES> options =
    new HashMap<String, EXTRA_FILES>();
  
  static {
   options.put("abort", EXTRA_FILES.ABORT);
   options.put("ignore", EXTRA_FILES.IGNORE);
   options.put("delete", EXTRA_FILES.DELETE);
   options.put("prompt", EXTRA_FILES.PROMPT);
  }
  
  private EXTRA_FILES logFiles = EXTRA_FILES.ABORT;
  private EXTRA_FILES otherFiles = EXTRA_FILES.IGNORE;

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
    conf.setInt("hbase.client.retries.number", 1);
  }
  
  /** {@inheritDoc} */
  public int run(String[] args) throws Exception {
    parseArgs(args);
    
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (admin.isMasterRunning()) {
        throw new IllegalStateException(
        "HBase cluster must be off-line while being upgraded");
      }
    } catch (MasterNotRunningException e) {
      // ignore
    }
    FileSystem fs = FileSystem.get(conf);               // get DFS handle
    Path rootdir = fs.makeQualified(new Path(          // get path for instance
        conf.get(HConstants.HBASE_DIR, HConstants.DEFAULT_HBASE_DIR)));

    // check for "extra" files

    extraFiles(fs, rootdir);

    // find root region

    Path rootRegion = new Path(rootdir, 
        OLD_PREFIX + HRegionInfo.rootRegionInfo.getEncodedName());

    if (!fs.exists(rootRegion)) {
      throw new IOException("cannot find root region " + rootRegion.toString());
    }

    processRegionDir(fs, rootdir, HConstants.ROOT_TABLE_NAME, rootRegion);
    scanRootRegion(fs, rootdir);

    // scan for left over regions

    extraRegions(fs, rootdir);

    return 0;
  }

  private void extraFiles(FileSystem fs, Path rootdir) throws IOException {
    FileStatus[] stats = fs.listStatus(rootdir);
    if (stats == null || stats.length == 0) {
      throw new IOException("No files found under root directory " +
          rootdir.toString());
    }
    for (int i = 0; i < stats.length; i++) {
      String name = stats[i].getPath().getName();
      if (!name.startsWith(OLD_PREFIX)) {
        if (name.startsWith("log_")) {
          String message = "unrecovered region server log file " + name; 
          extraFile(logFiles, message, fs, stats[i].getPath());
        } else {
          String message = "unrecognized file " + name;
          extraFile(otherFiles, message, fs, stats[i].getPath());
        }
      }
    }
  }

  private void extraFile(EXTRA_FILES action, String message, FileSystem fs,
      Path p) throws IOException {
    
    if (action == EXTRA_FILES.ABORT) {
      throw new IOException(message + " aborting");

    } else if (action == EXTRA_FILES.IGNORE) {
      LOG.info(message + " ignoring");

    } else if (action == EXTRA_FILES.DELETE) {
      LOG.info(message + " deleting");
      fs.delete(p);

    } else {
      // logFiles == EXTRA_FILES.PROMPT
      String response = prompt(message + " delete? [y/n]");

      if (response.startsWith("Y") || response.startsWith("y")) {
        LOG.info(message + " deleting");
        fs.delete(p);
      }
    }
  }
  
  private void processRegionDir(FileSystem fs, Path rootdir, Text tableName,
      Path oldPath) throws IOException {

    // Create directory where table will live

    Path tableDir = new Path(rootdir, tableName.toString());
    fs.mkdirs(tableDir);

    // Move the old region directory under the table directory

    Path newPath =
      new Path(tableDir, oldPath.getName().substring(OLD_PREFIX.length()));
    fs.rename(oldPath, newPath);

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
  
  private void scanRootRegion(FileSystem fs, Path rootdir) throws IOException {
    HLog log = new HLog(fs, new Path(rootdir, HConstants.HREGION_LOGDIR_NAME),
        conf, null);

    try {
      // Open root region so we can scan it

      HRegion rootRegion = new HRegion(
          new Path(rootdir, HConstants.ROOT_TABLE_NAME.toString()), log, fs, conf,
          HRegionInfo.rootRegionInfo, null, null);

      try {
        HScannerInterface rootScanner = rootRegion.getScanner(
            HConstants.COL_REGIONINFO_ARRAY, HConstants.EMPTY_START_ROW,
            HConstants.LATEST_TIMESTAMP, null);

        try {
          HStoreKey key = new HStoreKey();
          SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          while (rootScanner.next(key, results)) {
            HRegionInfo info = Writables.getHRegionInfoOrNull(
                results.get(HConstants.COL_REGIONINFO));
            if (info == null) {
              LOG.warn("region info is null for row " + key.getRow() +
                  " in table " + HConstants.ROOT_TABLE_NAME);
              continue;
            }

            // First move the meta region to where it should be and rename
            // subdirectories as necessary

            processRegionDir(fs, rootdir, HConstants.META_TABLE_NAME,
                new Path(rootdir, OLD_PREFIX + info.getEncodedName()));

            // Now scan and process the meta table

            scanMetaRegion(fs, rootdir, log, info);
          }

        } finally {
          rootScanner.close();
        }

      } finally {
        rootRegion.close();
      }

    } finally {
      log.closeAndDelete();
    }
  }
  
  private void scanMetaRegion(FileSystem fs, Path rootdir, HLog log,
      HRegionInfo info) throws IOException {

    HRegion metaRegion = new HRegion(
        new Path(rootdir, info.getTableDesc().getName().toString()), log, fs,
        conf, info, null, null);

    try {
      HScannerInterface metaScanner = metaRegion.getScanner(
          HConstants.COL_REGIONINFO_ARRAY, HConstants.EMPTY_START_ROW,
          HConstants.LATEST_TIMESTAMP, null);

      try {
        HStoreKey key = new HStoreKey();
        SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
        while (metaScanner.next(key, results)) {
          HRegionInfo region = Writables.getHRegionInfoOrNull(
              results.get(HConstants.COL_REGIONINFO));
          if (region == null) {
            LOG.warn("region info is null for row " + key.getRow() +
                " in table " + HConstants.META_TABLE_NAME);
            continue;
          }

          // Move the region to where it should be and rename
          // subdirectories as necessary

          processRegionDir(fs, rootdir, region.getTableDesc().getName(),
              new Path(rootdir, OLD_PREFIX + region.getEncodedName()));

          results.clear();
        }

      } finally {
        metaScanner.close();
      }

    } finally {
      metaRegion.close();
    }
  }
  
  private void extraRegions(FileSystem fs, Path rootdir) throws IOException {
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
            "region not in meta table but other regions reference it " + name;

        } else {
          message = 
            "region not in meta table and no other regions reference it " + name;
        }
        extraFile(otherFiles, message, fs, stats[i].getPath());
      }
    }
  }
  
  @SuppressWarnings("static-access")
  private void parseArgs(String[] args) {
    Options opts = new Options();
    Option logFiles = OptionBuilder.withArgName("abort|ignore|delete|prompt")
    .hasArg()
    .withDescription(
        "disposition of unrecovered region server logs: {abort|ignore|delete|prompt}")
    .create("logfiles");

    Option extraFiles = OptionBuilder.withArgName("abort|ignore|delete|prompt")
    .hasArg()
    .withDescription("disposition of 'extra' files: {abort|ignore|delete|prompt}")
    .create("extrafiles");
    
    opts.addOption(logFiles);
    opts.addOption(extraFiles);
    
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine commandLine = parser.parse(opts, args, true);
      if (commandLine.hasOption("log-files")) {
        this.logFiles = options.get(commandLine.getOptionValue("log-files"));
      }
      if (commandLine.hasOption("extra-files")) {
        this.otherFiles = options.get(commandLine.getOptionValue("extra-files"));
      }
    } catch (ParseException e) {
      LOG.error("options parsing failed", e);
      
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("options are: ", opts);
    }
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
    Tool t = new Migrate();
    GenericOptionsParser hadoopOpts =
      new GenericOptionsParser(t.getConf(), args);

    int status = 0;
    try {
      status = ToolRunner.run(t, hadoopOpts.getRemainingArgs());
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
