/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.migration.nineteen;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Migrate;
import org.apache.hadoop.hbase.util.FSUtils.DirFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Mapper that rewrites hbase 0.19 HStoreFiles as 0.20 StoreFiles.
 * Creates passed directories as input and output.  On startup, it does not
 * check filesystem is 0.19 generation just in case it fails part way so it
 * should be possible to rerun the MR job.  It'll just fix the 0.19 regions
 * found.
 * If the input dir does not exist, it first crawls the filesystem to find the
 * files to migrate writing a file into the input directory.  Next it starts up
 * the MR job to rewrite the 0.19 HStoreFiles as 0.20 StoreFiles deleting the
 * old as it goes.  Presumption is that only
 * one file per in the family Store else stuff breaks; i.e. the 0.19 install
 * was major compacted before migration began.  If this job fails, fix why then
 * it should be possible to rerun the job.  You may want to edit the
 * generated file in the input dir first.
 */
public class HStoreFileToStoreFile extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(HStoreFileToStoreFile.class);
  public static final String JOBNAME = "hsf2sf";

  HStoreFileToStoreFile() {
    super();
  }

  public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
    throws java.io.IOException, InterruptedException {
      HBaseConfiguration c = new HBaseConfiguration(context.getConfiguration());
      Path p = new Path(value.toString());
      context.setStatus(key.toString() + " " + p.toString());
      Migrate.rewrite(c, FileSystem.get(c), p);
    }
  }

  private static void writeInputFiles(final HBaseConfiguration conf,
      final FileSystem fs, final Path dir)
  throws IOException {
    if (fs.exists(dir)) {
      LOG.warn("Input directory already exits. Using content for this MR job.");
      return;
    }
    FSDataOutputStream out = fs.create(new Path(dir, "mapfiles"));
    try {
      gathermapfiles(conf, fs, out);
    } finally {
      if (out != null) out.close();
    }
  }
  
  private static void gathermapfiles(final HBaseConfiguration conf,
      final FileSystem fs, final FSDataOutputStream out)
  throws IOException {
    // Presumes any directory under hbase.rootdir is a table.
    FileStatus [] tableDirs =
      fs.listStatus(FSUtils.getRootDir(conf), new DirFilter(fs));
    for (int i = 0; i < tableDirs.length; i++) {
      // Inside a table, there are compaction.dir directories to skip.
      // Otherwise, all else should be regions.  Then in each region, should
      // only be family directories.  Under each of these, should be a mapfile
      // and info directory and in these only one file.
      Path d = tableDirs[i].getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) continue;
      FileStatus [] regionDirs = fs.listStatus(d, new DirFilter(fs));
      for (int j = 0; j < regionDirs.length; j++) {
        Path dd = regionDirs[j].getPath();
        if (dd.equals(HConstants.HREGION_COMPACTIONDIR_NAME)) continue;
        // Else its a region name.  Now look in region for families.
        FileStatus [] familyDirs = fs.listStatus(dd, new DirFilter(fs));
        for (int k = 0; k < familyDirs.length; k++) {
          Path family = familyDirs[k].getPath();
          FileStatus [] infoAndMapfile = fs.listStatus(family);
          // Assert that only info and mapfile in family dir.
          if (infoAndMapfile.length != 2) {
            LOG.warn(family.toString() + " has more than just info and mapfile: " +
              infoAndMapfile.length + ". Continuing...");
            continue;
          }
          // Make sure directory named info or mapfile.
          for (int ll = 0; ll < 2; ll++) {
            if (infoAndMapfile[ll].getPath().getName().equals("info") ||
                infoAndMapfile[ll].getPath().getName().equals("mapfiles"))
              continue;
            LOG.warn("Unexpected directory name: " +
                infoAndMapfile[ll].getPath() + ". Continuing...");
            continue;
          }
          // Now in family, there are 'mapfile' and 'info' subdirs.  Just
          // look in the 'mapfile' subdir.
          Path mfsdir = new Path(family, "mapfiles");
          FileStatus [] familyStatus = fs.listStatus(mfsdir);
          if (familyStatus == null || familyStatus.length > 1) {
            LOG.warn(family.toString() + " has " +
              ((familyStatus == null) ? "null": familyStatus.length) +
              " files.  Continuing...");
            continue;
          }
          if (familyStatus.length == 1) {
            // If we got here, then this is good.  Add the mapfile to out
            String str = familyStatus[0].getPath().makeQualified(fs).toString();
            LOG.info(str);
            out.write(Bytes.toBytes(str + "\n"));
          } else {
            // Special case.  Empty region.  Remove the mapfiles and info dirs.
            Path infodir = new Path(family, "info");
            LOG.info("Removing " + mfsdir + " and " + infodir + " because empty");
            fs.delete(mfsdir, true);
            fs.delete(infodir, true);
          }
        }
      }
    }
  }

  public int run(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("ERROR: Wrong number of arguments: " + args.length);
      System.err.println("Usage: " + getClass().getSimpleName() +
        " <inputdir> <outputdir>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    Path input = new Path(args[0]);
    HBaseConfiguration conf = (HBaseConfiguration)getConf();
    FileSystem fs = FileSystem.get(conf);
    writeInputFiles(conf, fs, input);
    Job job = new Job(conf);
    job.setJarByClass(HStoreFileToStoreFile.class);
    job.setJobName(JOBNAME);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, input);
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
      new HStoreFileToStoreFile(), args);
    System.exit(exitCode);
  }
}