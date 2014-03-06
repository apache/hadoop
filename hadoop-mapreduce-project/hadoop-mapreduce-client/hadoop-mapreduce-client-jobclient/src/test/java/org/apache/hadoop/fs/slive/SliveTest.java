/**
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

package org.apache.hadoop.fs.slive;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.ArgumentParser.ParsedOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Slive test entry point + main program
 * 
 * This program will output a help message given -help which can be used to
 * determine the program options and configuration which will affect the program
 * runtime. The program will take these options, either from configuration or
 * command line and process them (and merge) and then establish a job which will
 * thereafter run a set of mappers & reducers and then the output of the
 * reduction will be reported on.
 * 
 * The number of maps is specified by "slive.maps".
 * The number of reduces is specified by "slive.reduces".
 */
public class SliveTest implements Tool {

  private static final Log LOG = LogFactory.getLog(SliveTest.class);

  // ensures the hdfs configurations are loaded if they exist
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  private Configuration base;

  public SliveTest(Configuration base) {
    this.base = base;
  }

  public int run(String[] args) {
    ParsedOutput parsedOpts = null;
    try {
      ArgumentParser argHolder = new ArgumentParser(args);
      parsedOpts = argHolder.parse();
      if (parsedOpts.shouldOutputHelp()) {
        parsedOpts.outputHelp();
        return 1;
      }
    } catch (Exception e) {
      LOG.error("Unable to parse arguments due to error: ", e);
      return 1;
    }
    LOG.info("Running with option list " + Helper.stringifyArray(args, " "));
    ConfigExtractor config = null;
    try {
      ConfigMerger cfgMerger = new ConfigMerger();
      Configuration cfg = cfgMerger.getMerged(parsedOpts,
                                              new Configuration(base));
      if (cfg != null) {
        config = new ConfigExtractor(cfg);
      }
    } catch (Exception e) {
      LOG.error("Unable to merge config due to error: ", e);
      return 1;
    }
    if (config == null) {
      LOG.error("Unable to merge config & options!");
      return 1;
    }
    try {
      LOG.info("Options are:");
      ConfigExtractor.dumpOptions(config);
    } catch (Exception e) {
      LOG.error("Unable to dump options due to error: ", e);
      return 1;
    }
    boolean jobOk = false;
    try {
      LOG.info("Running job:");
      runJob(config);
      jobOk = true;
    } catch (Exception e) {
      LOG.error("Unable to run job due to error: ", e);
    }
    if (jobOk) {
      try {
        LOG.info("Reporting on job:");
        writeReport(config);
      } catch (Exception e) {
        LOG.error("Unable to report on job due to error: ", e);
      }
    }
    // attempt cleanup (not critical)
    boolean cleanUp = getBool(parsedOpts
        .getValue(ConfigOption.CLEANUP.getOpt()));
    if (cleanUp) {
      try {
        LOG.info("Cleaning up job:");
        cleanup(config);
      } catch (Exception e) {
        LOG.error("Unable to cleanup job due to error: ", e);
      }
    }
    // all mostly worked
    if (jobOk) {
      return 0;
    }
    // maybe didn't work
    return 1;
  }

  /**
   * Checks if a string is a boolean or not and what type
   * 
   * @param val
   *          val to check
   * @return boolean
   */
  private boolean getBool(String val) {
    if (val == null) {
      return false;
    }
    String cleanupOpt = val.toLowerCase().trim();
    if (cleanupOpt.equals("true") || cleanupOpt.equals("1")) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Sets up a job conf for the given job using the given config object. Ensures
   * that the correct input format is set, the mapper and and reducer class and
   * the input and output keys and value classes along with any other job
   * configuration.
   * 
   * @param config
   * @return JobConf representing the job to be ran
   * @throws IOException
   */
  private JobConf getJob(ConfigExtractor config) throws IOException {
    JobConf job = new JobConf(config.getConfig(), SliveTest.class);
    job.setInputFormat(DummyInputFormat.class);
    FileOutputFormat.setOutputPath(job, config.getOutputPath());
    job.setMapperClass(SliveMapper.class);
    job.setPartitionerClass(SlivePartitioner.class);
    job.setReducerClass(SliveReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setCompressOutput(job, false);
    job.setNumReduceTasks(config.getReducerAmount());
    job.setNumMapTasks(config.getMapAmount());
    return job;
  }

  /**
   * Runs the job given the provided config
   * 
   * @param config
   *          the config to run the job with
   * 
   * @throws IOException
   *           if can not run the given job
   */
  private void runJob(ConfigExtractor config) throws IOException {
    JobClient.runJob(getJob(config));
  }

  /**
   * Attempts to write the report to the given output using the specified
   * config. It will open up the expected reducer output file and read in its
   * contents and then split up by operation output and sort by operation type
   * and then for each operation type it will generate a report to the specified
   * result file and the console.
   * 
   * @param cfg
   *          the config specifying the files and output
   * 
   * @throws Exception
   *           if files can not be opened/closed/read or invalid format
   */
  private void writeReport(ConfigExtractor cfg) throws Exception {
    Path dn = cfg.getOutputPath();
    LOG.info("Writing report using contents of " + dn);
    FileSystem fs = dn.getFileSystem(cfg.getConfig());
    FileStatus[] reduceFiles = fs.listStatus(dn);
    BufferedReader fileReader = null;
    PrintWriter reportWriter = null;
    try {
      List<OperationOutput> noOperations = new ArrayList<OperationOutput>();
      Map<String, List<OperationOutput>> splitTypes = new TreeMap<String, List<OperationOutput>>();
      for(FileStatus fn : reduceFiles) {
        if(!fn.getPath().getName().startsWith("part")) continue;
        fileReader = new BufferedReader(new InputStreamReader(
            new DataInputStream(fs.open(fn.getPath()))));
        String line;
        while ((line = fileReader.readLine()) != null) {
          String pieces[] = line.split("\t", 2);
          if (pieces.length == 2) {
            OperationOutput data = new OperationOutput(pieces[0], pieces[1]);
            String op = (data.getOperationType());
            if (op != null) {
              List<OperationOutput> opList = splitTypes.get(op);
              if (opList == null) {
                opList = new ArrayList<OperationOutput>();
              }
              opList.add(data);
              splitTypes.put(op, opList);
            } else {
              noOperations.add(data);
            }
          } else {
            throw new IOException("Unparseable line " + line);
          }
        }
        fileReader.close();
        fileReader = null;
      }
      File resFile = null;
      if (cfg.getResultFile() != null) {
        resFile = new File(cfg.getResultFile());
      }
      if (resFile != null) {
        LOG.info("Report results being placed to logging output and to file "
            + resFile.getCanonicalPath());
        reportWriter = new PrintWriter(new FileOutputStream(resFile));
      } else {
        LOG.info("Report results being placed to logging output");
      }
      ReportWriter reporter = new ReportWriter();
      if (!noOperations.isEmpty()) {
        reporter.basicReport(noOperations, reportWriter);
      }
      for (String opType : splitTypes.keySet()) {
        reporter.opReport(opType, splitTypes.get(opType), reportWriter);
      }
    } finally {
      if (fileReader != null) {
        fileReader.close();
      }
      if (reportWriter != null) {
        reportWriter.close();
      }
    }
  }

  /**
   * Cleans up the base directory by removing it
   * 
   * @param cfg
   *          ConfigExtractor which has location of base directory
   * 
   * @throws IOException
   */
  private void cleanup(ConfigExtractor cfg) throws IOException {
    Path base = cfg.getBaseDirectory();
    if (base != null) {
      LOG.info("Attempting to recursively delete " + base);
      FileSystem fs = base.getFileSystem(cfg.getConfig());
      fs.delete(base, true);
    }
  }

  /**
   * The main program entry point. Sets up and parses the command line options,
   * then merges those options and then dumps those options and the runs the
   * corresponding map/reduce job that those operations represent and then
   * writes the report for the output of the run that occurred.
   * 
   * @param args
   *          command line options
   */
  public static void main(String[] args) throws Exception {
    Configuration startCfg = new Configuration(true);
    SliveTest runner = new SliveTest(startCfg);
    int ec = ToolRunner.run(runner, args);
    System.exit(ec);
  }

  @Override // Configurable
  public Configuration getConf() {
    return this.base;
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    this.base = conf;
  }
}
