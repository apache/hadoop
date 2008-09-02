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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class ExecDriver extends Task<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected JobConf job;

  /**
   * Constructor when invoked from QL
   */
  public ExecDriver() {
    super();
  }

  /**
   * Initialization when invoked from QL
   */
  public void initialize (HiveConf conf) {
    super.initialize(conf);
    job = new JobConf(conf, ExecDriver.class);
  }

  /**
   * Constructor/Initialization for invocation as independent utility
   */
  public ExecDriver(mapredWork plan, JobConf job, boolean isSilent) {
    setWork(plan);
    this.job = job;
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG, isSilent);    
  }

  protected void fillInDefaults() {
    // this is a temporary hack to fix things that are not fixed in the compiler
    if(work.getNumReduceTasks() == null) {
      if(work.getReducer() == null) {
        LOG.warn("Number of reduce tasks not specified. Defaulting to 0 since there's no reduce operator");
        work.setNumReduceTasks(Integer.valueOf(0));
      } else {
        LOG.warn("Number of reduce tasks not specified. Defaulting to jobconf value of: " + job.getNumReduceTasks());
        work.setNumReduceTasks(job.getNumReduceTasks());
      }
    } 
  }

  /**
   * from StreamJob.java
   */
  public void jobInfo(RunningJob rj) {
    if (job.get("mapred.job.tracker", "local").equals("local")) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      String hp = job.get("mapred.job.tracker");
      console.printInfo("Starting Job = " + rj.getJobID() + ", Tracking URL = " + rj.getTrackingURL());
      console.printInfo("Kill Command = " +
                  HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN) +
                  " job  -Dmapred.job.tracker=" + hp + " -kill "
                  + rj.getJobID());
    }
  }

  /**
   * from StreamJob.java
   */
  public RunningJob jobProgress(JobClient jc, RunningJob rj)
    throws IOException {
    String lastReport = "";
    while (!rj.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      rj = jc.getJob(rj.getJobID());
      String report = null;
      report = " map = " + Math.round(rj.mapProgress() * 100) + "%,  reduce ="
        + Math.round(rj.reduceProgress() * 100) + "%";
      
      if (!report.equals(lastReport)) {
        console.printInfo(report);
        lastReport = report;
      }
    }
    return rj;
  }

  /**
   * Execute a query plan using Hadoop
   */
  public int execute() {

    fillInDefaults();

    String invalidReason = work.isInvalid();
    if(invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: "+invalidReason);
    }

    Utilities.setMapRedWork(job, work);
    
    for(String onefile: work.getPathToAliases().keySet()) {
      LOG.info("Adding input file " + onefile);
      FileInputFormat.addInputPaths(job, onefile);
    }
    
    String hiveScratchDir = HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR);
    String jobScratchDir = hiveScratchDir + Utilities.randGen.nextInt();
    FileOutputFormat.setOutputPath(job, new Path(jobScratchDir));
    job.setMapperClass(ExecMapper.class);

    if(!work.getNeedsTagging()) {
      job.setMapOutputValueClass(NoTagWritableHiveObject.class);
      job.setMapOutputKeyClass(NoTagWritableComparableHiveObject.class);    
      job.setOutputKeyComparatorClass(NoTagHiveObjectComparator.class);
    } else {
      job.setMapOutputValueClass(WritableHiveObject.class);
      job.setMapOutputKeyClass(WritableComparableHiveObject.class);    
      job.setOutputKeyComparatorClass(HiveObjectComparator.class);
    }

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    job.setInputFormat(org.apache.hadoop.hive.ql.io.HiveInputFormat.class);

    // No-Op - we don't really write anything here .. 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    if (StringUtils.isNotBlank(auxJars)) {
      LOG.info("adding libjars: " + auxJars);
      job.set("tmpjars", auxJars);
    }

    int returnVal = 0;
    FileSystem fs = null;
    RunningJob rj = null;

    try {
      fs = FileSystem.get(job);
      JobClient jc = new JobClient(job);
      rj = jc.submitJob(job);

      jobInfo(rj);
      rj = jobProgress(jc, rj);
      
      String statusMesg = "Ended Job = " + rj.getJobID();
      if(!rj.isSuccessful()) {
        statusMesg += " with errors";
        returnVal = 2;
        console.printError(statusMesg);
      } else {
        console.printInfo(statusMesg);
      }
    } catch (Exception e) {
      String mesg = " with exception '" + e.getMessage() + "'";
      if(rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }
      // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
      console.printError(mesg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));

      returnVal = 1;
    } finally {
      Utilities.clearMapRedWork(job);
      try {
        fs.delete(new Path(jobScratchDir), true);
        if(returnVal != 0 && rj != null) {
          rj.killJob();
        }
      } catch (Exception e) {}
    }
    return (returnVal);
  }

  private static void printUsage() {
    System.out.println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...]");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, HiveException {
    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String> ();
    boolean isSilent = false;

    try{
      for(int i=0; i<args.length; i++) {
        if(args[i].equals("-plan")) {
          planFileName = args[++i];
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-silent")) {
          isSilent = true;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    if(planFileName == null) {
      System.err.println("Must specify Plan File Name");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    for(String one: jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if(eqIndex != -1) {
        try {
          conf.set(one.substring(0, eqIndex),
                   URLDecoder.decode(one.substring(eqIndex+1), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          System.err.println("Unexpected error " + e.getMessage() + " while encoding " +
                             one.substring(eqIndex+1));
          System.exit(3);
        }
      }
    }

    URI pathURI = (new Path(planFileName)).toUri();
    InputStream pathData;
    if(StringUtils.isEmpty(pathURI.getScheme())) {
      // default to local file system
      pathData = new FileInputStream(planFileName);
    } else {
      // otherwise may be in hadoop ..
      FileSystem fs = FileSystem.get(conf);
      pathData = fs.open(new Path(planFileName));
    }
    
    mapredWork plan = Utilities.deserializeMapRedWork(pathData);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);
    int ret = ed.execute();
    if(ret != 0) {
      System.out.println("Job Failed");
      System.exit(2);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line
   * fragment for passing such configuration information to ExecDriver
   */
  public static String generateCmdLine(HiveConf hconf) {
    StringBuilder sb = new StringBuilder ();
    Properties deltaP = hconf.getChangedProperties();

    for(Object one: deltaP.keySet()) {
      String oneProp = (String)one;
      String oneValue = deltaP.getProperty(oneProp);

      sb.append("-jobconf ");
      sb.append(oneProp);
      sb.append("=");
      try {
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      sb.append(" ");
    }
    return sb.toString();
  }
}

