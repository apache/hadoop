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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.raid.RaidNode.Statistics;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;

public class DistRaid extends Configured {

  protected static final Log LOG = LogFactory.getLog(DistRaid.class);

  static final String NAME = "distRaid";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final int   OP_LIST_BLOCK_SIZE = 32 * 1024 * 1024; // block size of control file
  static final short OP_LIST_REPLICATION = 10; // replication factor of control file

  public static final String OPS_PER_TASK = "raid.distraid.opspertask";
  private static final int DEFAULT_OPS_PER_TASK = 100;
  private static final int SYNC_FILE_MAX = 10;
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  static enum Counter {
    FILES_SUCCEEDED, FILES_FAILED, PROCESSED_BLOCKS, PROCESSED_SIZE, META_BLOCKS, META_SIZE
  }

  public DistRaid(Configuration conf) {
    super(conf);
  }

  private static final Random RANDOM = new Random();

  protected static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  /**
   *
   * helper class which holds the policy and paths
   *
   */
  public static class RaidPolicyPathPair {
    public PolicyInfo policy;
    public List<FileStatus> srcPaths;

    RaidPolicyPathPair(PolicyInfo policy, List<FileStatus> srcPaths) {
      this.policy = policy;
      this.srcPaths = srcPaths;
    }
  }

  List<RaidPolicyPathPair> raidPolicyPathPairList = new ArrayList<RaidPolicyPathPair>();

  private Job runningJob;
  private String lastReport = null;

  /** Responsible for generating splits of the src file list. */
  static class DistRaidInputFormat extends 
		  SequenceFileInputFormat<Text, PolicyInfo> {
    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     *
     * @param job
     *          The handle to the Configuration object
     * @param numSplits
     *          Number of splits requested
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      Configuration conf = job.getConfiguration();

      // We create only one input file. So just get the first file in the first
      // input directory.
      Path inDir = getInputPaths(job)[0];
      FileSystem fs = inDir.getFileSystem(conf);
      FileStatus[] inputFiles = fs.listStatus(inDir);
      Path inputFile = inputFiles[0].getPath();

      List<InputSplit> splits = new ArrayList<InputSplit>();
      SequenceFile.Reader in =
        new SequenceFile.Reader(conf, Reader.file(inputFile));
      long prev = 0L;
      final int opsPerTask = conf.getInt(OPS_PER_TASK, DEFAULT_OPS_PER_TASK);
      try {
        Text key = new Text();
        PolicyInfo value = new PolicyInfo();
        int count = 0; // count src
        while (in.next(key, value)) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > opsPerTask) {
            count = 0;
            splits.add(new FileSplit(inputFile, prev, delta, (String[]) null));
            prev = curr;
          }
        }
      } finally {
        in.close();
      }
      long remaining = fs.getFileStatus(inputFile).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(inputFile, prev, remaining, (String[]) null));
      }
      return splits;
    }
  }

  /** The mapper for raiding files. */
  static class DistRaidMapper extends Mapper<Text, PolicyInfo, Text, Text> {
    private boolean ignoreFailures = false;

    private int failcount = 0;
    private int succeedcount = 0;
    private Statistics st = new Statistics();

    private String getCountString() {
      return "Succeeded: " + succeedcount + " Failed: " + failcount;
    }

    /** Run a FileOperation
     * @throws IOException
     * @throws InterruptedException */
    public void map(Text key, PolicyInfo policy, Context context)
        throws IOException, InterruptedException {
      try {
        Configuration jobConf = context.getConfiguration();
        LOG.info("Raiding file=" + key.toString() + " policy=" + policy);
        Path p = new Path(key.toString());
        FileStatus fs = p.getFileSystem(jobConf).getFileStatus(p);
        st.clear();
        RaidNode.doRaid(jobConf, policy, fs, st, context);

        ++succeedcount;

        context.getCounter(Counter.PROCESSED_BLOCKS).increment(st.numProcessedBlocks);
        context.getCounter(Counter.PROCESSED_SIZE).increment(st.processedSize);
        context.getCounter(Counter.META_BLOCKS).increment(st.numMetaBlocks);
        context.getCounter(Counter.META_SIZE).increment(st.metaSize);
        context.getCounter(Counter.FILES_SUCCEEDED).increment(1);
      } catch (IOException e) {
        ++failcount;
        context.getCounter(Counter.FILES_FAILED).increment(1);

        String s = "FAIL: " + policy + ", " + key + " "
            + StringUtils.stringifyException(e);
        context.write(new Text(key), new Text(s));
        LOG.error(s);
      } finally {
        context.setStatus(getCountString());
      }
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      if (failcount == 0 || ignoreFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  /**
   * Set options specified in raid.scheduleroption.
   * The string should be formatted as key:value[,key:value]*
   */
  static void setSchedulerOption(Configuration conf) {
    String schedulerOption = conf.get("raid.scheduleroption");
    if (schedulerOption != null) {
      // Parse the scheduler option to get key:value pairs.
      String[] keyValues = schedulerOption.trim().split(",");
      for (String keyValue: keyValues) {
        String[] fields = keyValue.trim().split(":");
        String key = fields[0].trim();
        String value = fields[1].trim();
        conf.set(key, value);
      }
    }
  }

  /**
   * Creates a new Job object.
   * @param conf
   * @return a Job object
   * @throws IOException
   */
  static Job createJob(Configuration jobConf) throws IOException {
    String jobName = NAME + " " + dateForm.format(new Date(RaidNode.now()));

    setSchedulerOption(jobConf);

    Job job = Job.getInstance(jobConf, jobName);
    job.setSpeculativeExecution(false);
    job.setJarByClass(DistRaid.class);
    job.setInputFormatClass(DistRaidInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(DistRaidMapper.class);
    job.setNumReduceTasks(0);

    return job;
  }

  /** Add paths to be raided */
  public void addRaidPaths(PolicyInfo info, List<FileStatus> paths) {
    raidPolicyPathPairList.add(new RaidPolicyPathPair(info, paths));
  }

  /** Invokes a map-reduce job do parallel raiding.
   *  @return true if the job was started, false otherwise
   * @throws InterruptedException
   */
  public boolean startDistRaid() throws IOException {
    assert(raidPolicyPathPairList.size() > 0);
    Job job = createJob(getConf());
    createInputFile(job);
    try {
      job.submit();
      this.runningJob = job;
      LOG.info("Job Started: " + runningJob.getJobID());
      return true;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      return false;
    }
  }

   /** Checks if the map-reduce job has completed.
    *
    * @return true if the job completed, false otherwise.
    * @throws IOException
    */
   public boolean checkComplete() throws IOException {
     JobID jobID = runningJob.getJobID();
     try {
      if (runningJob.isComplete()) {
         // delete job directory
         Configuration jobConf = runningJob.getConfiguration();
         final String jobdir = jobConf.get(JOB_DIR_LABEL);
         if (jobdir != null) {
           final Path jobpath = new Path(jobdir);
           jobpath.getFileSystem(jobConf).delete(jobpath, true);
         }
         if (runningJob.isSuccessful()) {
           LOG.info("Job Complete(Succeeded): " + jobID);
         } else {
           LOG.error("Job Complete(Failed): " + jobID);
         }
         raidPolicyPathPairList.clear();
         return true;
       } else {
         String report =  (" job " + jobID +
           " map " + StringUtils.formatPercent(runningJob.mapProgress(), 0)+
           " reduce " + StringUtils.formatPercent(runningJob.reduceProgress(), 0));
         if (!report.equals(lastReport)) {
           LOG.info(report);
           lastReport = report;
         }
         return false;
       }
    } catch (InterruptedException e) {
      return false;
    }
   }

   public boolean successful() throws IOException {
     try {
      return runningJob.isSuccessful();
    } catch (InterruptedException e) {
      return false;
    }
   }

  /**
   * set up input file which has the list of input files.
   *
   * @return boolean
   * @throws IOException
   */
  private void createInputFile(Job job) throws IOException {
    Configuration jobConf = job.getConfiguration();
    Path jobDir = new Path(JOB_DIR_LABEL + getRandomId());
    Path inDir = new Path(jobDir, "in");
    Path outDir = new Path(jobDir, "out");
    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);
    Path opList = new Path(inDir, NAME);

    Configuration tmp = new Configuration(jobConf);
    // The control file should have small size blocks. This helps
    // in spreading out the load from mappers that will be spawned.
    tmp.setInt("dfs.blocks.size",  OP_LIST_BLOCK_SIZE);
    FileSystem fs = opList.getFileSystem(tmp);

    int opCount = 0, synCount = 0;
    SequenceFile.Writer opWriter = null;
    try {
      opWriter = SequenceFile.createWriter(
          jobConf, Writer.file(opList), Writer.keyClass(Text.class),
          Writer.valueClass(PolicyInfo.class),
          Writer.compression(SequenceFile.CompressionType.NONE));
      for (RaidPolicyPathPair p : raidPolicyPathPairList) {
        // If a large set of files are Raided for the first time, files
        // in the same directory that tend to have the same size will end up
        // with the same map. This shuffle mixes things up, allowing a better
        // mix of files.
        java.util.Collections.shuffle(p.srcPaths);
        for (FileStatus st : p.srcPaths) {
          opWriter.append(new Text(st.getPath().toString()), p.policy);
          opCount++;
          if (++synCount > SYNC_FILE_MAX) {
            opWriter.sync();
            synCount = 0;
          }
        }
      }

    } finally {
      if (opWriter != null) {
        opWriter.close();
      }
      // increase replication for control file
      fs.setReplication(opList, OP_LIST_REPLICATION);
    }
    raidPolicyPathPairList.clear();
    LOG.info("Number of files=" + opCount);
  }
}
