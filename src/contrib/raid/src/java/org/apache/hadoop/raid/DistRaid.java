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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.raid.RaidNode.Statistics;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;

public class DistRaid {

  protected static final Log LOG = LogFactory.getLog(DistRaid.class);

  static final String NAME = "distRaid";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String OP_LIST_LABEL = NAME + ".op.list";
  static final String OP_COUNT_LABEL = NAME + ".op.count";
  static final int   OP_LIST_BLOCK_SIZE = 32 * 1024 * 1024; // block size of control file
  static final short OP_LIST_REPLICATION = 10; // replication factor of control file

  private static final long OP_PER_MAP = 100;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int SYNC_FILE_MAX = 10;
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static String jobName = NAME;

  static enum Counter {
    FILES_SUCCEEDED, FILES_FAILED, PROCESSED_BLOCKS, PROCESSED_SIZE, META_BLOCKS, META_SIZE
  }

  protected JobConf jobconf;

  /** {@inheritDoc} */
  public void setConf(Configuration conf) {
    if (jobconf != conf) {
      jobconf = conf instanceof JobConf ? (JobConf) conf : new JobConf(conf);
    }
  }

  /** {@inheritDoc} */
  public JobConf getConf() {
    return jobconf;
  }

  public DistRaid(Configuration conf) {
    setConf(createJobConf(conf));
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

  /** Responsible for generating splits of the src file list. */
  static class DistRaidInputFormat implements InputFormat<Text, PolicyInfo> {
    /** Do nothing. */
    public void validateInput(JobConf job) {
    }

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * 
     * @param job
     *          The handle to the JobConf object
     * @param numSplits
     *          Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      final int srcCount = job.getInt(OP_COUNT_LABEL, -1);
      final int targetcount = srcCount / numSplits;
      String srclist = job.get(OP_LIST_LABEL, "");
      if (srcCount < 0 || "".equals(srclist)) {
        throw new RuntimeException("Invalid metadata: #files(" + srcCount
            + ") listuri(" + srclist + ")");
      }
      Path srcs = new Path(srclist);
      FileSystem fs = srcs.getFileSystem(job);

      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

      Text key = new Text();
      PolicyInfo value = new PolicyInfo();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0; // count src
      try {
        for (in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value);) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > targetcount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[]) null));
            prev = curr;
          }
        }
      } finally {
        in.close();
      }
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[]) null));
      }
      LOG.info("jobname= " + jobName + " numSplits=" + numSplits + 
               ", splits.size()=" + splits.size());
      return splits.toArray(new FileSplit[splits.size()]);
    }

    /** {@inheritDoc} */
    public RecordReader<Text, PolicyInfo> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, PolicyInfo>(job,
          (FileSplit) split);
    }
  }

  /** The mapper for raiding files. */
  static class DistRaidMapper implements
      Mapper<Text, PolicyInfo, WritableComparable, Text> {
    private JobConf jobconf;
    private boolean ignoreFailures;

    private int failcount = 0;
    private int succeedcount = 0;
    private Statistics st = null;

    private String getCountString() {
      return "Succeeded: " + succeedcount + " Failed: " + failcount;
    }

    /** {@inheritDoc} */
    public void configure(JobConf job) {
      this.jobconf = job;
      ignoreFailures = false;
      st = new Statistics();
    }

    /** Run a FileOperation */
    public void map(Text key, PolicyInfo policy,
        OutputCollector<WritableComparable, Text> out, Reporter reporter)
        throws IOException {
      try {
        LOG.info("Raiding file=" + key.toString() + " policy=" + policy);
        Path p = new Path(key.toString());
        FileStatus fs = p.getFileSystem(jobconf).getFileStatus(p);
        st.clear();
        RaidNode.doRaid(jobconf, policy, fs, st, reporter);

        ++succeedcount;

        reporter.incrCounter(Counter.PROCESSED_BLOCKS, st.numProcessedBlocks);
        reporter.incrCounter(Counter.PROCESSED_SIZE, st.processedSize);
        reporter.incrCounter(Counter.META_BLOCKS, st.numMetaBlocks);
        reporter.incrCounter(Counter.META_SIZE, st.metaSize);

        reporter.incrCounter(Counter.FILES_SUCCEEDED, 1);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FILES_FAILED, 1);

        String s = "FAIL: " + policy + ", " + key + " "
            + StringUtils.stringifyException(e);
        out.collect(null, new Text(s));
        LOG.info(s);
      } finally {
        reporter.setStatus(getCountString());
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
   * create new job conf based on configuration passed.
   * 
   * @param conf
   * @return
   */
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistRaid.class);
    jobName = NAME + " " + dateForm.format(new Date(RaidNode.now()));
    jobconf.setJobName(jobName);
    jobconf.setMapSpeculativeExecution(false);

    jobconf.setJarByClass(DistRaid.class);
    jobconf.setInputFormat(DistRaidInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(DistRaidMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  /** Add paths to be raided */
  public void addRaidPaths(PolicyInfo info, List<FileStatus> paths) {
    raidPolicyPathPairList.add(new RaidPolicyPathPair(info, paths));
  }

  /** Calculate how many maps to run. */
  private static int getMapCount(int srcCount, int numNodes) {
    int numMaps = (int) (srcCount / OP_PER_MAP);
    numMaps = Math.min(numMaps, numNodes * MAX_MAPS_PER_NODE);
    return Math.max(numMaps, 1);
  }

  /** invokes mapred job do parallel raiding */
  public void doDistRaid() throws IOException {
    if (raidPolicyPathPairList.size() == 0) {
      LOG.info("DistRaid has no paths to raid.");
      return;
    }
    try {
      if (setup()) {
        JobClient.runJob(jobconf);
      }
    } finally {
      // delete job directory
      final String jobdir = jobconf.get(JOB_DIR_LABEL);
      if (jobdir != null) {
        final Path jobpath = new Path(jobdir);
        jobpath.getFileSystem(jobconf).delete(jobpath, true);
      }
    }
    raidPolicyPathPairList.clear();
  }

  /**
   * set up input file which has the list of input files.
   * 
   * @return boolean
   * @throws IOException
   */
  private boolean setup() throws IOException {
    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobconf);
    Path jobdir = new Path(jClient.getSystemDir(), NAME + "_" + randomId);

    LOG.info(JOB_DIR_LABEL + "=" + jobdir);
    jobconf.set(JOB_DIR_LABEL, jobdir.toString());
    Path log = new Path(jobdir, "_logs");

    // The control file should have small size blocks. This helps
    // in spreading out the load from mappers that will be spawned.
    jobconf.setInt("dfs.blocks.size",  OP_LIST_BLOCK_SIZE);

    FileOutputFormat.setOutputPath(jobconf, log);
    LOG.info("log=" + log);

    // create operation list
    FileSystem fs = jobdir.getFileSystem(jobconf);
    Path opList = new Path(jobdir, "_" + OP_LIST_LABEL);
    jobconf.set(OP_LIST_LABEL, opList.toString());
    int opCount = 0, synCount = 0;
    SequenceFile.Writer opWriter = null;
    try {
      opWriter = SequenceFile.createWriter(fs, jobconf, opList, Text.class,
          PolicyInfo.class, SequenceFile.CompressionType.NONE);
      for (RaidPolicyPathPair p : raidPolicyPathPairList) {
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
      fs.setReplication(opList, OP_LIST_REPLICATION); // increase replication for control file
    }
    raidPolicyPathPairList.clear();
    
    jobconf.setInt(OP_COUNT_LABEL, opCount);
    LOG.info("Number of files=" + opCount);
    jobconf.setNumMapTasks(getMapCount(opCount, new JobClient(jobconf)
        .getClusterStatus().getTaskTrackers()));
    LOG.info("jobName= " + jobName + " numMapTasks=" + jobconf.getNumMapTasks());
    return opCount != 0;

  }
}
