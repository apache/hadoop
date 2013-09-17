/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.util.ToolRunner;

public class CommonJobTest {
  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  protected static int NJOBS = 2;
  protected static final long GENDATA = 1; // in megabytes
  protected static GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.REPLAY;
  private static File workspace = new File("target" + File.separator
          + TestGridmixSubmission.class.getName() + "-test");

  static class DebugGridmix extends Gridmix {

    private JobFactory<?> factory;
    private TestMonitor monitor;

    @Override
    protected JobMonitor createJobMonitor(Statistics stats, Configuration conf)
            throws IOException {
      monitor = new TestMonitor(3, stats);
      return monitor;
    }

    @Override
    protected JobFactory<?> createJobFactory(JobSubmitter submitter,
                                             String traceIn, Path scratchDir, Configuration conf,
                                             CountDownLatch startFlag, UserResolver userResolver) throws IOException {
      factory = DebugJobFactory.getFactory(submitter, scratchDir, NJOBS, conf,
              startFlag, userResolver);
      return factory;
    }

    public void checkMonitor() throws Exception {
      monitor.verify(((DebugJobFactory.Debuggable) factory).getSubmitted());
    }
  }

  static class TestMonitor extends JobMonitor {
    private final BlockingQueue<Job> retiredJobs;
    private final int expected;
    static final long SLOPBYTES = 1024;

    public TestMonitor(int expected, Statistics stats) {
      super(3, TimeUnit.SECONDS, stats, 1);
      this.expected = expected;
      retiredJobs = new LinkedBlockingQueue<Job>();
    }

    @Override
    protected void onSuccess(Job job) {
      LOG.info(" Job Success " + job);
      retiredJobs.add(job);
    }

    @Override
    protected void onFailure(Job job) {
      fail("Job failure: " + job);
    }

    public void verify(ArrayList<JobStory> submitted) throws Exception {
      assertEquals("Bad job count", expected, retiredJobs.size());

      final ArrayList<Job> succeeded = new ArrayList<Job>();
      assertEquals("Bad job count", expected, retiredJobs.drainTo(succeeded));
      final HashMap<String, JobStory> sub = new HashMap<String, JobStory>();
      for (JobStory spec : submitted) {
        sub.put(spec.getJobID().toString(), spec);
      }
      for (Job job : succeeded) {
        final String jobName = job.getJobName();
        Configuration configuration = job.getConfiguration();
        if (GenerateData.JOB_NAME.equals(jobName)) {
          RemoteIterator<LocatedFileStatus> rit = GridmixTestUtils.dfs
                  .listFiles(new Path("/"), true);
          while (rit.hasNext()) {
            System.out.println(rit.next().toString());
          }
          final Path in = new Path("foo").makeQualified(
                  GridmixTestUtils.dfs.getUri(),
                  GridmixTestUtils.dfs.getWorkingDirectory());
          // data was compressed. All files = compressed test size+ logs= 1000000/2 + logs
          final ContentSummary generated = GridmixTestUtils.dfs
                  .getContentSummary(in);
          assertEquals(550000, generated.getLength(), 10000);

          Counter counter = job.getCounters()
                  .getGroup("org.apache.hadoop.mapreduce.FileSystemCounter")
                  .findCounter("HDFS_BYTES_WRITTEN");

          assertEquals(generated.getLength(), counter.getValue());

          continue;
        } else if (GenerateDistCacheData.JOB_NAME.equals(jobName)) {
          continue;
        }

        final String originalJobId = configuration.get(Gridmix.ORIGINAL_JOB_ID);
        final JobStory spec = sub.get(originalJobId);
        assertNotNull("No spec for " + jobName, spec);
        assertNotNull("No counters for " + jobName, job.getCounters());
        final String originalJobName = spec.getName();
        System.out.println("originalJobName=" + originalJobName
                + ";GridmixJobName=" + jobName + ";originalJobID=" + originalJobId);
        assertTrue("Original job name is wrong.",
                originalJobName.equals(configuration.get(Gridmix.ORIGINAL_JOB_NAME)));

        // Gridmix job seqNum contains 6 digits
        int seqNumLength = 6;
        String jobSeqNum = new DecimalFormat("000000").format(configuration.getInt(
                GridmixJob.GRIDMIX_JOB_SEQ, -1));
        // Original job name is of the format MOCKJOB<6 digit sequence number>
        // because MockJob jobNames are of this format.
        assertTrue(originalJobName.substring(
                originalJobName.length() - seqNumLength).equals(jobSeqNum));

        assertTrue("Gridmix job name is not in the expected format.",
                jobName.equals(GridmixJob.JOB_NAME_PREFIX + jobSeqNum));
        final FileStatus stat = GridmixTestUtils.dfs.getFileStatus(new Path(
                GridmixTestUtils.DEST, "" + Integer.valueOf(jobSeqNum)));
        assertEquals("Wrong owner for " + jobName, spec.getUser(),
                stat.getOwner());
        final int nMaps = spec.getNumberMaps();
        final int nReds = spec.getNumberReduces();

        final JobClient client = new JobClient(
                GridmixTestUtils.mrvl.getConfig());
        final TaskReport[] mReports = client.getMapTaskReports(JobID
                .downgrade(job.getJobID()));
        assertEquals("Mismatched map count", nMaps, mReports.length);
        check(TaskType.MAP, spec, mReports, 0, 0, SLOPBYTES, nReds);

        final TaskReport[] rReports = client.getReduceTaskReports(JobID
                .downgrade(job.getJobID()));
        assertEquals("Mismatched reduce count", nReds, rReports.length);
        check(TaskType.REDUCE, spec, rReports, nMaps * SLOPBYTES, 2 * nMaps, 0,
                0);

      }

    }
    // Verify if correct job queue is used
    private void check(final TaskType type, JobStory spec,
                       final TaskReport[] runTasks, long extraInputBytes,
                       int extraInputRecords, long extraOutputBytes, int extraOutputRecords)
            throws Exception {

      long[] runInputRecords = new long[runTasks.length];
      long[] runInputBytes = new long[runTasks.length];
      long[] runOutputRecords = new long[runTasks.length];
      long[] runOutputBytes = new long[runTasks.length];
      long[] specInputRecords = new long[runTasks.length];
      long[] specInputBytes = new long[runTasks.length];
      long[] specOutputRecords = new long[runTasks.length];
      long[] specOutputBytes = new long[runTasks.length];

      for (int i = 0; i < runTasks.length; ++i) {
        final TaskInfo specInfo;
        final Counters counters = runTasks[i].getCounters();
        switch (type) {
          case MAP:
            runInputBytes[i] = counters.findCounter("FileSystemCounters",
                    "HDFS_BYTES_READ").getValue()
                    - counters.findCounter(TaskCounter.SPLIT_RAW_BYTES).getValue();
            runInputRecords[i] = (int) counters.findCounter(
                    TaskCounter.MAP_INPUT_RECORDS).getValue();
            runOutputBytes[i] = counters
                    .findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
            runOutputRecords[i] = (int) counters.findCounter(
                    TaskCounter.MAP_OUTPUT_RECORDS).getValue();

            specInfo = spec.getTaskInfo(TaskType.MAP, i);
            specInputRecords[i] = specInfo.getInputRecords();
            specInputBytes[i] = specInfo.getInputBytes();
            specOutputRecords[i] = specInfo.getOutputRecords();
            specOutputBytes[i] = specInfo.getOutputBytes();

            LOG.info(String.format(type + " SPEC: %9d -> %9d :: %5d -> %5d\n",
                    specInputBytes[i], specOutputBytes[i], specInputRecords[i],
                    specOutputRecords[i]));
            LOG.info(String.format(type + " RUN:  %9d -> %9d :: %5d -> %5d\n",
                    runInputBytes[i], runOutputBytes[i], runInputRecords[i],
                    runOutputRecords[i]));
            break;
          case REDUCE:
            runInputBytes[i] = 0;
            runInputRecords[i] = (int) counters.findCounter(
                    TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            runOutputBytes[i] = counters.findCounter("FileSystemCounters",
                    "HDFS_BYTES_WRITTEN").getValue();
            runOutputRecords[i] = (int) counters.findCounter(
                    TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

            specInfo = spec.getTaskInfo(TaskType.REDUCE, i);
            // There is no reliable counter for reduce input bytes. The
            // variable-length encoding of intermediate records and other noise
            // make this quantity difficult to estimate. The shuffle and spec
            // input bytes are included in debug output for reference, but are
            // not checked
            specInputBytes[i] = 0;
            specInputRecords[i] = specInfo.getInputRecords();
            specOutputRecords[i] = specInfo.getOutputRecords();
            specOutputBytes[i] = specInfo.getOutputBytes();
            LOG.info(String.format(type + " SPEC: (%9d) -> %9d :: %5d -> %5d\n",
                    specInfo.getInputBytes(), specOutputBytes[i],
                    specInputRecords[i], specOutputRecords[i]));
            LOG.info(String
                    .format(type + " RUN:  (%9d) -> %9d :: %5d -> %5d\n", counters
                            .findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue(),
                            runOutputBytes[i], runInputRecords[i], runOutputRecords[i]));
            break;
          default:
            fail("Unexpected type: " + type);
        }
      }

      // Check input bytes
      Arrays.sort(specInputBytes);
      Arrays.sort(runInputBytes);
      for (int i = 0; i < runTasks.length; ++i) {
        assertTrue("Mismatched " + type + " input bytes " + specInputBytes[i]
                + "/" + runInputBytes[i],
                eqPlusMinus(runInputBytes[i], specInputBytes[i], extraInputBytes));
      }

      // Check input records
      Arrays.sort(specInputRecords);
      Arrays.sort(runInputRecords);
      for (int i = 0; i < runTasks.length; ++i) {
        assertTrue(
                "Mismatched " + type + " input records " + specInputRecords[i]
                        + "/" + runInputRecords[i],
                eqPlusMinus(runInputRecords[i], specInputRecords[i],
                        extraInputRecords));
      }

      // Check output bytes
      Arrays.sort(specOutputBytes);
      Arrays.sort(runOutputBytes);
      for (int i = 0; i < runTasks.length; ++i) {
        assertTrue(
                "Mismatched " + type + " output bytes " + specOutputBytes[i] + "/"
                        + runOutputBytes[i],
                eqPlusMinus(runOutputBytes[i], specOutputBytes[i], extraOutputBytes));
      }

      // Check output records
      Arrays.sort(specOutputRecords);
      Arrays.sort(runOutputRecords);
      for (int i = 0; i < runTasks.length; ++i) {
        assertTrue(
                "Mismatched " + type + " output records " + specOutputRecords[i]
                        + "/" + runOutputRecords[i],
                eqPlusMinus(runOutputRecords[i], specOutputRecords[i],
                        extraOutputRecords));
      }

    }

    private static boolean eqPlusMinus(long a, long b, long x) {
      final long diff = Math.abs(a - b);
      return diff <= x;
    }

  }

  protected void doSubmission(String jobCreatorName, boolean defaultOutputPath)
          throws Exception {
    final Path in = new Path("foo").makeQualified(
            GridmixTestUtils.dfs.getUri(),
            GridmixTestUtils.dfs.getWorkingDirectory());
    final Path out = GridmixTestUtils.DEST.makeQualified(
            GridmixTestUtils.dfs.getUri(),
            GridmixTestUtils.dfs.getWorkingDirectory());
    final Path root = new Path(workspace.getName()).makeQualified(
        GridmixTestUtils.dfs.getUri(), GridmixTestUtils.dfs.getWorkingDirectory());
    if (!workspace.exists()) {
      assertTrue(workspace.mkdirs());
    }
    Configuration conf = null;

    try {
      ArrayList<String> argsList = new ArrayList<String>();

      argsList.add("-D" + FilePool.GRIDMIX_MIN_FILE + "=0");
      argsList.add("-D" + Gridmix.GRIDMIX_USR_RSV + "="
              + EchoUserResolver.class.getName());
      if (jobCreatorName != null) {
        argsList.add("-D" + JobCreator.GRIDMIX_JOB_TYPE + "=" + jobCreatorName);
      }

      // Set the config property gridmix.output.directory only if
      // defaultOutputPath is false. If defaultOutputPath is true, then
      // let us allow gridmix to use the path foo/gridmix/ as output dir.
      if (!defaultOutputPath) {
        argsList.add("-D" + Gridmix.GRIDMIX_OUT_DIR + "=" + out);
      }
      argsList.add("-generate");
      argsList.add(String.valueOf(GENDATA) + "m");
      argsList.add(in.toString());
      argsList.add("-"); // ignored by DebugGridmix

      String[] argv = argsList.toArray(new String[argsList.size()]);

      DebugGridmix client = new DebugGridmix();
      conf = GridmixTestUtils.mrvl.getConfig();

      CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
      conf.setEnum(GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy);

      conf.setBoolean(GridmixJob.GRIDMIX_USE_QUEUE_IN_TRACE, true);
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      conf.set(MRJobConfig.USER_NAME, ugi.getUserName());

      // allow synthetic users to create home directories
      GridmixTestUtils.dfs.mkdirs(root, new FsPermission((short) 777));
      GridmixTestUtils.dfs.setPermission(root, new FsPermission((short) 777));

      int res = ToolRunner.run(conf, client, argv);
      assertEquals("Client exited with nonzero status", 0, res);
      client.checkMonitor();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      in.getFileSystem(conf).delete(in, true);
      out.getFileSystem(conf).delete(out, true);
      root.getFileSystem(conf).delete(root, true);
    }
  }
}
