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
package org.apache.hadoop.mapred.gridmix.test.system;

import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.junit.Assert;
import java.text.ParseException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.mapred.gridmix.GridmixSystemTestCase;

/**
 * Verifying each Gridmix job with corresponding job story in a trace file.
 */
public class GridmixJobVerification {

  private static Log LOG = LogFactory.getLog(GridmixJobVerification.class);
  private Path path;
  private Configuration conf;
  private JTClient jtClient;
  private String userResolverVal;
  static final String origJobIdKey = GridMixConfig.GRIDMIX_ORIGINAL_JOB_ID;
  static final String jobSubKey = GridMixConfig.GRIDMIX_SUBMISSION_POLICY;
  static final String jobTypeKey = GridMixConfig.GRIDMIX_JOB_TYPE;
  static final String mapTaskKey = GridMixConfig.GRIDMIX_SLEEPJOB_MAPTASK_ONLY;
  static final String usrResolver = GridMixConfig.GRIDMIX_USER_RESOLVER;
  static final String fileOutputFormatKey = "mapred.output.compress";
  static final String fileInputFormatKey = "mapred.input.dir";
  static final String compEmulKey = GridMixConfig.GRIDMIX_COMPRESSION_ENABLE;
  static final String inputDecompKey = 
      GridMixConfig.GRIDMIX_INPUT_DECOMPRESS_ENABLE;
  static final String mapInputCompRatio = 
      GridMixConfig.GRIDMIX_INPUT_COMPRESS_RATIO;
  static final String mapOutputCompRatio = 
      GridMixConfig.GRIDMIX_INTERMEDIATE_COMPRESSION_RATIO;
  static final String reduceOutputCompRatio = 
      GridMixConfig.GRIDMIX_OUTPUT_COMPRESSION_RATIO;
  private Map<String, List<JobConf>> simuAndOrigJobsInfo = 
      new HashMap<String, List<JobConf>>();

  /**
   * Gridmix job verification constructor
   * @param path - path of the gridmix output directory.
   * @param conf - cluster configuration.
   * @param jtClient - jobtracker client.
   */
  public GridmixJobVerification(Path path, Configuration conf, 
     JTClient jtClient) {
    this.path = path;
    this.conf = conf;
    this.jtClient = jtClient;
  }
  
  /**
   * It verifies the Gridmix jobs with corresponding job story in a trace file.
   * @param jobids - gridmix job ids.
   * @throws IOException - if an I/O error occurs.
   * @throws ParseException - if an parse error occurs.
   */
  public void verifyGridmixJobsWithJobStories(List<JobID> jobids) 
      throws Exception {

    SortedMap <Long, String> origSubmissionTime = new TreeMap <Long, String>();
    SortedMap <Long, String> simuSubmissionTime = new TreeMap<Long, String>();
    GridmixJobStory gjs = new GridmixJobStory(path, conf);
    final Iterator<JobID> ite = jobids.iterator();
    File destFolder = new File(System.getProperty("java.io.tmpdir") 
                              + "/gridmix-st/");
    destFolder.mkdir();
    while (ite.hasNext()) {
      JobID simuJobId = ite.next();
      
      JobHistory.JobInfo jhInfo = getSimulatedJobHistory(simuJobId);
      Assert.assertNotNull("Job history not found.", jhInfo);
      Counters counters = 
          Counters.fromEscapedCompactString(jhInfo.getValues()
              .get(JobHistory.Keys.COUNTERS));
      JobConf simuJobConf = getSimulatedJobConf(simuJobId, destFolder);
      int cnt = 1;
      do {
        if (simuJobConf != null) {
          break;
        }
        Thread.sleep(100);
        simuJobConf = getSimulatedJobConf(simuJobId, destFolder);
        cnt++;
      } while(cnt < 30);

      String origJobId = simuJobConf.get(origJobIdKey);
      LOG.info("OriginalJobID<->CurrentJobID:" 
              + origJobId + "<->" + simuJobId);

      if (userResolverVal == null) {
        userResolverVal = simuJobConf.get(usrResolver);
      }
    
      ZombieJob zombieJob = gjs.getZombieJob(JobID.forName(origJobId));
      Map<String, Long> mapJobCounters = getJobMapCounters(zombieJob);
      Map<String, Long> reduceJobCounters = getJobReduceCounters(zombieJob);
      if (simuJobConf.get(jobSubKey).contains("REPLAY")) {
          origSubmissionTime.put(zombieJob.getSubmissionTime(), 
                                 origJobId.toString() + "^" + simuJobId); 
          simuSubmissionTime.put(Long.parseLong(jhInfo.getValues().get(JobHistory.Keys.SUBMIT_TIME)), 
                                 origJobId.toString() + "^" + simuJobId); ;
      }

      LOG.info("Verifying the job <" + simuJobId + "> and wait for a while...");
      verifySimulatedJobSummary(zombieJob, jhInfo, simuJobConf);
      verifyJobMapCounters(counters, mapJobCounters, simuJobConf);
      verifyJobReduceCounters(counters, reduceJobCounters, simuJobConf); 
      verifyCompressionEmulation(zombieJob.getJobConf(), simuJobConf, counters, 
                                 reduceJobCounters, mapJobCounters);
      verifyDistributeCache(zombieJob,simuJobConf);
      setJobDistributedCacheInfo(simuJobId.toString(), simuJobConf, 
         zombieJob.getJobConf());
      verifyHighRamMemoryJobs(zombieJob, simuJobConf);
      verifyCPUEmulationOfJobs(zombieJob, jhInfo, simuJobConf);
      verifyMemoryEmulationOfJobs(zombieJob, jhInfo, simuJobConf);
      LOG.info("Done.");
    }
    verifyDistributedCacheBetweenJobs(simuAndOrigJobsInfo);
  }

  /**
   * Verify the job submission order between the jobs in replay mode.
   * @param origSubmissionTime - sorted map of original jobs submission times.
   * @param simuSubmissionTime - sorted map of simulated jobs submission times.
   */
  public void verifyJobSumissionTime(SortedMap<Long, String> origSubmissionTime, 
      SortedMap<Long, String> simuSubmissionTime) { 
    Assert.assertEquals("Simulated job's submission time count has " 
                     + "not match with Original job's submission time count.", 
                     origSubmissionTime.size(), simuSubmissionTime.size());
    for ( int index = 0; index < origSubmissionTime.size(); index ++) {
        String origAndSimuJobID = origSubmissionTime.get(index);
        String simuAndorigJobID = simuSubmissionTime.get(index);
        Assert.assertEquals("Simulated jobs have not submitted in same " 
                           + "order as original jobs submitted in REPLAY mode.", 
                           origAndSimuJobID, simuAndorigJobID);
    }
  }

  /**
   * It verifies the simulated job map counters.
   * @param counters - Original job map counters.
   * @param mapJobCounters - Simulated job map counters.
   * @param jobConf - Simulated job configuration.
   * @throws ParseException - If an parser error occurs.
   */
  public void verifyJobMapCounters(Counters counters, 
     Map<String,Long> mapCounters, JobConf jobConf) throws ParseException {
    if (!jobConf.get(jobTypeKey, "LOADJOB").equals("SLEEPJOB")) {
      Assert.assertEquals("Map input records have not matched.",
                          mapCounters.get("MAP_INPUT_RECS").longValue(), 
                          getCounterValue(counters, "MAP_INPUT_RECORDS"));
    } else {
      Assert.assertTrue("Map Input Bytes are zero", 
                        getCounterValue(counters,"HDFS_BYTES_READ") != 0);
      Assert.assertNotNull("Map Input Records are zero", 
                           getCounterValue(counters, "MAP_INPUT_RECORDS")!=0);
    }
  }

  /**
   *  It verifies the simulated job reduce counters.
   * @param counters - Original job reduce counters.
   * @param reduceCounters - Simulated job reduce counters.
   * @param jobConf - simulated job configuration.
   * @throws ParseException - if an parser error occurs.
   */
  public void verifyJobReduceCounters(Counters counters, 
     Map<String,Long> reduceCounters, JobConf jobConf) throws ParseException {
    if (jobConf.get(jobTypeKey, "LOADJOB").equals("SLEEPJOB")) {
      Assert.assertTrue("Reduce output records are not zero for sleep job.",
          getCounterValue(counters, "REDUCE_OUTPUT_RECORDS") == 0);
      Assert.assertTrue("Reduce output bytes are not zero for sleep job.", 
          getCounterValue(counters,"HDFS_BYTES_WRITTEN") == 0);
    }
  }

  /**
   * It verifies the gridmix simulated job summary.
   * @param zombieJob - Original job summary.
   * @param jhInfo  - Simulated job history info.
   * @param jobConf - simulated job configuration.
   * @throws IOException - if an I/O error occurs.
   */
  public void verifySimulatedJobSummary(ZombieJob zombieJob, 
     JobHistory.JobInfo jhInfo, JobConf jobConf) throws IOException {
    Assert.assertEquals("Job id has not matched", zombieJob.getJobID(), 
                        JobID.forName(jobConf.get(origJobIdKey)));

    Assert.assertEquals("Job maps have not matched", String.valueOf(zombieJob.getNumberMaps()),
                        jhInfo.getValues().get(JobHistory.Keys.TOTAL_MAPS));

    if (!jobConf.getBoolean(mapTaskKey, false)) { 
      Assert.assertEquals("Job reducers have not matched", 
          String.valueOf(zombieJob.getNumberReduces()), jhInfo.getValues().get(JobHistory.Keys.TOTAL_REDUCES));
    } else {
      Assert.assertEquals("Job reducers have not matched",
                          0, Integer.parseInt(jhInfo.getValues().get(JobHistory.Keys.TOTAL_REDUCES)));
    }

    Assert.assertEquals("Job status has not matched.", 
                        zombieJob.getOutcome().name(), 
                        convertJobStatus(jhInfo.getValues().get(JobHistory.Keys.JOB_STATUS)));

    LoggedJob loggedJob = zombieJob.getLoggedJob();
    Assert.assertEquals("Job priority has not matched.", 
                        loggedJob.getPriority().toString(), 
                        jhInfo.getValues().get(JobHistory.Keys.JOB_PRIORITY));

    if (jobConf.get(usrResolver).contains("RoundRobin")) {
       String user = UserGroupInformation.getLoginUser().getShortUserName();
       Assert.assertTrue(jhInfo.getValues().get(JobHistory.Keys.JOBID).toString() 
                        + " has not impersonate with other user.", 
                        !jhInfo.getValues().get(JobHistory.Keys.USER).equals(user));
    }
  }

  /**
   * Get the original job map counters from a trace.
   * @param zombieJob - Original job story.
   * @return - map counters as a map.
   */
  public Map<String, Long> getJobMapCounters(ZombieJob zombieJob) {
    long expMapInputBytes = 0;
    long expMapOutputBytes = 0;
    long expMapInputRecs = 0;
    long expMapOutputRecs = 0;
    Map<String,Long> mapCounters = new HashMap<String,Long>();
    for (int index = 0; index < zombieJob.getNumberMaps(); index ++) {
      TaskInfo mapTask = zombieJob.getTaskInfo(TaskType.MAP, index);
      expMapInputBytes += mapTask.getInputBytes();
      expMapOutputBytes += mapTask.getOutputBytes();
      expMapInputRecs += mapTask.getInputRecords();
      expMapOutputRecs += mapTask.getOutputRecords();
    }
    mapCounters.put("MAP_INPUT_BYTES", expMapInputBytes);
    mapCounters.put("MAP_OUTPUT_BYTES", expMapOutputBytes);
    mapCounters.put("MAP_INPUT_RECS", expMapInputRecs);
    mapCounters.put("MAP_OUTPUT_RECS", expMapOutputRecs);
    return mapCounters;
  }
  
  /**
   * Get the original job reduce counters from a trace.
   * @param zombieJob - Original job story.
   * @return - reduce counters as a map.
   */
  public Map<String,Long> getJobReduceCounters(ZombieJob zombieJob) {
    long expReduceInputBytes = 0;
    long expReduceOutputBytes = 0;
    long expReduceInputRecs = 0;
    long expReduceOutputRecs = 0;
    Map<String,Long> reduceCounters = new HashMap<String,Long>();
    for (int index = 0; index < zombieJob.getNumberReduces(); index ++) {
      TaskInfo reduceTask = zombieJob.getTaskInfo(TaskType.REDUCE, index);
      expReduceInputBytes += reduceTask.getInputBytes();
      expReduceOutputBytes += reduceTask.getOutputBytes();
      expReduceInputRecs += reduceTask.getInputRecords();
      expReduceOutputRecs += reduceTask.getOutputRecords();
    }
    reduceCounters.put("REDUCE_INPUT_BYTES", expReduceInputBytes);
    reduceCounters.put("REDUCE_OUTPUT_BYTES", expReduceOutputBytes);
    reduceCounters.put("REDUCE_INPUT_RECS", expReduceInputRecs);
    reduceCounters.put("REDUCE_OUTPUT_RECS", expReduceOutputRecs);
    return reduceCounters;
  }

  /**
   * Get the simulated job configuration of a job.
   * @param simulatedJobID - Simulated job id.
   * @param tmpJHFolder - temporary job history folder location.
   * @return - simulated job configuration.
   * @throws IOException - If an I/O error occurs.
   */
  public JobConf getSimulatedJobConf(JobID simulatedJobID, File tmpJHFolder) 
      throws IOException, InterruptedException {
    FileSystem fs = null;
    try {

      String historyFilePath = jtClient.getProxy()
          .getJobHistoryLocationForRetiredJob(simulatedJobID);
      int cnt = 0;
      do {
        if (historyFilePath != null) {
           break;
        }
        Thread.sleep(100);
        historyFilePath = jtClient.getProxy()
            .getJobHistoryLocationForRetiredJob(simulatedJobID);
        cnt++;
      } while( cnt < 30 );
      Assert.assertNotNull("History file has not available for the job ["
              + simulatedJobID + "] for 3 secs.", historyFilePath);
      Path jhpath = new Path(historyFilePath);
      LOG.info("Parent:" + jhpath.getParent());
      fs = jhpath.getFileSystem(conf);
      fs.copyToLocalFile(jhpath,new Path(tmpJHFolder.toString()));
      fs.copyToLocalFile(new Path(jhpath.getParent() + "/" + simulatedJobID + "_conf.xml"), 
                         new Path(tmpJHFolder.toString()));
      JobConf jobConf = new JobConf();
      jobConf.addResource(new Path(tmpJHFolder.toString() 
                         + "/" + simulatedJobID + "_conf.xml"));
      jobConf.reloadConfiguration();
      return jobConf;

    }finally {
      fs.close();
    }
  }

  /**
   * Get the simulated job history of a job.
   * @param simulatedJobID - simulated job id.
   * @return - simulated job information.
   * @throws IOException - if an I/O error occurs.
   */
  public JobHistory.JobInfo getSimulatedJobHistory(JobID simulatedJobID) 
      throws IOException, InterruptedException {
    FileSystem fs = null;
    try {
      String historyFilePath = jtClient.getProxy().
          getJobHistoryLocationForRetiredJob(simulatedJobID);
      int cnt = 0;
      do {
        if (historyFilePath != null) {
          break;
        }
        Thread.sleep(100);
        historyFilePath = jtClient.getProxy()
            .getJobHistoryLocationForRetiredJob(simulatedJobID);
        cnt++;
      } while( cnt < 30 ); 
      LOG.info("HistoryFilePath:" + historyFilePath);
      Assert.assertNotNull("History file path has not found for a job[" 
              + simulatedJobID + "] for 3 secs.");
      Path jhpath = new Path(historyFilePath);
      fs = jhpath.getFileSystem(conf);
      JobHistory.JobInfo jobInfo = 
          new JobHistory.JobInfo(simulatedJobID.toString());
      DefaultJobHistoryParser.parseJobTasks(historyFilePath, jobInfo, fs);
      return jobInfo;
    } finally {
      fs.close();
    }
  }

  /**
   * It verifies the cpu resource usage of gridmix jobs against
   * the original job cpu resource usage.
   * @param origJobHistory - Original job history.
   * @param simuJobHistoryInfo - Simulated job history.
   * @param simuJobConf - simulated job configuration.
   */
  public void verifyCPUEmulationOfJobs(ZombieJob origJobHistory,
                                   JobHistory.JobInfo simuJobHistoryInfo,
                                   JobConf simuJobConf) throws Exception {
    boolean isCPUEmulON = false;
    if (simuJobConf.get(GridMixConfig.GRIDMIX_CPU_EMULATION) != null) {
      isCPUEmulON = 
          simuJobConf.get(GridMixConfig.GRIDMIX_CPU_EMULATION).
              contains(GridMixConfig.GRIDMIX_CPU_EMULATION_PLUGIN);
    }
   
    if (isCPUEmulON) {
      Map<String,Long> origJobMetrics =
                       getOriginalJobCPUMetrics(origJobHistory);
      Map<String,Long> simuJobMetrics =
                       getSimulatedJobCPUMetrics(simuJobHistoryInfo);

      long origMapUsage = origJobMetrics.get("MAP");
      LOG.info("Total cpu usage of Maps for a original job:" + origMapUsage);

      long origReduceUsage = origJobMetrics.get("REDUCE");
      LOG.info("Total cpu usage of Reduces for a original job:" 
              + origReduceUsage);

      long simuMapUsage = simuJobMetrics.get("MAP");
      LOG.info("Total cpu usage of Maps for a simulated job:" + simuMapUsage);

      long simuReduceUsage = simuJobMetrics.get("REDUCE");
      LOG.info("Total cpu usage of Reduces for a simulated job:" 
              + simuReduceUsage);

      int mapCount = Integer.parseInt(
          simuJobHistoryInfo.getValues().get(JobHistory.Keys.TOTAL_MAPS));
      int reduceCount = Integer.parseInt(
          simuJobHistoryInfo.getValues().get(JobHistory.Keys.TOTAL_REDUCES));

      if (mapCount > 0) {
        double mapEmulFactor = (simuMapUsage * 100) / origMapUsage;
        long mapEmulAccuracy = Math.round(mapEmulFactor);
        LOG.info("CPU emulation accuracy for maps in job " +
                 simuJobHistoryInfo.getValues().get(JobHistory.Keys.JOBID) +
                 ":"+ mapEmulAccuracy + "%");
        Assert.assertTrue("Map-side cpu emulaiton inaccurate!" +
                          " Actual cpu usage: " + simuMapUsage +
                          " Expected cpu usage: " + origMapUsage, mapEmulAccuracy 
                          >= GridMixConfig.GRIDMIX_CPU_EMULATION_LOWER_LIMIT
                          && mapEmulAccuracy 
                          <= GridMixConfig.GRIDMIX_CPU_EMULATION_UPPER_LIMIT);
      }

      if (reduceCount >0) {
        double reduceEmulFactor = (simuReduceUsage * 100) / origReduceUsage;
        long reduceCPUUsage = simuReduceUsage / 1000;
        LOG.info("Reduce CPU Usage:" + reduceCPUUsage);
        LOG.info("Reduce emulation factor:" + reduceEmulFactor);
        long reduceEmulAccuracy = Math.round(reduceEmulFactor);
        LOG.info("CPU emulation accuracy for reduces in job " +
                 simuJobHistoryInfo.getValues().get(JobHistory.Keys.JOBID) + 
                 ": " + reduceEmulAccuracy + "%");
        if ( reduceCPUUsage >= 10 ) {
          Assert.assertTrue("Reduce side cpu emulaiton inaccurate!" +
                            " Actual cpu usage:" + simuReduceUsage +
                            "Expected cpu usage: " + origReduceUsage,
                            reduceEmulAccuracy 
                            >= GridMixConfig.GRIDMIX_CPU_EMULATION_LOWER_LIMIT
                            && reduceEmulAccuracy 
                            <= GridMixConfig.GRIDMIX_CPU_EMULATION_UPPER_LIMIT);
        } else {
          Assert.assertTrue("Reduce side cpu emulaiton inaccurate!" +
                            " Actual cpu usage:" + simuReduceUsage +
                            "Expected cpu usage: " + origReduceUsage,
                            reduceEmulAccuracy
                            >= 60 && reduceEmulAccuracy <= 100);
        } 
      }
    }
  }

  /**
   * It verifies the heap memory resource usage of gridmix jobs with
   * corresponding original job in the trace.
   * @param zombieJob - Original job history.
   * @param jhInfo - Simulated job history.
   * @param simuJobConf - simulated job configuration.
   */
  public void verifyMemoryEmulationOfJobs(ZombieJob zombieJob,
          JobHistory.JobInfo jhInfo, JobConf simuJobConf) throws Exception {
    long origJobMapsTHU = 0;
    long origJobReducesTHU = 0;
    long simuJobMapsTHU = 0;
    long simuJobReducesTHU = 0;
    boolean isMemEmulOn = false;
    String strHeapRatio = "0.3F";

    if (simuJobConf.get(GridMixConfig.GRIDMIX_MEMORY_EMULATION) != null) {
      isMemEmulOn =
          simuJobConf.get(GridMixConfig.GRIDMIX_MEMORY_EMULATION).
              contains(GridMixConfig.GRIDMIX_MEMORY_EMULATION_PLUGIN);
    }
    
    if (isMemEmulOn) {
      
      for (int index = 0; index < zombieJob.getNumberMaps(); index ++) {
        TaskInfo mapTask = zombieJob.getTaskInfo(TaskType.MAP, index);
        if (mapTask.getResourceUsageMetrics().getHeapUsage() > 0) {
          origJobMapsTHU +=
                  mapTask.getResourceUsageMetrics().getHeapUsage();
        }
      }
      LOG.info("Total Heap Usage of Maps for original job: " 
              + origJobMapsTHU);

      for (int index = 0; index < zombieJob.getNumberReduces(); index ++) {
        TaskInfo reduceTask = zombieJob.getTaskInfo(TaskType.REDUCE, index);
        if (reduceTask.getResourceUsageMetrics().getHeapUsage() > 0) {
          origJobReducesTHU +=
                  reduceTask.getResourceUsageMetrics().getHeapUsage();
        }
      }
      LOG.info("Total Heap Usage of Reduces for original job: " 
              + origJobReducesTHU);
      
      Counters mapCounters = 
          Counters.fromEscapedCompactString(jhInfo.getValues()
                  .get(JobHistory.Keys.MAP_COUNTERS));

      Counters reduceCounters = 
          Counters.fromEscapedCompactString(jhInfo.getValues()
                  .get(JobHistory.Keys.REDUCE_COUNTERS));

      simuJobMapsTHU = 
          getCounterValue(mapCounters, 
              Task.Counter.COMMITTED_HEAP_BYTES.toString());
      LOG.info("Simulated Job Maps Total Heap Usage: " + simuJobMapsTHU);

      simuJobReducesTHU = 
          getCounterValue(reduceCounters, 
              Task.Counter.COMMITTED_HEAP_BYTES.toString());
      LOG.info("Simulated Jobs Reduces Total Heap Usage: " + simuJobReducesTHU);

      long mapCount = 
          Integer.parseInt(jhInfo.getValues()
                  .get(JobHistory.Keys.TOTAL_MAPS));
      long reduceCount = 
          Integer.parseInt(jhInfo.getValues()
                  .get(JobHistory.Keys.TOTAL_REDUCES));

      if (simuJobConf.get(GridMixConfig
          .GRIDMIX_HEAP_FREE_MEMORY_RATIO) != null) {
        strHeapRatio = "0.3F";
      }

      if (mapCount > 0) {
        double mapEmulFactor = (simuJobMapsTHU * 100) / origJobMapsTHU;
        long mapEmulAccuracy = Math.round(mapEmulFactor);
        LOG.info("Maps memory emulation accuracy of a job:"
                + mapEmulAccuracy + "%");
        Assert.assertTrue("Map phase total memory emulation had crossed the "
                         + "configured max limit.", mapEmulAccuracy
                         <= GridMixConfig.GRIDMIX_MEMORY_EMULATION_UPPER_LIMIT);
        Assert.assertTrue("Map phase total memory emulation had not crossed "
                         + "the configured min limit.", mapEmulAccuracy
                         >= GridMixConfig.GRIDMIX_MEMORY_EMULATION_LOWER_LIMIT);
        double expHeapRatio = Double.parseDouble(strHeapRatio);
        LOG.info("expHeapRatio for maps:" + expHeapRatio);
        double actHeapRatio =
                ((double)Math.abs(origJobMapsTHU - simuJobMapsTHU)) ;
        actHeapRatio /= origJobMapsTHU;
          LOG.info("actHeapRatio for maps:" + actHeapRatio);
          Assert.assertTrue("Simulate job maps heap ratio not matched.",
                            actHeapRatio <= expHeapRatio);
      }

      if (reduceCount >0) {
        double reduceEmulFactor = (simuJobReducesTHU * 100) / origJobReducesTHU;
        long reduceEmulAccuracy = Math.round(reduceEmulFactor);
        LOG.info("Reduces memory emulation accuracy of a job:"
                + reduceEmulAccuracy + "%");
        Assert.assertTrue("Reduce phase total memory emulation had crossed "
                         + "configured max limit.", reduceEmulAccuracy
                         <= GridMixConfig.GRIDMIX_MEMORY_EMULATION_UPPER_LIMIT);
        Assert.assertTrue("Reduce phase total memory emulation had not "
                         + "crosssed configured min limit.", reduceEmulAccuracy
                         >= GridMixConfig.GRIDMIX_MEMORY_EMULATION_LOWER_LIMIT);
        double expHeapRatio = Double.parseDouble(strHeapRatio);
        LOG.info("expHeapRatio for reduces:" + expHeapRatio);
        double actHeapRatio =
                ((double)Math.abs(origJobReducesTHU - simuJobReducesTHU));
        actHeapRatio /= origJobReducesTHU;
          LOG.info("actHeapRatio for reduces:" + actHeapRatio);
          Assert.assertTrue("Simulate job reduces heap ratio not matched.",
                            actHeapRatio <= expHeapRatio);
      }
    }
  }

  /**
   *  Get the simulated job cpu metrics.
   * @param jhInfo - Simulated job history
   * @return - cpu metrics as a map.
   * @throws Exception - if an error occurs.
   */
  private Map<String,Long> getSimulatedJobCPUMetrics(
                           JobHistory.JobInfo jhInfo) throws Exception {
    Map<String, Long> resourceMetrics = new HashMap<String, Long>();
    Counters mapCounters = Counters.fromEscapedCompactString(
        jhInfo.getValues().get(JobHistory.Keys.MAP_COUNTERS));
    long mapCPUUsage =
        getCounterValue(mapCounters,
                        Task.Counter.CPU_MILLISECONDS.toString());
    resourceMetrics.put("MAP", mapCPUUsage);

    Counters reduceCounters = Counters.fromEscapedCompactString(
        jhInfo.getValues().get(JobHistory.Keys.REDUCE_COUNTERS));
    long reduceCPUUsage =
        getCounterValue(reduceCounters,
                        Task.Counter.CPU_MILLISECONDS.toString());
    resourceMetrics.put("REDUCE", reduceCPUUsage);
    return resourceMetrics;
  }

  /**
   * Get the original job cpu metrics.
   * @param zombieJob - original job history.
   * @return - cpu metrics as map.
   */
  private Map<String, Long> getOriginalJobCPUMetrics(ZombieJob zombieJob) {
    long mapTotalCPUUsage = 0;
    long reduceTotalCPUUsage = 0;
    Map<String,Long> resourceMetrics = new HashMap<String,Long>();

    for (int index = 0; index < zombieJob.getNumberMaps(); index++) {
      TaskInfo mapTask = zombieJob.getTaskInfo(TaskType.MAP, index);
      if (mapTask.getResourceUsageMetrics().getCumulativeCpuUsage() > 0) {
        mapTotalCPUUsage +=
            mapTask.getResourceUsageMetrics().getCumulativeCpuUsage();
      }
    }
    resourceMetrics.put("MAP", mapTotalCPUUsage);

    for (int index = 0; index < zombieJob.getNumberReduces(); index++) {
      TaskInfo reduceTask = zombieJob.getTaskInfo(TaskType.REDUCE, index);
      if (reduceTask.getResourceUsageMetrics().getCumulativeCpuUsage() > 0) {
        reduceTotalCPUUsage +=
            reduceTask.getResourceUsageMetrics().getCumulativeCpuUsage();
      }
    }
    resourceMetrics.put("REDUCE", reduceTotalCPUUsage);
    return resourceMetrics;
  }

  /**
   * Get the user resolver of a job.
   */
  public String getJobUserResolver() {
    return userResolverVal;
  }

  /**
   * It verifies the compression ratios of mapreduce jobs.
   * @param origJobConf - original job configuration.
   * @param simuJobConf - simulated job configuration.
   * @param counters  - simulated job counters.
   * @param origReduceCounters - original job reduce counters.
   * @param origMapCounters - original job map counters.
   * @throws ParseException - if a parser error occurs.
   * @throws IOException - if an I/O error occurs.
   */
  public void verifyCompressionEmulation(JobConf origJobConf, 
                                         JobConf simuJobConf,Counters counters, 
                                         Map<String, Long> origReduceCounters, 
                                         Map<String, Long> origMapJobCounters) 
                                         throws ParseException,IOException { 
    if (simuJobConf.getBoolean(compEmulKey, false)) {
      String inputDir = origJobConf.get(fileInputFormatKey);
      Assert.assertNotNull(fileInputFormatKey + " is Null",inputDir);
      long simMapInputBytes = getCounterValue(counters, "HDFS_BYTES_READ");
      long uncompressedInputSize = origMapJobCounters.get("MAP_INPUT_BYTES"); 
      long simReduceInputBytes =
            getCounterValue(counters, "REDUCE_SHUFFLE_BYTES");
        long simMapOutputBytes = getCounterValue(counters, "MAP_OUTPUT_BYTES");

      // Verify input compression whether it's enable or not.
      if (inputDir.contains(".gz") || inputDir.contains(".tgz") 
         || inputDir.contains(".bz")) { 
        Assert.assertTrue("Input decompression attribute has been not set for " 
                         + "for compressed input",
                         simuJobConf.getBoolean(inputDecompKey, false));

        float INPUT_COMP_RATIO = 
            getExpectedCompressionRatio(simuJobConf, mapInputCompRatio);
        float INTERMEDIATE_COMP_RATIO = 
            getExpectedCompressionRatio(simuJobConf, mapOutputCompRatio);

        // Verify Map Input Compression Ratio.
        assertMapInputCompressionRatio(simMapInputBytes, uncompressedInputSize, 
                                       INPUT_COMP_RATIO);

        // Verify Map Output Compression Ratio.
        assertMapOuputCompressionRatio(simReduceInputBytes, simMapOutputBytes, 
                                       INTERMEDIATE_COMP_RATIO);
      } else {
        Assert.assertEquals("MAP input bytes has not matched.", 
                            convertBytes(uncompressedInputSize), 
                            convertBytes(simMapInputBytes));
      }

      Assert.assertEquals("Simulated job output format has not matched with " 
                         + "original job output format.",
                         origJobConf.getBoolean(fileOutputFormatKey,false), 
                         simuJobConf.getBoolean(fileOutputFormatKey,false));

      if (simuJobConf.getBoolean(fileOutputFormatKey,false)) { 
        float OUTPUT_COMP_RATIO = 
            getExpectedCompressionRatio(simuJobConf, reduceOutputCompRatio);

         //Verify reduce output compression ratio.
         long simReduceOutputBytes = 
             getCounterValue(counters, "HDFS_BYTES_WRITTEN");
         long origReduceOutputBytes = 
             origReduceCounters.get("REDUCE_OUTPUT_BYTES");
         assertReduceOutputCompressionRatio(simReduceOutputBytes, 
                                            origReduceOutputBytes, 
                                            OUTPUT_COMP_RATIO);
      }
    }
  }

  private void assertMapInputCompressionRatio(long simMapInputBytes, 
                                   long origMapInputBytes, 
                                   float expInputCompRatio) { 
    LOG.info("***Verify the map input bytes compression ratio****");
    LOG.info("Simulated job's map input bytes(REDUCE_SHUFFLE_BYTES): " 
            + simMapInputBytes);
    LOG.info("Original job's map input bytes: " + origMapInputBytes);

    final float actInputCompRatio = 
        getActualCompressionRatio(simMapInputBytes, origMapInputBytes);
    LOG.info("Expected Map Input Compression Ratio:" + expInputCompRatio);
    LOG.info("Actual Map Input Compression Ratio:" + actInputCompRatio);

    float diffVal = (float)(expInputCompRatio * 0.06);
    LOG.info("Expected Difference of Map Input Compression Ratio is <= " + 
            + diffVal);
    float delta = Math.abs(expInputCompRatio - actInputCompRatio);
    LOG.info("Actual Difference of Map Iput Compression Ratio:" + delta);
    Assert.assertTrue("Simulated job input compression ratio has mismatched.", 
                      delta <= diffVal);
    LOG.info("******Done******");
  }

  private void assertMapOuputCompressionRatio(long simReduceInputBytes, 
                                              long simMapoutputBytes, 
                                              float expMapOuputCompRatio) { 
    LOG.info("***Verify the map output bytes compression ratio***");
    LOG.info("Simulated job reduce input bytes:" + simReduceInputBytes);
    LOG.info("Simulated job map output bytes:" + simMapoutputBytes);

    final float actMapOutputCompRatio = 
        getActualCompressionRatio(simReduceInputBytes, simMapoutputBytes);
    LOG.info("Expected Map Output Compression Ratio:" + expMapOuputCompRatio);
    LOG.info("Actual Map Output Compression Ratio:" + actMapOutputCompRatio);

    float diffVal = 0.05f;
    LOG.info("Expected Difference Of Map Output Compression Ratio is <= " 
            + diffVal);
    float delta = Math.abs(expMapOuputCompRatio - actMapOutputCompRatio);
    LOG.info("Actual Difference Of Map Ouput Compression Ratio :" + delta);

    Assert.assertTrue("Simulated job map output compression ratio " 
                     + "has not been matched.", delta <= diffVal);
    LOG.info("******Done******");
  }

  private void assertReduceOutputCompressionRatio(long simReduceOutputBytes, 
      long origReduceOutputBytes , float expOutputCompRatio ) {
      LOG.info("***Verify the reduce output bytes compression ratio***");
      final float actOuputputCompRatio = 
          getActualCompressionRatio(simReduceOutputBytes, origReduceOutputBytes);
      LOG.info("Simulated job's reduce output bytes:" + simReduceOutputBytes);
      LOG.info("Original job's reduce output bytes:" + origReduceOutputBytes);
      LOG.info("Expected output compression ratio:" + expOutputCompRatio);
      LOG.info("Actual output compression ratio:" + actOuputputCompRatio);
      long diffVal = (long)(origReduceOutputBytes * 0.15);
      long delta = Math.abs(origReduceOutputBytes - simReduceOutputBytes);
      LOG.info("Expected difference of output compressed bytes is <= " 
              + diffVal);
      LOG.info("Actual difference of compressed ouput bytes:" + delta);
      Assert.assertTrue("Simulated job reduce output compression ratio " +
         "has not been matched.", delta <= diffVal);
      LOG.info("******Done******");
  }

  private float getExpectedCompressionRatio(JobConf simuJobConf, 
                                            String RATIO_TYPE) {
    // Default decompression ratio is 0.50f irrespective of original 
    //job compression ratio.
    if (simuJobConf.get(RATIO_TYPE) != null) {
      return Float.parseFloat(simuJobConf.get(RATIO_TYPE));
    } else {
      return 0.50f;
    }
  }

  private float getActualCompressionRatio(long compressBytes, 
                                          long uncompessBytes) {
    double ratio = ((double)compressBytes) / uncompessBytes; 
    int significant = (int)Math.round(ratio * 100);
    return ((float)significant)/100; 
  }

  /**
   * Verify the distributed cache files between the jobs in a gridmix run.
   * @param jobsInfo - jobConfs of simulated and original jobs as a map.
   */
  public void verifyDistributedCacheBetweenJobs(
      Map<String,List<JobConf>> jobsInfo) {
     if (jobsInfo.size() > 1) {
       Map<String, Integer> simJobfilesOccurBtnJobs = 
           getDistcacheFilesOccurenceBetweenJobs(jobsInfo, 0);
       Map<String, Integer> origJobfilesOccurBtnJobs = 
           getDistcacheFilesOccurenceBetweenJobs(jobsInfo, 1);
       List<Integer> simuOccurList = 
           getMapValuesAsList(simJobfilesOccurBtnJobs);
       Collections.sort(simuOccurList);
       List<Integer> origOccurList = 
           getMapValuesAsList(origJobfilesOccurBtnJobs);
       Collections.sort(origOccurList);
       Assert.assertEquals("The unique count of distibuted cache files in " 
                        + "simulated jobs have not matched with the unique "
                        + "count of original jobs distributed files ", 
                        simuOccurList.size(), origOccurList.size());
       int index = 0;
       for (Integer origDistFileCount : origOccurList) {
         Assert.assertEquals("Distributed cache file reused in simulated " 
                            + "jobs has not matched with reused of distributed"
                            + "cache file in original jobs.",
                            origDistFileCount, simuOccurList.get(index));
         index ++;
       }
     }
  }

  /**
   * Get the unique distributed cache files and occurrence between the jobs.
   * @param jobsInfo - job's configurations as a map.
   * @param jobConfIndex - 0 for simulated job configuration and 
   *                       1 for original jobs configuration.
   * @return  - unique distributed cache files and occurrences as map.
   */
  private Map<String, Integer> getDistcacheFilesOccurenceBetweenJobs(
      Map<String, List<JobConf>> jobsInfo, int jobConfIndex) {
    Map<String,Integer> filesOccurBtnJobs = new HashMap <String,Integer>();
    Set<String> jobIds = jobsInfo.keySet();
    Iterator<String > ite = jobIds.iterator();
    while (ite.hasNext()) {
      String jobId = ite.next();
      List<JobConf> jobconfs = jobsInfo.get(jobId);
      String [] distCacheFiles = jobconfs.get(jobConfIndex).get(
          GridMixConfig.GRIDMIX_DISTCACHE_FILES).split(",");
      String [] distCacheFileTimeStamps = jobconfs.get(jobConfIndex).get(
          GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
      String [] distCacheFileVisib = jobconfs.get(jobConfIndex).get(
          GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
      int indx = 0;
      for (String distCacheFile : distCacheFiles) {
        String fileAndSize = distCacheFile + "^" 
                           + distCacheFileTimeStamps[indx] + "^" 
                           + jobconfs.get(jobConfIndex).getUser();
        if (filesOccurBtnJobs.get(fileAndSize) != null) {
          int count = filesOccurBtnJobs.get(fileAndSize);
          count ++;
          filesOccurBtnJobs.put(fileAndSize, count);
        } else {
          filesOccurBtnJobs.put(fileAndSize, 1);
        }
      }
    }
    return filesOccurBtnJobs;
  }

  /**
   * It verifies the distributed cache emulation of  a job.
   * @param zombieJob - Original job story.
   * @param simuJobConf - Simulated job configuration.
   */
  public void verifyDistributeCache(ZombieJob zombieJob, 
                                    JobConf simuJobConf) throws IOException {
    if (simuJobConf.getBoolean(GridMixConfig.GRIDMIX_DISTCACHE_ENABLE, false)) {
      JobConf origJobConf = zombieJob.getJobConf();
      assertFileVisibility(simuJobConf);
      assertDistcacheFiles(simuJobConf,origJobConf);
      assertFileSizes(simuJobConf,origJobConf);
      assertFileStamps(simuJobConf,origJobConf);
    } else {
      Assert.assertNull("Configuration has distributed cache visibilites" 
          + "without enabled distributed cache emulation.", 
          simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES));
      Assert.assertNull("Configuration has distributed cache files time " 
          + "stamps without enabled distributed cache emulation.", 
          simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP));
      Assert.assertNull("Configuration has distributed cache files paths" 
          + "without enabled distributed cache emulation.", 
          simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_FILES));
      Assert.assertNull("Configuration has distributed cache files sizes" 
          + "without enabled distributed cache emulation.", 
          simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE));
    }
  }

  private void assertFileStamps(JobConf simuJobConf, JobConf origJobConf) {
    //Verify simulated jobs against distributed cache files time stamps.
    String [] origDCFTS = 
        origJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
    String [] simuDCFTS = 
        simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
    for (int index = 0; index < origDCFTS.length; index++) { 
      Assert.assertTrue("Invalid time stamps between original "
          +"and simulated job", Long.parseLong(origDCFTS[index]) 
          < Long.parseLong(simuDCFTS[index]));
    }
  }

  private void assertFileVisibility(JobConf simuJobConf ) {
    // Verify simulated jobs against distributed cache files visibilities.
    String [] distFiles = 
        simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistVisibilities = 
        simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
    List<Boolean> expFileVisibility = new ArrayList<Boolean >();
    int index = 0;
    for (String distFile : distFiles) {
      boolean isLocalDistCache = GridmixSystemTestCase.isLocalDistCache(
                                 distFile, 
                                 simuJobConf.getUser(), 
                                 Boolean.valueOf(simuDistVisibilities[index]));
      if (!isLocalDistCache) {
        expFileVisibility.add(true);
      } else {
        expFileVisibility.add(false);
      }
      index ++;
    }
    index = 0;
    for (String actFileVisibility :  simuDistVisibilities) {
      Assert.assertEquals("Simulated job distributed cache file " 
                         + "visibilities has not matched.", 
                         expFileVisibility.get(index),
                         Boolean.valueOf(actFileVisibility));
      index ++;
    }
  }
  
  private void assertDistcacheFiles(JobConf simuJobConf, JobConf origJobConf) 
      throws IOException {
    //Verify simulated jobs against distributed cache files.
    String [] origDistFiles = origJobConf.get(
        GridMixConfig.GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistFiles = simuJobConf.get(
        GridMixConfig.GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistVisibilities = simuJobConf.get(
        GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
    Assert.assertEquals("No. of simulatued job's distcache files mismacted" 
                       + "with no.of original job's distcache files", 
                       origDistFiles.length, simuDistFiles.length);

    int index = 0;
    for (String simDistFile : simuDistFiles) {
      Path distPath = new Path(simDistFile);
      boolean isLocalDistCache = 
          GridmixSystemTestCase.isLocalDistCache(simDistFile,
              simuJobConf.getUser(),
              Boolean.valueOf(simuDistVisibilities[index]));
      if (!isLocalDistCache) {
        FileSystem fs = distPath.getFileSystem(conf);
        FileStatus fstat = fs.getFileStatus(distPath);
        FsPermission permission = fstat.getPermission();
        Assert.assertTrue("HDFS distributed cache file has wrong " 
                         + "permissions for users.", 
                         FsAction.READ_WRITE.SYMBOL 
                         == permission.getUserAction().SYMBOL);
        Assert.assertTrue("HDFS distributed cache file has wrong " 
                         + "permissions for groups.", 
                         FsAction.READ.SYMBOL 
                         == permission.getGroupAction().SYMBOL);
        Assert.assertTrue("HDSFS distributed cache file has wrong " 
                         + "permissions for others.", 
                         FsAction.READ.SYMBOL 
                         == permission.getOtherAction().SYMBOL);
      }
      index++;
    }
  }

  private void assertFileSizes(JobConf simuJobConf, JobConf origJobConf) { 
    // Verify simulated jobs against distributed cache files size.
    List<String> origDistFilesSize = 
        Arrays.asList(origJobConf.get(
            GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE).split(","));
    Collections.sort(origDistFilesSize);

    List<String> simuDistFilesSize = 
        Arrays.asList(simuJobConf.get(
            GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE).split(","));
    Collections.sort(simuDistFilesSize);

    Assert.assertEquals("Simulated job's file size list has not " 
                       + "matched with the Original job's file size list.",
                       origDistFilesSize.size(),
                       simuDistFilesSize.size());

    for (int index = 0; index < origDistFilesSize.size(); index ++) {
       Assert.assertEquals("Simulated job distcache file size has not " 
                          + "matched with original job distcache file size.", 
                          origDistFilesSize.get(index), 
                          simuDistFilesSize.get(index));
    }
  }

  private void setJobDistributedCacheInfo(String jobId, JobConf simuJobConf, 
     JobConf origJobConf) { 
    if (simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_FILES) != null) {
      List<JobConf> jobConfs = new ArrayList<JobConf>();
      jobConfs.add(simuJobConf);
      jobConfs.add(origJobConf);
      simuAndOrigJobsInfo.put(jobId,jobConfs);
    }
  }

  private List<Integer> getMapValuesAsList(Map<String,Integer> jobOccurs) { 
    List<Integer> occursList = new ArrayList<Integer>();
    Set<String> files = jobOccurs.keySet();
    Iterator<String > ite = files.iterator();
    while (ite.hasNext()) {
      String file = ite.next(); 
      occursList.add(jobOccurs.get(file));
    }
    return occursList;
  }

  /**
   * It verifies the high ram gridmix jobs.
   * @param zombieJob - Original job story.
   * @param simuJobConf - Simulated job configuration.
   */
  @SuppressWarnings("deprecation")
  public void verifyHighRamMemoryJobs(ZombieJob zombieJob,
                                      JobConf simuJobConf) {
    JobConf origJobConf = zombieJob.getJobConf();
    int origMapFactor = getMapFactor(origJobConf);
    int origReduceFactor = getReduceFactor(origJobConf);
    boolean isHighRamEnable = 
        simuJobConf.getBoolean(GridMixConfig.GRIDMIX_HIGH_RAM_JOB_ENABLE, 
                               false);
    if (isHighRamEnable) {
        if (origMapFactor >= 2 && origReduceFactor >= 2) {
          assertGridMixHighRamJob(simuJobConf, origJobConf, 1);
        } else if(origMapFactor >= 2) {
          assertGridMixHighRamJob(simuJobConf, origJobConf, 2);
        } else if(origReduceFactor >= 2) {
          assertGridMixHighRamJob(simuJobConf, origJobConf, 3);
        }
    } else {
        if (origMapFactor >= 2 && origReduceFactor >= 2) {
              assertGridMixHighRamJob(simuJobConf, origJobConf, 4);
        } else if(origMapFactor >= 2) {
              assertGridMixHighRamJob(simuJobConf, origJobConf, 5);
        } else if(origReduceFactor >= 2) {
              assertGridMixHighRamJob(simuJobConf, origJobConf, 6);
        }
    }
  }

  /**
   * Get the value for identifying the slots used by the map.
   * @param jobConf - job configuration
   * @return - map factor value.
   */
  public static int getMapFactor(Configuration jobConf) {
    long clusterMapMem = 
        Long.parseLong(jobConf.get(GridMixConfig.CLUSTER_MAP_MEMORY));
    long jobMapMem = 
        Long.parseLong(jobConf.get(GridMixConfig.JOB_MAP_MEMORY_MB));
    return (int)Math.ceil((double)jobMapMem / clusterMapMem);  
  }

  /**
   * Get the value for identifying the slots used by the reduce.
   * @param jobConf - job configuration.
   * @return - reduce factor value.
   */
  public static int getReduceFactor(Configuration jobConf) {
    long clusterReduceMem = 
        Long.parseLong(jobConf.get(GridMixConfig.CLUSTER_REDUCE_MEMORY));
    long jobReduceMem = 
        Long.parseLong(jobConf.get(GridMixConfig.JOB_REDUCE_MEMORY_MB));
    return (int)Math.ceil((double)jobReduceMem / clusterReduceMem);
  }

  @SuppressWarnings("deprecation")
  private void assertGridMixHighRamJob(JobConf simuJobConf, 
                                       Configuration origConf, int option) {
    int simuMapFactor = getMapFactor(simuJobConf);
    int simuReduceFactor = getReduceFactor(simuJobConf);
    /**
     *  option 1 : Both map and reduce honors the high ram.
     *  option 2 : Map only honors the high ram.
     *  option 3 : Reduce only honors the high ram.
     *  option 4 : Both map and reduce should not honors the high ram
     *             in disable state.
     *  option 5 : Map should not honors the high ram in disable state.
     *  option 6 : Reduce should not honors the high ram in disable state.
     */
    switch (option) {
      case 1 :
               Assert.assertTrue("Gridmix job has not honored the high "
                                + "ram for map.", simuMapFactor >= 2 
                                && simuMapFactor == getMapFactor(origConf));
               Assert.assertTrue("Gridmix job has not honored the high "
                                + "ram for reduce.", simuReduceFactor >= 2 
                                && simuReduceFactor 
                                == getReduceFactor(origConf));
               break;
      case 2 :
               Assert.assertTrue("Gridmix job has not honored the high "
                                + "ram for map.", simuMapFactor >= 2 
                                && simuMapFactor == getMapFactor(origConf));
               break;
      case 3 :
               Assert.assertTrue("Girdmix job has not honored the high "
                                + "ram for reduce.", simuReduceFactor >= 2 
                                && simuReduceFactor 
                                == getReduceFactor(origConf));
               break;
      case 4 :
               Assert.assertTrue("Gridmix job has honored the high "
                                + "ram for map in emulation disable state.", 
                                simuMapFactor < 2 
                                && simuMapFactor != getMapFactor(origConf));
               Assert.assertTrue("Gridmix job has honored the high "
                                + "ram for reduce in emulation disable state.", 
                                simuReduceFactor < 2 
                                && simuReduceFactor 
                                != getReduceFactor(origConf));
               break;
      case 5 :
               Assert.assertTrue("Gridmix job has honored the high "
                                + "ram for map in emulation disable state.", 
                                simuMapFactor < 2 
                                && simuMapFactor != getMapFactor(origConf));
               break;
      case 6 :
               Assert.assertTrue("Girdmix job has honored the high "
                                + "ram for reduce in emulation disable state.", 
                                simuReduceFactor < 2 
                                && simuReduceFactor 
                                != getReduceFactor(origConf));
               break;
    }
  }

  /**
   * Get task memory after scaling based on cluster configuration.
   * @param jobTaskKey - Job task key attribute.
   * @param clusterTaskKey - Cluster task key attribute.
   * @param origConf - Original job configuration.
   * @param simuConf - Simulated job configuration.
   * @return scaled task memory value.
   */
  @SuppressWarnings("deprecation")
  public static long getScaledTaskMemInMB(String jobTaskKey, 
                                          String clusterTaskKey, 
                                          Configuration origConf, 
                                          Configuration simuConf) { 
    long simuClusterTaskValue = 
        simuConf.getLong(clusterTaskKey, JobConf.DISABLED_MEMORY_LIMIT);
    long origClusterTaskValue = 
        origConf.getLong(clusterTaskKey, JobConf.DISABLED_MEMORY_LIMIT);
    long origJobTaskValue = 
        origConf.getLong(jobTaskKey, JobConf.DISABLED_MEMORY_LIMIT);
    double scaleFactor = 
        Math.ceil((double)origJobTaskValue / origClusterTaskValue);
    long simulatedJobValue = (long)(scaleFactor * simuClusterTaskValue);
    return simulatedJobValue;
  }

  /**
   * It Verifies the memory limit of a task.
   * @param TaskMemInMB - task memory limit.
   * @param taskLimitInMB - task upper limit.
   */
  public static void verifyMemoryLimits(long TaskMemInMB, long taskLimitInMB) {
    if (TaskMemInMB > taskLimitInMB) {
      Assert.fail("Simulated job's task memory exceeds the " 
                 + "upper limit of task virtual memory.");
    }
  }

  private String convertJobStatus(String jobStatus) {
    if (jobStatus.equals("SUCCEEDED")) { 
      return "SUCCESS";
    } else {
      return jobStatus;
    }
  }
  
  private String convertBytes(long bytesValue) {
    int units = 1024;
    if( bytesValue < units ) {
      return String.valueOf(bytesValue)+ "B";
    } else {
      // it converts the bytes into either KB or MB or GB or TB etc.
      int exp = (int)(Math.log(bytesValue) / Math.log(units));
      return String.format("%1d%sB",(long)(bytesValue / Math.pow(units, exp)), 
          "KMGTPE".charAt(exp -1));
    }
  }
 

  private long getCounterValue(Counters counters, String key) 
     throws ParseException { 
    for (String groupName : counters.getGroupNames()) {
       Group totalGroup = counters.getGroup(groupName);
       Iterator<Counter> itrCounter = totalGroup.iterator();
       while (itrCounter.hasNext()) {
         Counter counter = itrCounter.next();
         if (counter.getName().equals(key)) {
           return counter.getValue();
         }
       }
    }
    return 0;
  }
}

