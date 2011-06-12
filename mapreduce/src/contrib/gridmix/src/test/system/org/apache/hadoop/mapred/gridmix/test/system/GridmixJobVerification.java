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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
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
      throws IOException, ParseException {

    SortedMap <Long, String> origSubmissionTime = new TreeMap <Long, String>();
    SortedMap <Long, String> simuSubmissionTime = new TreeMap<Long, String>();
    GridmixJobStory gjs = new GridmixJobStory(path, conf);
    final Iterator<JobID> ite = jobids.iterator();
    File destFolder = new File(System.getProperty("java.io.tmpdir") + 
        "/gridmix-st/");

    destFolder.mkdir();
    while (ite.hasNext()) {
      long expMapInputBytes = 0;
      long expMapOutputBytes = 0;
      long expMapInputRecs = 0;
      long expMapOutputRecs = 0;
      long expReduceInputBytes = 0;
      long expReduceOutputBytes = 0;
      long expReduceInputRecs = 0;
      long expReduceOutputRecs = 0;
      
      JobID simuJobId = ite.next();
      JobHistoryParser.JobInfo jhInfo = getSimulatedJobHistory(simuJobId);
      Assert.assertNotNull("Job history not found.", jhInfo);
      Counters counters = jhInfo.getTotalCounters();
      JobConf simuJobConf = getSimulatedJobConf(simuJobId,destFolder);
      String origJobId = simuJobConf.get("gridmix.job.original-job-id");
      LOG.info("OriginalJobID<->CurrentJobID:" + 
          origJobId + "<->" + simuJobId);

      ZombieJob zombieJob = gjs.getZombieJob(JobID.forName(origJobId));
      Map<String, Long> mapJobCounters = getJobMapCounters(zombieJob);
      Map<String, Long> reduceJobCounters = getJobReduceCounters(zombieJob);

      if (simuJobConf.get("gridmix.job-submission.policy").contains("REPLAY")) {
          origSubmissionTime.put(zombieJob.getSubmissionTime(), 
              origJobId.toString() + "^" + simuJobId); 
          simuSubmissionTime.put(jhInfo.getSubmitTime() , 
              origJobId.toString() + "^" + simuJobId); ;
      }

      LOG.info("Verifying the job <" + simuJobId + "> and wait for a while...");
      verifySimulatedJobSummary(zombieJob, jhInfo, simuJobConf);
      verifyJobMapCounters(counters,mapJobCounters,simuJobConf);
      verifyJobReduceCounters(counters,reduceJobCounters,simuJobConf); 
      verifyDistributeCache(zombieJob,simuJobConf);
      setJobDistributedCacheInfo(simuJobId.toString(), simuJobConf, 
         zombieJob.getJobConf());
      LOG.info("Done.");
    }
    verifyDistributedCacheBetweenJobs(simuAndOrigJobsInfo);
  }

  /**
   * Verify the distributed cache files between the jobs in a gridmix run.
   * @param jobsInfo - jobConfs of simulated and original jobs as a map.
   */
  public void verifyDistributedCacheBetweenJobs(Map<String, 
      List<JobConf>> jobsInfo) {
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
       Assert.assertTrue("The unique count of distibuted cache files in " + 
           "simulated jobs have not matched with the unique count of " + 
           "original jobs distributed files ", 
           simuOccurList.size() == origOccurList.size());
       int index = 0;
       for(Integer origDistFileCount : origOccurList) {
         Assert.assertTrue("Distributed cache file reused in simulated " +
             "jobs has not matched with reused of distributed cache file " + 
             "in original jobs.",origDistFileCount == simuOccurList.get(index));
         index ++;
       }
     }
  }
  
  private List<Integer> getMapValuesAsList(Map<String,Integer> jobOccurs) {
    List<Integer> occursList = new ArrayList<Integer>();
    Set<String> files = jobOccurs.keySet();
    Iterator<String > ite = files.iterator();
    while(ite.hasNext()) {
      String file = ite.next(); 
      occursList.add(jobOccurs.get(file));
    }
    return occursList;
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
    while(ite.hasNext()){
        String jobId = ite.next();
        List<JobConf> jobconfs = jobsInfo.get(jobId);
        String [] distCacheFiles = jobconfs.get(jobConfIndex).
        get(GridMixConfig.GRIDMIX_DISTCACHE_FILES).split(",");
        String [] distCacheFileTimeStamps = jobconfs.get(jobConfIndex).
        get(GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
        String [] distCacheFileVisib = jobconfs.get(jobConfIndex).
        get(GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
        int indx = 0;
        for (String distCacheFile : distCacheFiles) {
          String fileAndSize = distCacheFile + "^" + 
              distCacheFileTimeStamps[indx] + "^" +  
              jobconfs.get(jobConfIndex).getUser();
          if (filesOccurBtnJobs.get(fileAndSize)!= null) {
            int count = filesOccurBtnJobs.get(fileAndSize);
            count ++;
            filesOccurBtnJobs.put(fileAndSize,count);
          } else {
            filesOccurBtnJobs.put(fileAndSize,1);
          }
        }
    }
    return filesOccurBtnJobs;
  }
  
  private void setJobDistributedCacheInfo(String jobId, JobConf simuJobConf, 
     JobConf origJobConf) {
    if (simuJobConf.get(GridMixConfig.GRIDMIX_DISTCACHE_FILES)!= null) {
      List<JobConf> jobConfs = new ArrayList<JobConf>();
      jobConfs.add(simuJobConf);
      jobConfs.add(origJobConf);
      simuAndOrigJobsInfo.put(jobId,jobConfs);
    }
  }

  /**
   * Verify the job subimssion order between the jobs in replay mode.
   * @param origSubmissionTime - sorted map of original jobs submission times.
   * @param simuSubmissionTime - sorted map of simulated jobs submission times.
   */
  public void verifyJobSumissionTime(SortedMap<Long, String> origSubmissionTime, 
     SortedMap<Long, String> simuSubmissionTime){
    Assert.assertTrue("Simulated job's submission time count has " + 
       "not match with Original job's submission time count.", 
       origSubmissionTime.size() == simuSubmissionTime.size());
    for ( int index = 0; index < origSubmissionTime.size(); index ++) {
        String origAndSimuJobID = origSubmissionTime.get(index);
        String simuAndorigJobID = simuSubmissionTime.get(index);
        Assert.assertEquals("Simulated jobs have not submitted in same " + 
            "order as original jobs submitted inREPLAY mode.", 
            origAndSimuJobID, simuAndorigJobID);
    }
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
      checkFileVisibility(simuJobConf);
      checkDistcacheFiles(simuJobConf,origJobConf);
      checkFileSizes(simuJobConf,origJobConf);
      checkFileStamps(simuJobConf,origJobConf);
    } else {
      Assert.assertNull("Configuration has distributed cache visibilites" +
          "without enabled distributed cache emulation.", simuJobConf.
          get(GridMixConfig.GRIDMIX_DISTCACHE_VISIBILITIES));
      Assert.assertNull("Configuration has distributed cache files time " +
          "stamps without enabled distributed cache emulation.",simuJobConf.
          get(GridMixConfig.GRIDMIX_DISTCACHE_TIMESTAMP));
      Assert.assertNull("Configuration has distributed cache files paths" +
          "without enabled distributed cache emulation.",simuJobConf.
          get(GridMixConfig.GRIDMIX_DISTCACHE_FILES));
      Assert.assertNull("Configuration has distributed cache files sizes" +
          "without enabled distributed cache emulation.",simuJobConf.
          get(GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE));
    }  
  }

  private void checkFileStamps(JobConf simuJobConf, JobConf origJobConf) {
    //Verify simulated jobs against distributed cache files time stamps.
    String [] origDCFTS = origJobConf.get(GridMixConfig.
        GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
    String [] simuDCFTS = simuJobConf.get(GridMixConfig.
        GRIDMIX_DISTCACHE_TIMESTAMP).split(",");
    for (int index = 0; index < origDCFTS.length; index++) {
       Assert.assertTrue("Invalid time stamps between original " +
           "and simulated job",Long.parseLong(origDCFTS[index]) < 
           Long.parseLong(simuDCFTS[index]));
    }
  }

  private void checkFileVisibility(JobConf simuJobConf ) {
    // Verify simulated jobs against distributed cache files visibilities.
    String [] distFiles = simuJobConf.get(GridMixConfig.
        GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistVisibilities = simuJobConf.get(GridMixConfig.
        GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
    List<Boolean> expFileVisibility = new ArrayList<Boolean >();
    int index = 0;
    for (String distFile : distFiles) {
      if (!GridmixSystemTestCase.isLocalDistCache(distFile,
          simuJobConf.getUser(),Boolean.valueOf(simuDistVisibilities[index]))) {
        expFileVisibility.add(true);
      } else {
        expFileVisibility.add(false);
      }
      index ++;
    }
    index = 0;
    for (String actFileVisibility :  simuDistVisibilities) {
      Assert.assertEquals("Simulated job distributed cache file " +
          "visibilities has not matched.", 
           expFileVisibility.get(index),Boolean.valueOf(actFileVisibility));
      index ++;
    }
  }
  
  private void checkDistcacheFiles(JobConf simuJobConf, JobConf origJobConf) 
      throws IOException {
    //Verify simulated jobs against distributed cache files.
    String [] origDistFiles = origJobConf.get(GridMixConfig.
            GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistFiles = simuJobConf.get(GridMixConfig.
            GRIDMIX_DISTCACHE_FILES).split(",");
    String [] simuDistVisibilities = simuJobConf.get(GridMixConfig.
        GRIDMIX_DISTCACHE_VISIBILITIES).split(",");
    Assert.assertEquals("No. of simulatued job's distcache files " +
          "haven't matched with no.of original job's distcache files", 
          origDistFiles.length, simuDistFiles.length);

    int index = 0;
    for (String simDistFile : simuDistFiles) {
      Path distPath = new Path(simDistFile);
      if (!GridmixSystemTestCase.isLocalDistCache(simDistFile, 
          simuJobConf.getUser(),Boolean.valueOf(simuDistVisibilities[index]))) {
        FileSystem fs = distPath.getFileSystem(conf);
        FileStatus fstat = fs.getFileStatus(distPath);
        FsPermission permission = fstat.getPermission();
          Assert.assertTrue("HDFS distributed cache file has wrong " + 
              "permissions for users.", FsAction.READ_WRITE.SYMBOL == 
              permission.getUserAction().SYMBOL);
          Assert.assertTrue("HDFS distributed cache file has wrong " + 
              "permissions for groups.",FsAction.READ.SYMBOL == 
              permission.getGroupAction().SYMBOL);
          Assert.assertTrue("HDSFS distributed cache file has wrong " + 
              "permissions for others.",FsAction.READ.SYMBOL == 
              permission.getOtherAction().SYMBOL);
      }
    }
    index ++;
  }

  private void checkFileSizes(JobConf simuJobConf, JobConf origJobConf) {
    // Verify simulated jobs against distributed cache files size.
    List<String> origDistFilesSize = Arrays.asList(origJobConf.
        get(GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE).split(","));
    Collections.sort(origDistFilesSize);
    List<String> simuDistFilesSize = Arrays.asList(simuJobConf.
        get(GridMixConfig.GRIDMIX_DISTCACHE_FILESSIZE).split(","));
    Collections.sort(simuDistFilesSize);
    Assert.assertEquals("Simulated job's file size list has not " + 
        "matched with the Original job's file size list.",
        origDistFilesSize.size(),
        simuDistFilesSize.size());
    for ( int index = 0; index < origDistFilesSize.size(); index ++ ) {
       Assert.assertEquals("Simulated job distcache file size has not " +
        "matched with original job distcache file size.", 
        origDistFilesSize.get(index), simuDistFilesSize.get(index));
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
    if (!jobConf.get("gridmix.job.type", "LOADJOB").equals("SLEEPJOB")) {
    //The below statements have commented due to a bug(MAPREDUCE-2135).
    /*Assert.assertTrue("Map input bytes have not matched.<exp:[" +
          convertBytes(mapCounters.get("MAP_INPUT_BYTES").longValue()) +"]><act:[" +
          convertBytes(getCounterValue(counters,"HDFS_BYTES_READ")) + "]>",
          convertBytes(mapCounters.get("MAP_INPUT_BYTES").longValue()).equals(
          convertBytes(getCounterValue(counters,"HDFS_BYTES_READ"))));

    Assert.assertTrue("Map output bytes has not matched.<exp:[" +
        convertBytes(mapCounters.get("MAP_OUTPUT_BYTES").longValue()) + "]><act:[" +
        convertBytes(getCounterValue(counters, "MAP_OUTPUT_BYTES")) + "]>",
        convertBytes(mapCounters.get("MAP_OUTPUT_BYTES").longValue()).equals(
        convertBytes(getCounterValue(counters, "MAP_OUTPUT_BYTES"))));*/

    Assert.assertEquals("Map input records have not matched.<exp:[" +
        mapCounters.get("MAP_INPUT_RECS").longValue() + "]><act:[" +
        getCounterValue(counters, "MAP_INPUT_RECORDS") + "]>",
        mapCounters.get("MAP_INPUT_RECS").longValue(), 
        getCounterValue(counters, "MAP_INPUT_RECORDS"));

    // The below statements have commented due to a bug(MAPREDUCE-2154).
    /*Assert.assertEquals("Map output records have not matched.<exp:[" +
          mapCounters.get("MAP_OUTPUT_RECS").longValue() + "]><act:[" +
          getCounterValue(counters, "MAP_OUTPUT_RECORDS") + "]>",
          mapCounters.get("MAP_OUTPUT_RECS").longValue(), 
          getCounterValue(counters, "MAP_OUTPUT_RECORDS"));*/
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
    if (!jobConf.get("gridmix.job.type", "LOADJOB").equals("SLEEPJOB")) {
    /*Assert.assertTrue("Reduce input bytes have not matched.<exp:[" +
          convertBytes(reduceCounters.get("REDUCE_INPUT_BYTES").longValue()) + "]><act:[" +
          convertBytes(getCounterValue(counters,"REDUCE_SHUFFLE_BYTES")) + "]>",
          convertBytes(reduceCounters.get("REDUCE_INPUT_BYTES").longValue()).equals(
          convertBytes(getCounterValue(counters,"REDUCE_SHUFFLE_BYTES"))));*/

    /*Assert.assertEquals("Reduce output bytes have not matched.<exp:[" +
          convertBytes(reduceCounters.get("REDUCE_OUTPUT_BYTES").longValue()) + "]><act:[" +
          convertBytes(getCounterValue(counters,"HDFS_BYTES_WRITTEN")) + "]>",
          convertBytes(reduceCounters.get("REDUCE_OUTPUT_BYTES").longValue()).equals(
          convertBytes(getCounterValue(counters,"HDFS_BYTES_WRITTEN"))));*/

    /*Assert.assertEquals("Reduce output records have not matched.<exp:[" +
          reduceCounters.get("REDUCE_OUTPUT_RECS").longValue() + "]><act:[" + getCounterValue(counters,
          "REDUCE_OUTPUT_RECORDS") + "]>",
          reduceCounters.get("REDUCE_OUTPUT_RECS").longValue(), getCounterValue(counters,
          "REDUCE_OUTPUT_RECORDS"));*/

    /*Assert.assertEquals("Reduce input records have not matched.<exp:[" +
          reduceCounters.get("REDUCE_INPUT_RECS").longValue() + "]><act:[" + getCounterValue(counters,
          "REDUCE_INPUT_RECORDS") + "]>",
          reduceCounters.get("REDUCE_INPUT_RECS").longValue(),
          getCounterValue(counters,"REDUCE_INPUT_RECORDS"));*/
    } else {
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
     JobHistoryParser.JobInfo jhInfo, JobConf jobConf) throws IOException {
    Assert.assertEquals("Job id has not matched",
        zombieJob.getJobID(), JobID.forName(
        jobConf.get("gridmix.job.original-job-id")));

    Assert.assertEquals("Job maps have not matched", 
        zombieJob.getNumberMaps(), jhInfo.getTotalMaps());

    if (!jobConf.getBoolean("gridmix.sleep.maptask-only",false)) {
      Assert.assertEquals("Job reducers have not matched",
          zombieJob.getNumberReduces(), jhInfo.getTotalReduces());
    } else {
      Assert.assertEquals("Job reducers have not matched",
          0, jhInfo.getTotalReduces());
    }

    Assert.assertEquals("Job status has not matched.", 
        zombieJob.getOutcome().name(), 
        convertJobStatus(jhInfo.getJobStatus()));

    LoggedJob loggedJob = zombieJob.getLoggedJob();
    Assert.assertEquals("Job priority has not matched.", 
        loggedJob.getPriority().toString(), jhInfo.getPriority());

    if (jobConf.get("gridmix.user.resolve.class").contains("RoundRobin")) {
       Assert.assertTrue(jhInfo.getJobId().toString() + 
           " has not impersonate with other user.", !jhInfo.getUsername()
           .equals(UserGroupInformation.getLoginUser().getShortUserName()));
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
      throws IOException{
    FileSystem fs = null;
    try {
      String historyFilePath = jtClient.getProxy().
          getJobHistoryLocationForRetiredJob(simulatedJobID);
      Path jhpath = new Path(historyFilePath);
      fs = jhpath.getFileSystem(conf);
      fs.copyToLocalFile(jhpath,new Path(tmpJHFolder.toString()));
      fs.copyToLocalFile(new Path(historyFilePath + "_conf.xml"), 
         new Path(tmpJHFolder.toString()));
      JobConf jobConf = new JobConf();
      jobConf.addResource(new Path(tmpJHFolder.toString() + "/" + 
          simulatedJobID + "_conf.xml"));
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
  public JobHistoryParser.JobInfo getSimulatedJobHistory(JobID simulatedJobID) 
      throws IOException {
    FileSystem fs = null;
    try {
      String historyFilePath = jtClient.getProxy().
          getJobHistoryLocationForRetiredJob(simulatedJobID);
      Path jhpath = new Path(historyFilePath);
      fs = jhpath.getFileSystem(conf);
      JobHistoryParser jhparser = new JobHistoryParser(fs, jhpath);
      JobHistoryParser.JobInfo jhInfo = jhparser.parse();
      return jhInfo;
    } finally {
      fs.close();
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
      int exp = (int)(Math.log(bytesValue) / Math.log(units));
      return String.format("%1d%sB",(long)(bytesValue / Math.pow(units, exp)), 
          "KMGTPE".charAt(exp -1));
    }
  }
 

  private long getCounterValue(Counters counters,String key) 
     throws ParseException { 
    for (String groupName : counters.getGroupNames()) {
       CounterGroup totalGroup = counters.getGroup(groupName);
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
