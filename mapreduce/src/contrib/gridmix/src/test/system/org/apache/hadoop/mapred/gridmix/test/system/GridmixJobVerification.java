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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
/**
 * Verifying each Gridmix job with corresponding job story in a trace file.
 */
public class GridmixJobVerification {

  private static Log LOG = LogFactory.getLog(GridmixJobVerification.class);
  private Path path;
  private Configuration conf;
  private JTClient jtClient;
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

    List<Long> origSubmissionTime = new ArrayList<Long>();
    List<Long> simuSubmissionTime = new ArrayList<Long>();
    GridmixJobStory gjs = new GridmixJobStory(path, conf);
    final Iterator<JobID> ite = jobids.iterator();
    java.io.File destFolder = new java.io.File("/tmp/gridmix-st/");

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
      
      JobID currJobId = ite.next();
      String historyFilePath = jtClient.getProxy().
          getJobHistoryLocationForRetiredJob(currJobId);
      Path jhpath = new Path(historyFilePath);
      FileSystem fs = jhpath.getFileSystem(conf);
      JobHistoryParser jhparser = new JobHistoryParser(fs, jhpath);
      JobHistoryParser.JobInfo jhInfo = jhparser.parse();
      Counters counters = jhInfo.getTotalCounters();

      fs.copyToLocalFile(jhpath,new Path(destFolder.toString()));
      fs.copyToLocalFile(new Path(historyFilePath + "_conf.xml"), 
          new Path(destFolder.toString()));
      JobConf jobConf = new JobConf(conf);
      jobConf.addResource(new Path("/tmp/gridmix-st/" + 
          currJobId + "_conf.xml"));
      String origJobId = jobConf.get("gridmix.job.original-job-id");
      LOG.info("OriginalJobID<->CurrentJobID:" + 
          origJobId + "<->" + currJobId);

      ZombieJob zombieJob = gjs.getZombieJob(JobID.forName(origJobId));
      LoggedJob loggedJob = zombieJob.getLoggedJob();
      
      for (int index = 0; index < zombieJob.getNumberMaps(); index ++) {
        TaskInfo mapTask = zombieJob.getTaskInfo(TaskType.MAP, index);
        expMapInputBytes += mapTask.getInputBytes();
        expMapOutputBytes += mapTask.getOutputBytes();
        expMapInputRecs += mapTask.getInputRecords();
        expMapOutputRecs += mapTask.getOutputRecords();
      }

      for (int index = 0; index < zombieJob.getNumberReduces(); index ++) {
        TaskInfo reduceTask = zombieJob.getTaskInfo(TaskType.REDUCE, index);
        expReduceInputBytes += reduceTask.getInputBytes();
        expReduceOutputBytes += reduceTask.getOutputBytes();
        expReduceInputRecs += reduceTask.getInputRecords();
        expReduceOutputRecs += reduceTask.getOutputRecords();
      }

      LOG.info("Verifying the job <" + currJobId + "> and wait for a while...");
      Assert.assertEquals("Job id has not matched",
          zombieJob.getJobID(), JobID.forName(origJobId));

      Assert.assertEquals("Job maps have not matched", 
          zombieJob.getNumberMaps(), 
          jhInfo.getTotalMaps());

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

      Assert.assertEquals("Job priority has not matched.", 
         loggedJob.getPriority().toString(), jhInfo.getPriority());

      if (jobConf.get("gridmix.user.resolve.class").contains("RoundRobin")) {
         Assert.assertTrue(currJobId + "has not impersonate with other user.",
             !jhInfo.getUsername().equals(UserGroupInformation.
             getLoginUser().getShortUserName()));
      }

      if (jobConf.get("gridmix.job-submission.policy").contains("REPLAY")) {
        origSubmissionTime.add(zombieJob.getSubmissionTime()); 
        simuSubmissionTime.add(jhInfo.getSubmitTime());
      }

      if (!jobConf.get("gridmix.job.type", "LOADJOB").equals("SLEEPJOB")) {
        
        //The below statements have commented due to a bug(MAPREDUCE-2135).
      /*  Assert.assertTrue("Map input bytes have not matched.<exp:[" + 
            convertBytes(expMapInputBytes) +"]><act:[" + 
            convertBytes(getCounterValue(counters,"HDFS_BYTES_READ")) + "]>", 
            convertBytes(expMapInputBytes).equals( 
            convertBytes(getCounterValue(counters,"HDFS_BYTES_READ"))));

        Assert.assertTrue("Map output bytes has not matched.<exp:[" +
            convertBytes(expMapOutputBytes) + "]><act:[" +
            convertBytes(getCounterValue(counters, "MAP_OUTPUT_BYTES")) + "]>", 
            convertBytes(expMapOutputBytes).equals( 
            convertBytes(getCounterValue(counters, "MAP_OUTPUT_BYTES"))));*/

        Assert.assertEquals("Map input records have not matched.<exp:[" +
            expMapInputRecs + "]><act:[" +
            getCounterValue(counters, "MAP_INPUT_RECORDS") + "]>", 
            expMapInputRecs, getCounterValue(counters, "MAP_INPUT_RECORDS"));

        // The below statements have commented due to a bug(MAPREDUCE-2154).
        /*Assert.assertEquals("Map output records have not matched.<exp:[" +
            expMapOutputRecs + "]><act:[" +
            getCounterValue(counters, "MAP_OUTPUT_RECORDS") + "]>", 
            expMapOutputRecs, getCounterValue(counters, "MAP_OUTPUT_RECORDS"));*/

        /*Assert.assertTrue("Reduce input bytes have not matched.<exp:[" +
            convertBytes(expReduceInputBytes) + "]><act:[" +
            convertBytes(getCounterValue(counters,"REDUCE_SHUFFLE_BYTES")) + "]>", 
            convertBytes(expReduceInputBytes).equals( 
            convertBytes(getCounterValue(counters,"REDUCE_SHUFFLE_BYTES"))));*/ 

        /*Assert.assertEquals("Reduce output bytes have not matched.<exp:[" + 
            convertBytes(expReduceOutputBytes) + "]><act:[" +
            convertBytes(getCounterValue(counters,"HDFS_BYTES_WRITTEN")) + "]>", 
            convertBytes(expReduceOutputBytes).equals( 
            convertBytes(getCounterValue(counters,"HDFS_BYTES_WRITTEN"))));*/

        /*Assert.assertEquals("Reduce output records have not matched.<exp:[" + 
            expReduceOutputRecs + "]><act:[" + getCounterValue(counters,
            "REDUCE_OUTPUT_RECORDS") + "]>", 
            expReduceOutputRecs, getCounterValue(counters,
            "REDUCE_OUTPUT_RECORDS"));*/ 
 
         /*Assert.assertEquals("Reduce input records have not matched.<exp:[" + 
            expReduceInputRecs + "]><act:[" + getCounterValue(counters,
            "REDUCE_INPUT_RECORDS") + "]>",
            expReduceInputRecs, 
            getCounterValue(counters,"REDUCE_INPUT_RECORDS"));*/
        LOG.info("Done.");
      }
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
