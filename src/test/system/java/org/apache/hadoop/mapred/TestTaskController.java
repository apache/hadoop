package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController;
import org.apache.hadoop.examples.SleepJob;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Set the invalid configuration to task controller and verify whether the
 * task status of a job.
 */
public class TestTaskController {
  private static final Log LOG = LogFactory.getLog(TestTaskController.class);
  private static Configuration conf = new Configuration();
  private static MRCluster cluster;
  private static JTProtocol remoteJTClient;
  private static JTClient jtClient;
  private static String confFile = "mapred-site.xml";
  @Before
  public void before() throws Exception {
    Hashtable<String,Object> prop = new Hashtable<String,Object>();
    prop.put("mapred.local.dir","/mapred/local");
    prop.put("mapred.map.max.attempts", 1L);
    prop.put("mapreduce.job.complete.cancel.delegation.tokens", false);
    String [] expExcludeList = {"java.net.ConnectException",
       "java.io.IOException"};
    cluster = MRCluster.createCluster(conf);
    cluster.setExcludeExpList(expExcludeList);
    cluster.setUp();
    cluster.restartClusterWithNewConfig(prop, confFile);
    jtClient = cluster.getJTClient();
    remoteJTClient = jtClient.getProxy();
  }

  @After
  public void after() throws Exception {
    cluster.tearDown();
    cluster.restart();
  }
  
  /**
   * Set the invalid mapred local directory location and run the job.
   * Verify whether job has failed or not. 
   * @throws Exception - if an error occurs.
   */
  @Test
  public void testJobStatusForInvalidTaskControllerConf() 
      throws Exception {
    conf = remoteJTClient.getDaemonConf();
    if (conf.get("mapred.task.tracker.task-controller").
            equals("org.apache.hadoop.mapred.LinuxTaskController")) {
      TaskController linuxTC = new LinuxTaskController();
      linuxTC.setConf(conf);
      SleepJob job = new SleepJob();
      job.setConf(conf);
      final JobConf jobConf = job.setupJobConf(2, 1, 4000, 4000, 100, 100);
      JobClient jobClient = jtClient.getClient();
      RunningJob runJob = jobClient.submitJob(jobConf);
      JobID jobId = runJob.getID();
      Assert.assertTrue("Job has not been started for 1 min.", 
              jtClient.isJobStarted(jobId));
      TaskInfo[] taskInfos = remoteJTClient.getTaskInfo(jobId);
      TaskInfo taskInfo = null;
      for (TaskInfo taskinfo : taskInfos) {
        if (!taskinfo.isSetupOrCleanup()) {
          taskInfo = taskinfo;
          break;
        }
      }
      Assert.assertTrue("Task has not been started for 1 min.",
          jtClient.isTaskStarted(taskInfo));
      TaskID tID = TaskID.downgrade(taskInfo.getTaskID());
      TaskAttemptID taskAttID = new TaskAttemptID(tID, 0);
      TaskStatus taskStatus = null;
      int counter = 0;
      while(counter++ < 60) {
        if (taskInfo.getTaskStatus().length > 0) {
           taskStatus = taskInfo.getTaskStatus()[0];
           break;
        }
       taskInfo = remoteJTClient.getTaskInfo(tID);
       UtilsForTests.waitFor(1000);
      }
      while (taskInfo.getTaskStatus()[0].getRunState() == 
        TaskStatus.State.RUNNING) {
        UtilsForTests.waitFor(1000);
        taskInfo = remoteJTClient.getTaskInfo(tID);
      } 
      Assert.assertTrue("Job has not been stopped for 1 min.", 
         jtClient.isJobStopped(jobId));
      JobInfo jobInfo = remoteJTClient.getJobInfo(jobId);
      Assert.assertEquals("Job has not been failed", 
          jobInfo.getStatus().getRunState(), JobStatus.FAILED);
      } else {
        Assert.assertTrue("Linux Task controller not found.", false);
      }
  }
}
