package org.apache.hadoop.mapred;

import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.test.system.FinishTaskControlAction;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.MRCluster;
import org.apache.hadoop.mapreduce.test.system.TTClient;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCluster {

  private static final Log LOG = LogFactory.getLog(TestCluster.class);

  private static MRCluster cluster;

  public TestCluster() throws Exception {
    
  }

  @BeforeClass
  public static void before() throws Exception {
    cluster = MRCluster.createCluster(new Configuration());
    cluster.setUp();
  }

  @AfterClass
  public static void after() throws Exception {
    cluster.tearDown();
  }

  @Test
  public void testProcessInfo() throws Exception {
    LOG.info("Process info of master is : "
        + cluster.getMaster().getProcessInfo());
    Assert.assertNotNull(cluster.getMaster().getProcessInfo());
    Collection<TTClient> slaves = cluster.getSlaves().values();
    for (TTClient slave : slaves) {
      LOG.info("Process info of slave is : " + slave.getProcessInfo());
      Assert.assertNotNull(slave.getProcessInfo());
    }
  }
  
  @Test
  public void testJobSubmission() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getMaster().getProxy();
    JobInfo[] jobs = wovenClient.getAllJobInfo();
    SleepJob job = new SleepJob();
    job.setConf(conf);
    conf = job.setupJobConf(1, 1, 100, 100, 100, 100);
    RunningJob rJob = cluster.getMaster().submitAndVerifyJob(conf);
    cluster.getMaster().verifyJobHistory(rJob.getID());
  }

  @Test
  public void testFileStatus() throws Exception {
    JTClient jt = cluster.getMaster();
    String dir = ".";
    checkFileStatus(jt.getFileStatus(dir, true));
    checkFileStatus(jt.listStatus(dir, false, true), dir);
    for (TTClient tt : cluster.getSlaves().values()) {
      String[] localDirs = tt.getMapredLocalDirs();
      for (String localDir : localDirs) {
        checkFileStatus(tt.listStatus(localDir, true, false), localDir);
        checkFileStatus(tt.listStatus(localDir, true, true), localDir);
      }
    }
    String systemDir = jt.getClient().getSystemDir().toString();
    checkFileStatus(jt.listStatus(systemDir, false, true), systemDir);
    checkFileStatus(jt.listStatus(jt.getLogDir(), true, true), jt.getLogDir());
  }

  private void checkFileStatus(FileStatus[] fs, String path) {
    Assert.assertNotNull(fs);
    LOG.info("-----Listing for " + path + "  " + fs.length);
    for (FileStatus fz : fs) {
      checkFileStatus(fz);
    }
  }

  private void checkFileStatus(FileStatus fz) {
    Assert.assertNotNull(fz);
    LOG.info("FileStatus is " + fz.getPath() 
        + "  " + fz.getPermission()
        +"  " + fz.getOwner()
        +"  " + fz.getGroup()
        +"  " + fz.getClass());
  }

  /**
   * Test to showcase how to get a task status from a TaskTracker.
   * Does the following;
   * 1. Contacts the job tracker to get TaskInfo
   * 2. Uses taskinfo to get list of tts
   * 3. Contacts TT and gets task info.
   * 
   * Care should be taken that the task which you are searching
   * can need not be around.
   * @throws Exception
   */
  @Test
  public void testTaskStatus() throws Exception {
    Configuration conf = new Configuration(cluster.getConf());
    JTProtocol wovenClient = cluster.getMaster().getProxy();
    FinishTaskControlAction.configureControlActionForJob(conf);
    SleepJob job = new SleepJob();
    job.setConf(conf);

    conf = job.setupJobConf(1, 0, 100, 100, 100, 100);
    JobClient client = cluster.getMaster().getClient();

    RunningJob rJob = client.submitJob(new JobConf(conf));
    JobID id = rJob.getID();

    JobInfo jInfo = wovenClient.getJobInfo(id);

    while (jInfo.getStatus().getRunState() != JobStatus.RUNNING) {
      Thread.sleep(1000);
      jInfo = wovenClient.getJobInfo(id);
    }

    LOG.info("Waiting till job starts running one map");

    TaskInfo[] myTaskInfos = wovenClient.getTaskInfo(id);
    for(TaskInfo info : myTaskInfos) {
      if(!info.isSetupOrCleanup()) {
        String[] taskTrackers = info.getTaskTrackers();
        for(String taskTracker : taskTrackers) {
          TTInfo ttInfo = wovenClient.getTTInfo(taskTracker);
          TTClient ttCli =  cluster.getSlaves().get(
              ttInfo.getStatus().getHost());
          TTTaskInfo ttTaskInfo = ttCli.getProxy().getTask(info.getTaskID());
          Assert.assertNotNull(ttTaskInfo);
          FinishTaskControlAction action = new FinishTaskControlAction(
              TaskID.downgrade(info.getTaskID()));
          ttCli.getProxy().sendAction(action);
        }
      }
    }
    rJob.killJob();
  }
}
