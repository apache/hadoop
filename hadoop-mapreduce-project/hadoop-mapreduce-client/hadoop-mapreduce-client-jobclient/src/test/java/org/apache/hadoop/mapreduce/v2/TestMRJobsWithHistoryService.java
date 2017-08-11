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

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.junit.Assert;

import org.apache.avro.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMRJobsWithHistoryService {

  private static final Log LOG =
    LogFactory.getLog(TestMRJobsWithHistoryService.class);

  private static final EnumSet<RMAppState> TERMINAL_RM_APP_STATES =
    EnumSet.of(RMAppState.FINISHED, RMAppState.FAILED, RMAppState.KILLED);

  private static MiniMRYarnCluster mrCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR = localFs.makeQualified(
      new Path("target", TestMRJobs.class.getName() + "-tmpDir"));
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  @Before
  public void setup() throws InterruptedException, IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(getClass().getName());
      mrCluster.init(new Configuration());
      mrCluster.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @After
  public void tearDown() {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster != null) {
      mrCluster.stop();
    }
  }

  @Test (timeout = 90000)
  public void testJobHistoryData() throws IOException, InterruptedException,
      AvroRemoteException, ClassNotFoundException {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }


    
    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(mrCluster.getConfig());
    // Job with 3 maps and 2 reduces
    Job job = sleepJob.createJob(3, 2, 1000, 1, 500, 1);
    job.setJarByClass(SleepJob.class);
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.waitForCompletion(true);
    Counters counterMR = job.getCounters();
    JobId jobId = TypeConverter.toYarn(job.getJobID());
    ApplicationId appID = jobId.getAppId();
    int pollElapsed = 0;
    while (true) {
      Thread.sleep(1000);
      pollElapsed += 1000;

      if (TERMINAL_RM_APP_STATES.contains(
          mrCluster.getResourceManager().getRMContext().getRMApps().get(appID)
          .getState())) {
        break;
      }

      if (pollElapsed >= 60000) {
        LOG.warn("application did not reach terminal state within 60 seconds");
        break;
      }
    }
    Assert.assertEquals(RMAppState.FINISHED, mrCluster.getResourceManager()
      .getRMContext().getRMApps().get(appID).getState());
    Counters counterHS = job.getCounters();
    //TODO the Assert below worked. need to check
    //Should we compare each field or convert to V2 counter and compare
    LOG.info("CounterHS " + counterHS);
    LOG.info("CounterMR " + counterMR);
    Assert.assertEquals(counterHS, counterMR);
    
    HSClientProtocol historyClient = instantiateHistoryProxy();
    GetJobReportRequest gjReq = Records.newRecord(GetJobReportRequest.class);
    gjReq.setJobId(jobId);
    JobReport jobReport = historyClient.getJobReport(gjReq).getJobReport();
    verifyJobReport(jobReport, jobId);
  }

  private void verifyJobReport(JobReport jobReport, JobId jobId) {
    List<AMInfo> amInfos = jobReport.getAMInfos();
    Assert.assertEquals(1, amInfos.size());
    AMInfo amInfo = amInfos.get(0);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(jobId.getAppId(), 1);
    ContainerId amContainerId = ContainerId.newContainerId(appAttemptId, 1);
    Assert.assertEquals(appAttemptId, amInfo.getAppAttemptId());
    Assert.assertEquals(amContainerId, amInfo.getContainerId());
    Assert.assertTrue(jobReport.getSubmitTime() > 0);
    Assert.assertTrue(jobReport.getStartTime() > 0
        && jobReport.getStartTime() >= jobReport.getSubmitTime());
    Assert.assertTrue(jobReport.getFinishTime() > 0
        && jobReport.getFinishTime() >= jobReport.getStartTime());
  }
  
  private HSClientProtocol instantiateHistoryProxy() {
    final String serviceAddr =
        mrCluster.getConfig().get(JHAdminConfig.MR_HISTORY_ADDRESS);
    final YarnRPC rpc = YarnRPC.create(conf);
    HSClientProtocol historyClient =
        (HSClientProtocol) rpc.getProxy(HSClientProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), mrCluster.getConfig());
    return historyClient;
  }
}
