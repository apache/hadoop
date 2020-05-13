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

package org.apache.hadoop.mapreduce.v2.app;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster.RunningAppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


/**
 * Make sure that the job staging directory clean up happens.
 */
 public class TestStagingCleanup {
   
   private Configuration conf = new Configuration();
   private FileSystem fs;
   private String stagingJobDir = "tmpJobDir";
   private Path stagingJobPath = new Path(stagingJobDir);
   private final static RecordFactory recordFactory = RecordFactoryProvider.
       getRecordFactory(null);

   @After
   public void tearDown() {
     conf.setBoolean(MRJobConfig.PRESERVE_FAILED_TASK_FILES, false);
   }

   @Test
   public void testDeletionofStagingOnUnregistrationFailure()
       throws IOException {
     testDeletionofStagingOnUnregistrationFailure(2, false);
     testDeletionofStagingOnUnregistrationFailure(1, false);
   }

   @SuppressWarnings("resource")
   private void testDeletionofStagingOnUnregistrationFailure(
       int maxAttempts, boolean shouldHaveDeleted) throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(0, 1);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     TestMRApp appMaster = new TestMRApp(attemptId, null,
         JobStateInternal.RUNNING, maxAttempts);
     appMaster.crushUnregistration = true;
     appMaster.init(conf);
     appMaster.start();
     appMaster.shutDownJob();
     ((RunningAppContext) appMaster.getContext()).resetIsLastAMRetry();
     if (shouldHaveDeleted) {
       assertTrue(appMaster.isLastAMRetry());
       verify(fs).delete(stagingJobPath, true);
     } else {
       assertFalse(appMaster.isLastAMRetry());
       verify(fs, never()).delete(stagingJobPath, true);
     }
   }

   @Test
   public void testDeletionofStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.RUNNING, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     assertTrue(((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs).delete(stagingJobPath, true);
   }

   @Test (timeout = 30000)
   public void testNoDeletionofStagingOnReboot() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class),anyBoolean())).thenReturn(true);
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.REBOOT, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     //shutdown the job, not the lastRetry
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     assertFalse(((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs, times(0)).delete(stagingJobPath, true);
   }

   // FIXME:
   // Disabled this test because currently, when job state=REBOOT at shutdown 
   // when lastRetry = true in RM view, cleanup will not do. 
   // This will be supported after YARN-2261 completed
//   @Test (timeout = 30000)
   public void testDeletionofStagingOnReboot() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class),anyBoolean())).thenReturn(true);
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
         JobStateInternal.REBOOT, 1); //no retry
     appMaster.init(conf);
     appMaster.start();
     //shutdown the job, is lastRetry
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     assertTrue(((TestMRApp)appMaster).getTestIsLastAMRetry());
     verify(fs).delete(stagingJobPath, true);
   }
   
   @Test (timeout = 30000)
   public void testDeletionofStagingOnKill() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 0);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc);
     appMaster.init(conf);
     //simulate the process being killed
     MRAppMaster.MRAppMasterShutdownHook hook = 
       new MRAppMaster.MRAppMasterShutdownHook(appMaster);
     hook.run();
     verify(fs, times(0)).delete(stagingJobPath, true);
   }

  // FIXME:
  // Disabled this test because currently, when shutdown hook triggered at
  // lastRetry in RM view, cleanup will not do. This should be supported after
  // YARN-2261 completed
//   @Test (timeout = 30000)
   public void testDeletionofStagingOnKillLastTry() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
         0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc); //no retry
     appMaster.init(conf);
     assertTrue("appMaster.isLastAMRetry() is false", appMaster.isLastAMRetry());
     //simulate the process being killed
     MRAppMaster.MRAppMasterShutdownHook hook = 
       new MRAppMaster.MRAppMasterShutdownHook(appMaster);
     hook.run();
     assertTrue("MRAppMaster isn't stopped",
                appMaster.isInState(Service.STATE.STOPPED));
     verify(fs).delete(stagingJobPath, true);
   }

   @Test
   public void testByPreserveFailedStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     // TODO: Decide which failed task files that should
     // be kept are in application log directory.
     // Currently all files are not deleted from staging dir.
     conf.setBoolean(MRJobConfig.PRESERVE_FAILED_TASK_FILES, true);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
             JobStateInternal.FAILED, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     assertTrue(((TestMRApp) appMaster).getTestIsLastAMRetry());
     verify(fs, times(0)).delete(stagingJobPath, true);
   }

   @Test
   public void testPreservePatternMatchedStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     // The staging files that are matched to the pattern
     // should not be deleted
     conf.set(MRJobConfig.PRESERVE_FILES_PATTERN, "JobDir");
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
     ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
             JobStateInternal.RUNNING, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
     appMaster.init(conf);
     appMaster.start();
     appMaster.shutDownJob();
     //test whether notifyIsLastAMRetry called
     assertTrue(((TestMRApp) appMaster).getTestIsLastAMRetry());
     verify(fs, times(0)).delete(stagingJobPath, true);
   }

  @Test
  public void testNotPreserveNotPatternMatchedStaging() throws IOException {
    conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
    conf.set(MRJobConfig.PRESERVE_FILES_PATTERN, "NotMatching");
    fs = mock(FileSystem.class);
    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    //Staging Dir exists
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path stagingDir = MRApps.getStagingAreaDir(conf, user);
    when(fs.exists(stagingDir)).thenReturn(true);
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    JobId jobid = recordFactory.newRecordInstance(JobId.class);
    jobid.setAppId(appId);
    ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
    Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
    MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
            JobStateInternal.RUNNING, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
    appMaster.init(conf);
    appMaster.start();
    appMaster.shutDownJob();
    //test whether notifyIsLastAMRetry called
    assertTrue(((TestMRApp) appMaster).getTestIsLastAMRetry());
    //Staging dir should be deleted because it is not matched with
    //PRESERVE_FILES_PATTERN
    verify(fs, times(1)).delete(stagingJobPath, true);
  }

  @Test
  public void testPreservePatternMatchedAndFailedStaging() throws IOException {
    conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
    // When RESERVE_FILES_PATTERN and PRESERVE_FAILED_TASK_FILES are set,
    // files in staging dir are always kept.
    conf.set(MRJobConfig.PRESERVE_FILES_PATTERN, "JobDir");
    conf.setBoolean(MRJobConfig.PRESERVE_FAILED_TASK_FILES, true);
    fs = mock(FileSystem.class);
    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    //Staging Dir exists
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path stagingDir = MRApps.getStagingAreaDir(conf, user);
    when(fs.exists(stagingDir)).thenReturn(true);
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 0);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    JobId jobid = recordFactory.newRecordInstance(JobId.class);
    jobid.setAppId(appId);
    ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
    Assert.assertTrue(MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS > 1);
    MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc,
            JobStateInternal.RUNNING, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
    appMaster.init(conf);
    appMaster.start();
    appMaster.shutDownJob();
    //test whether notifyIsLastAMRetry called
    assertTrue(((TestMRApp) appMaster).getTestIsLastAMRetry());
    verify(fs, times(0)).delete(stagingJobPath, true);
  }

   private class TestMRApp extends MRAppMaster {
     ContainerAllocator allocator;
     boolean testIsLastAMRetry = false;
     JobStateInternal jobStateInternal;
     boolean crushUnregistration = false;

     public TestMRApp(ApplicationAttemptId applicationAttemptId, 
         ContainerAllocator allocator) {
       super(applicationAttemptId, ContainerId.newContainerId(
           applicationAttemptId, 1), "testhost", 2222, 3333,
           System.currentTimeMillis());
       this.allocator = allocator;
       this.successfullyUnregistered.set(true);
     }

     public TestMRApp(ApplicationAttemptId applicationAttemptId,
         ContainerAllocator allocator, JobStateInternal jobStateInternal,
             int maxAppAttempts) {
       this(applicationAttemptId, allocator);
       this.jobStateInternal = jobStateInternal;
     }

     @Override
     protected FileSystem getFileSystem(Configuration conf) {
       return fs;
     }

     @Override
     protected ContainerAllocator createContainerAllocator(
         final ClientService clientService, final AppContext context) {
       if(allocator == null) {
         if (crushUnregistration) {
           return new CustomContainerAllocator(context);
         } else {
           return super.createContainerAllocator(clientService, context);
         }
       }
       return allocator;
     }

     @Override
     protected Job createJob(Configuration conf, JobStateInternal forcedState,
         String diagnostic) {
       JobImpl jobImpl = mock(JobImpl.class);
       when(jobImpl.getInternalState()).thenReturn(this.jobStateInternal);
       when(jobImpl.getAllCounters()).thenReturn(new Counters());
       JobID jobID = JobID.forName("job_1234567890000_0001");
       JobId jobId = TypeConverter.toYarn(jobID);
       when(jobImpl.getID()).thenReturn(jobId);
       ((AppContext) getContext())
           .getAllJobs().put(jobImpl.getID(), jobImpl);
       return jobImpl;
     }

     @Override
     public void serviceStart() throws Exception {
       super.serviceStart();
       DefaultMetricsSystem.shutdown();
     }

     @Override
     public void notifyIsLastAMRetry(boolean isLastAMRetry){
       testIsLastAMRetry = isLastAMRetry;
       super.notifyIsLastAMRetry(isLastAMRetry);
     }

     @Override
     public RMHeartbeatHandler getRMHeartbeatHandler() {
       return getStubbedHeartbeatHandler(getContext());
     }

     @Override
     protected void sysexit() {      
     }

     @Override
     public Configuration getConfig() {
       return conf;
     }

     @Override
     protected void initJobCredentialsAndUGI(Configuration conf) {
     }

     public boolean getTestIsLastAMRetry(){
       return testIsLastAMRetry;
     }

    private class CustomContainerAllocator extends RMCommunicator
        implements ContainerAllocator {

      public CustomContainerAllocator(AppContext context) {
        super(null, context);
      }

      @Override
      public void serviceInit(Configuration conf) {
      }

      @Override
      public void serviceStart() {
      }

      @Override
      public void serviceStop() {
        unregister();
      }

      @Override
      protected void doUnregistration()
          throws YarnException, IOException, InterruptedException {
        throw new YarnException("test exception");
      }

      @Override
      protected void heartbeat() throws Exception {
      }

      @Override
      public void handle(ContainerAllocatorEvent event) {
      }
    }
   }

  private final class MRAppTestCleanup extends MRApp {
    int stagingDirCleanedup;
    int ContainerAllocatorStopped;
    int numStops;
    public MRAppTestCleanup(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
      stagingDirCleanedup = 0;
      ContainerAllocatorStopped = 0;
      numStops = 0;
    }

    @Override
    protected Job createJob(Configuration conf, JobStateInternal forcedState, 
        String diagnostic) {
      UserGroupInformation currentUser = null;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
      Job newJob = new TestJob(getJobId(), getAttemptID(), conf,
          getDispatcher().getEventHandler(),
          getTaskAttemptListener(), getContext().getClock(),
          getCommitter(), isNewApiCommitter(),
          currentUser.getUserName(), getContext(),
          forcedState, diagnostic);
      ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);

      getDispatcher().register(JobFinishEvent.Type.class,
          createJobFinishEventHandler());

      return newJob;
    }

    @Override
    protected ContainerAllocator createContainerAllocator(
        ClientService clientService, AppContext context) {
      return new TestCleanupContainerAllocator();
    }

    private class TestCleanupContainerAllocator extends AbstractService
        implements ContainerAllocator {
      private MRAppContainerAllocator allocator;

      TestCleanupContainerAllocator() {
        super(TestCleanupContainerAllocator.class.getName());
        allocator = new MRAppContainerAllocator();
      }

      @Override
      public void handle(ContainerAllocatorEvent event) {
        allocator.handle(event);
      }

      @Override
      protected void serviceStop() throws Exception {
        numStops++;
        ContainerAllocatorStopped = numStops;
        super.serviceStop();
      }
    }

    @Override
    public RMHeartbeatHandler getRMHeartbeatHandler() {
      return getStubbedHeartbeatHandler(getContext());
    }

    @Override
    public void cleanupStagingDir() throws IOException {
      numStops++;
      stagingDirCleanedup = numStops;
    }

    @Override
    protected void sysexit() {
    }
  }

  private static RMHeartbeatHandler getStubbedHeartbeatHandler(
      final AppContext appContext) {
    return new RMHeartbeatHandler() {
      @Override
      public long getLastHeartbeatTime() {
        return appContext.getClock().getTime();
      }
      @Override
      public void runOnNextHeartbeat(Runnable callback) {
        callback.run();
      }
    };
  }

  @Test(timeout=20000)
  public void testStagingCleanupOrder() throws Exception {
    MRAppTestCleanup app = new MRAppTestCleanup(1, 1, true,
        this.getClass().getName(), true);
    JobImpl job = (JobImpl)app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    int waitTime = 20 * 1000;
    while (waitTime > 0 && app.numStops < 2) {
      Thread.sleep(100);
      waitTime -= 100;
    }

    // assert ContainerAllocatorStopped and then tagingDirCleanedup
    Assert.assertEquals(1, app.ContainerAllocatorStopped);
    Assert.assertEquals(2, app.stagingDirCleanedup);
  }
 }
