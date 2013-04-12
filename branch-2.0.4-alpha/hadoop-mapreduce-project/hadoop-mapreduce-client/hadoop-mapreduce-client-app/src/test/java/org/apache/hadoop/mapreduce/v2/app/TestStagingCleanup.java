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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;


/**
 * Make sure that the job staging directory clean up happens.
 */
 public class TestStagingCleanup extends TestCase {
   
   private Configuration conf = new Configuration();
   private FileSystem fs;
   private String stagingJobDir = "tmpJobDir";
   private Path stagingJobPath = new Path(stagingJobDir);
   private final static RecordFactory recordFactory = RecordFactoryProvider.
       getRecordFactory(null);
   
   @Test
   public void testDeletionofStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationAttemptId attemptId = recordFactory.newRecordInstance(
         ApplicationAttemptId.class);
     attemptId.setAttemptId(0);
     ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
     appId.setClusterTimestamp(System.currentTimeMillis());
     appId.setId(0);
     attemptId.setApplicationId(appId);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     MRAppMaster appMaster = new TestMRApp(attemptId);
     appMaster.init(conf);
     appMaster.shutDownJob();
     verify(fs).delete(stagingJobPath, true);
   }
   
   @Test
   public void testDeletionofStagingOnKill() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     conf.setInt(YarnConfiguration.RM_AM_MAX_RETRIES, 4);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationAttemptId attemptId = recordFactory.newRecordInstance(
         ApplicationAttemptId.class);
     attemptId.setAttemptId(0);
     ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
     appId.setClusterTimestamp(System.currentTimeMillis());
     appId.setId(0);
     attemptId.setApplicationId(appId);
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
   
   @Test
   public void testDeletionofStagingOnKillLastTry() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     conf.setInt(YarnConfiguration.RM_AM_MAX_RETRIES, 1);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
     //Staging Dir exists
     String user = UserGroupInformation.getCurrentUser().getShortUserName();
     Path stagingDir = MRApps.getStagingAreaDir(conf, user);
     when(fs.exists(stagingDir)).thenReturn(true);
     ApplicationAttemptId attemptId = recordFactory.newRecordInstance(
         ApplicationAttemptId.class);
     attemptId.setAttemptId(1);
     ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
     appId.setClusterTimestamp(System.currentTimeMillis());
     appId.setId(0);
     attemptId.setApplicationId(appId);
     JobId jobid = recordFactory.newRecordInstance(JobId.class);
     jobid.setAppId(appId);
     ContainerAllocator mockAlloc = mock(ContainerAllocator.class);
     MRAppMaster appMaster = new TestMRApp(attemptId, mockAlloc);
     appMaster.init(conf);
     //simulate the process being killed
     MRAppMaster.MRAppMasterShutdownHook hook = 
       new MRAppMaster.MRAppMasterShutdownHook(appMaster);
     hook.run();
     verify(fs).delete(stagingJobPath, true);
   }

   private class TestMRApp extends MRAppMaster {
     ContainerAllocator allocator;

     public TestMRApp(ApplicationAttemptId applicationAttemptId, 
         ContainerAllocator allocator) {
       super(applicationAttemptId, BuilderUtils.newContainerId(
           applicationAttemptId, 1), "testhost", 2222, 3333, System
           .currentTimeMillis());
       this.allocator = allocator;
     }

     public TestMRApp(ApplicationAttemptId applicationAttemptId) {
       this(applicationAttemptId, null);
     }

     @Override
     protected FileSystem getFileSystem(Configuration conf) {
       return fs;
     }

     @Override
     protected ContainerAllocator createContainerAllocator(
         final ClientService clientService, final AppContext context) {
       if(allocator == null) {
         return super.createContainerAllocator(clientService, context);
       }
       return allocator;
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
     protected void downloadTokensAndSetupUGI(Configuration conf) {
     }

   }

  private final class MRAppTestCleanup extends MRApp {
    boolean stoppedContainerAllocator;
    boolean cleanedBeforeContainerAllocatorStopped;

    public MRAppTestCleanup(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
      stoppedContainerAllocator = false;
      cleanedBeforeContainerAllocatorStopped = false;
    }

    @Override
    protected Job createJob(Configuration conf, JobStateInternal forcedState, 
        String diagnostic) {
      UserGroupInformation currentUser = null;
      try {
        currentUser = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new YarnException(e);
      }
      Job newJob = new TestJob(getJobId(), getAttemptID(), conf,
          getDispatcher().getEventHandler(),
          getTaskAttemptListener(), getContext().getClock(),
          isNewApiCommitter(), currentUser.getUserName(), getContext(),
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
      public synchronized void stop() {
        stoppedContainerAllocator = true;
        super.stop();
      }
    }

    @Override
    public RMHeartbeatHandler getRMHeartbeatHandler() {
      return getStubbedHeartbeatHandler(getContext());
    }

    @Override
    public void cleanupStagingDir() throws IOException {
      cleanedBeforeContainerAllocatorStopped = !stoppedContainerAllocator;
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

  @Test
  public void testStagingCleanupOrder() throws Exception {
    MRAppTestCleanup app = new MRAppTestCleanup(1, 1, true,
        this.getClass().getName(), true);
    JobImpl job = (JobImpl)app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    int waitTime = 20 * 1000;
    while (waitTime > 0 && !app.cleanedBeforeContainerAllocatorStopped) {
      Thread.sleep(100);
      waitTime -= 100;
    }
    Assert.assertTrue("Staging directory not cleaned before notifying RM",
        app.cleanedBeforeContainerAllocatorStopped);
  }
 }
