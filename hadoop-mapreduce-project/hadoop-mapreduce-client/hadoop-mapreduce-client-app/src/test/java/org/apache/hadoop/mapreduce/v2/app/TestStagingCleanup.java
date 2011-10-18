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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
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
   private static final Log LOG = LogFactory.getLog(TestStagingCleanup.class);
   
   @Test
   public void testDeletionofStaging() throws IOException {
     conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, stagingJobDir);
     fs = mock(FileSystem.class);
     when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
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
     EventHandler<JobFinishEvent> handler = 
         appMaster.createJobFinishEventHandler();
     handler.handle(new JobFinishEvent(jobid));
     verify(fs).delete(stagingJobPath, true);
   }

   private class TestMRApp extends MRAppMaster {

    public TestMRApp(ApplicationAttemptId applicationAttemptId) {
      super(applicationAttemptId);
    }
     
    @Override
    protected FileSystem getFileSystem(Configuration conf) {
      return fs;
    }
    
    @Override
    protected void sysexit() {      
    }
    
    @Override
    public Configuration getConfig() {
      return conf;
    }
   }

 }