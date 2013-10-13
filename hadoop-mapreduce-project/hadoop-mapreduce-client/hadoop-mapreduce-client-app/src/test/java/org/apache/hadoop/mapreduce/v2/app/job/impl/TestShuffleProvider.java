/**
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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.junit.Assert;

public class TestShuffleProvider {

  @Test
  public void testShuffleProviders() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    JobId jobId = MRBuilderUtils.newJobId(appId, 1);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 1, TaskType.MAP);
    Path jobFile = mock(Path.class);

    EventHandler eventHandler = mock(EventHandler.class);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(new InetSocketAddress("localhost", 0));

    JobConf jobConf = new JobConf();
    jobConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    jobConf.setBoolean("fs.file.impl.disable.cache", true);
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "");

    jobConf.set(YarnConfiguration.NM_AUX_SERVICES,
      TestShuffleHandler1.MAPREDUCE_TEST_SHUFFLE_SERVICEID + "," +
      TestShuffleHandler2.MAPREDUCE_TEST_SHUFFLE_SERVICEID);

    String serviceName = TestShuffleHandler1.MAPREDUCE_TEST_SHUFFLE_SERVICEID;
    String serviceStr = String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, serviceName);
    jobConf.set(serviceStr, TestShuffleHandler1.class.getName());

    serviceName = TestShuffleHandler2.MAPREDUCE_TEST_SHUFFLE_SERVICEID;
    serviceStr = String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, serviceName);
    jobConf.set(serviceStr, TestShuffleHandler2.class.getName());

    jobConf.set(MRJobConfig.MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES,
                  TestShuffleHandler1.MAPREDUCE_TEST_SHUFFLE_SERVICEID
                     + "," + TestShuffleHandler2.MAPREDUCE_TEST_SHUFFLE_SERVICEID);

    Credentials credentials = new Credentials();
    Token<JobTokenIdentifier> jobToken = new Token<JobTokenIdentifier>(
        ("tokenid").getBytes(), ("tokenpw").getBytes(),
        new Text("tokenkind"), new Text("tokenservice"));
    TaskAttemptImpl taImpl =
        new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 1,
            mock(TaskSplitMetaInfo.class), jobConf, taListener,
            jobToken, credentials,
            new SystemClock(), null);

    jobConf.set(MRJobConfig.APPLICATION_ATTEMPT_ID, taImpl.getID().toString());

    ContainerLaunchContext launchCtx =
        TaskAttemptImpl.createContainerLaunchContext(null,
            jobConf, jobToken, taImpl.createRemoteTask(),
            TypeConverter.fromYarn(jobId),
            mock(WrappedJvmID.class), taListener,
            credentials);

    Map<String, ByteBuffer> serviceDataMap = launchCtx.getServiceData();
    Assert.assertNotNull("TestShuffleHandler1 is missing", serviceDataMap.get(TestShuffleHandler1.MAPREDUCE_TEST_SHUFFLE_SERVICEID));
    Assert.assertNotNull("TestShuffleHandler2 is missing", serviceDataMap.get(TestShuffleHandler2.MAPREDUCE_TEST_SHUFFLE_SERVICEID));
    Assert.assertTrue("mismatch number of services in map", serviceDataMap.size() == 3); // 2 that we entered + 1 for the built-in shuffle-provider
  }

  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }

  static public class TestShuffleHandler1 extends AuxiliaryService {
    public static final String MAPREDUCE_TEST_SHUFFLE_SERVICEID = "test_shuffle1";
    public TestShuffleHandler1() {
      super("testshuffle1");
    }
    @Override
    public void initializeApplication(ApplicationInitializationContext context) {
    }
    @Override
    public void stopApplication(ApplicationTerminationContext context) {
    }
    @Override
    public synchronized ByteBuffer getMetaData() {
      return ByteBuffer.allocate(0); // Don't 'return null' because of YARN-1256
    }
  }

  static public class TestShuffleHandler2 extends AuxiliaryService {
    public static final String MAPREDUCE_TEST_SHUFFLE_SERVICEID = "test_shuffle2";
    public TestShuffleHandler2() {
      super("testshuffle2");
    }
    @Override
    public void initializeApplication(ApplicationInitializationContext context) {
    }
    @Override
    public void stopApplication(ApplicationTerminationContext context) {
    }
    @Override
    public synchronized ByteBuffer getMetaData() {
      return ByteBuffer.allocate(0); // Don't 'return null' because of YARN-1256
    }
  }
}
