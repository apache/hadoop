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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataInputByteBuffer;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Test;

@SuppressWarnings({"rawtypes"})
public class TestTaskAttemptContainerRequest {

  //WARNING: This test must be the only test in this file.  This is because
  // there is an optimization where the credentials passed in are cached
  // statically so they do not need to be recomputed when creating a new
  // ContainerLaunchContext. if other tests run first this code will cache
  // their credentials and this test will fail trying to look for the
  // credentials it inserted in.

  @Test
  public void testAttemptContainerRequest() throws Exception {
    final Text SECRET_KEY_ALIAS = new Text("secretkeyalias");
    final byte[] SECRET_KEY = ("secretkey").getBytes();
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(1);
    acls.put(ApplicationAccessType.VIEW_APP, "otheruser");
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

    // setup UGI for security so tokens and keys are preserved
    jobConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(jobConf);

    Credentials credentials = new Credentials();
    credentials.addSecretKey(SECRET_KEY_ALIAS, SECRET_KEY);
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
        TaskAttemptImpl.createContainerLaunchContext(acls,
            jobConf, jobToken, taImpl.createRemoteTask(),
            TypeConverter.fromYarn(jobId),
            mock(WrappedJvmID.class), taListener,
            credentials);

    Assert.assertEquals("ACLs mismatch", acls, launchCtx.getApplicationACLs());
    Credentials launchCredentials = new Credentials();

    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(launchCtx.getTokens());
    launchCredentials.readTokenStorageStream(dibb);

    // verify all tokens specified for the task attempt are in the launch context
    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
      Token<? extends TokenIdentifier> launchToken =
          launchCredentials.getToken(token.getService());
      Assert.assertNotNull("Token " + token.getService() + " is missing",
          launchToken);
      Assert.assertEquals("Token " + token.getService() + " mismatch",
          token, launchToken);
    }

    // verify the secret key is in the launch context
    Assert.assertNotNull("Secret key missing",
        launchCredentials.getSecretKey(SECRET_KEY_ALIAS));
    Assert.assertTrue("Secret key mismatch", Arrays.equals(SECRET_KEY,
        launchCredentials.getSecretKey(SECRET_KEY_ALIAS)));
  }

  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }

}
