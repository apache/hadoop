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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRAppMaster {
  private static final Log LOG = LogFactory.getLog(TestMRAppMaster.class);
  static String stagingDir = "staging/";
  private static FileContext localFS = null;
  private static final File testDir = new File("target",
    TestMRAppMaster.class.getName() + "-tmpDir").getAbsoluteFile();
  
  @BeforeClass
  public static void setup() throws AccessControlException,
      FileNotFoundException, IllegalArgumentException, IOException {
    //Do not error out if metrics are inited multiple times
    DefaultMetricsSystem.setMiniClusterMode(true);
    File dir = new File(stagingDir);
    stagingDir = dir.getAbsolutePath();
    localFS = FileContext.getLocalFSFileContext();
    localFS.delete(new Path(testDir.getAbsolutePath()), true);
    testDir.mkdir();
  }
  
  @Before
  public void cleanup() throws IOException {
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
  }
  
  @Test
  public void testMRAppMasterForDifferentUser() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000001";
    String containerIdStr = "container_1317529182569_0004_000001_1";
    
    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMasterTest appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS);
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    Path userPath = new Path(stagingDir, userName);
    Path userStagingPath = new Path(userPath, ".staging");
    assertEquals(userStagingPath.toString(),
      appMaster.stagingDirPath.toString());
  }
  
  @Test
  public void testMRAppMasterMidLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    //Create the file, but no end file so we should unregister with an error.
    fs.create(start).close();
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS,
            false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.ERROR, appMaster.forcedState);
    appMaster.stop();
  }
  
  @Test
  public void testMRAppMasterSuccessLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    Path end = MRApps.getEndJobCommitSuccessFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    fs.create(start).close();
    fs.create(end).close();
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS,
            false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.SUCCEEDED, appMaster.forcedState);
    appMaster.stop();
  }
  
  @Test
  public void testMRAppMasterFailLock() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    JobId jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    Path start = MRApps.getStartJobCommitFile(conf, userName, jobId);
    Path end = MRApps.getEndJobCommitFailureFile(conf, userName, jobId);
    FileSystem fs = FileSystem.get(conf);
    fs.create(start).close();
    fs.create(end).close();
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS,
            false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    assertEquals(JobStateInternal.FAILED, appMaster.forcedState);
    appMaster.stop();
  }
  
  @Test
  public void testMRAppMasterMissingStaging() throws IOException,
      InterruptedException {
    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";
    String userName = "TestAppMasterUser";
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);

    //Delete the staging directory
    File dir = new File(stagingDir);
    if(dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    MRAppMaster appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
            System.currentTimeMillis(), MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS,
            false, false);
    boolean caught = false;
    try {
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
    } catch (IOException e) {
      //The IO Exception is expected
      LOG.info("Caught expected Exception", e);
      caught = true;
    }
    assertTrue(caught);
    assertTrue(appMaster.errorHappenedShutDown);
    //Copying the history file is disabled, but it is not really visible from 
    //here
    assertEquals(JobStateInternal.ERROR, appMaster.forcedState);
    appMaster.stop();
  }

  @Test (timeout = 30000)
  public void testMRAppMasterMaxAppAttempts() throws IOException,
      InterruptedException {
    int[] maxAppAttemtps = new int[] { 1, 2, 3 };
    Boolean[] expectedBools = new Boolean[]{ true, true, false };

    String applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
    String containerIdStr = "container_1317529182569_0004_000002_1";

    String userName = "TestAppMasterUser";
    ApplicationAttemptId applicationAttemptId = ConverterUtils
        .toApplicationAttemptId(applicationAttemptIdStr);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, stagingDir);

    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();
    for (int i = 0; i < maxAppAttemtps.length; ++i) {
      MRAppMasterTest appMaster =
          new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
              System.currentTimeMillis(), maxAppAttemtps[i], false, true);
      MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);
      assertEquals("isLastAMRetry is correctly computed.", expectedBools[i],
          appMaster.isLastAMRetry());
    }
  }

  // A dirty hack to modify the env of the current JVM itself - Dirty, but
  // should be okay for testing.
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void setNewEnvironmentHack(Map<String, String> newenv)
      throws Exception {
    try {
      Class<?> cl = Class.forName("java.lang.ProcessEnvironment");
      Field field = cl.getDeclaredField("theEnvironment");
      field.setAccessible(true);
      Map<String, String> env = (Map<String, String>) field.get(null);
      env.clear();
      env.putAll(newenv);
      Field ciField = cl.getDeclaredField("theCaseInsensitiveEnvironment");
      ciField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) ciField.get(null);
      cienv.clear();
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }

  @Test
  public void testMRAppMasterCredentials() throws Exception {

    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);

    // Simulate credentials passed to AM via client->RM->NM
    Credentials credentials = new Credentials();
    byte[] identifier = "MyIdentifier".getBytes();
    byte[] password = "MyPassword".getBytes();
    Text kind = new Text("MyTokenKind");
    Text service = new Text("host:port");
    Token<? extends TokenIdentifier> myToken =
        new Token<TokenIdentifier>(identifier, password, kind, service);
    Text tokenAlias = new Text("myToken");
    credentials.addToken(tokenAlias, myToken);
    Token<? extends TokenIdentifier> storedToken =
        credentials.getToken(tokenAlias);

    YarnConfiguration conf = new YarnConfiguration();

    Path tokenFilePath = new Path(testDir.getAbsolutePath(), "tokens-file");
    Map<String, String> newEnv = new HashMap<String, String>();
    newEnv.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, tokenFilePath
      .toUri().getPath());
    setNewEnvironmentHack(newEnv);
    credentials.writeTokenStorageFile(tokenFilePath, conf);

    ApplicationId appId = BuilderUtils.newApplicationId(12345, 56);
    ApplicationAttemptId applicationAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId containerId =
        BuilderUtils.newContainerId(applicationAttemptId, 546);
    String userName = UserGroupInformation.getCurrentUser().getShortUserName();

    // Create staging dir, so MRAppMaster doesn't barf.
    File stagingDir =
        new File(MRApps.getStagingAreaDir(conf, userName).toString());
    stagingDir.mkdirs();

    // Set login-user to null as that is how real world MRApp starts with.
    // This is null is the reason why token-file is read by UGI.
    UserGroupInformation.setLoginUser(null);

    MRAppMasterTest appMaster =
        new MRAppMasterTest(applicationAttemptId, containerId, "host", -1, -1,
          System.currentTimeMillis(), 1, false, true);
    MRAppMaster.initAndStartAppMaster(appMaster, conf, userName);

    // Now validate the credentials
    Credentials appMasterCreds = appMaster.credentials;
    Assert.assertNotNull(appMasterCreds);
    Token<? extends TokenIdentifier> usedToken =
        appMasterCreds.getToken(tokenAlias);
    Assert.assertNotNull(usedToken);
    Assert
      .assertEquals("MyIdentifier", new String(storedToken.getIdentifier()));
    Assert.assertEquals("MyPassword", new String(storedToken.getPassword()));
    Assert.assertEquals("MyTokenKind", storedToken.getKind().toString());
    Assert.assertEquals("host:port", storedToken.getService().toString());
  }
}

class MRAppMasterTest extends MRAppMaster {

  Path stagingDirPath;
  private Configuration conf;
  private boolean overrideInit;
  private boolean overrideStart;
  ContainerAllocator mockContainerAllocator;
  CommitterEventHandler mockCommitterEventHandler;
  RMHeartbeatHandler mockRMHeartbeatHandler;
  Credentials credentials;

  public MRAppMasterTest(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime, int maxAppAttempts) {
    this(applicationAttemptId, containerId, host, port, httpPort,
        submitTime, maxAppAttempts, true, true);
  }
  public MRAppMasterTest(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String host, int port, int httpPort,
      long submitTime, int maxAppAttempts, boolean overrideInit,
      boolean overrideStart) {
    super(applicationAttemptId, containerId, host, port, httpPort, submitTime,
        maxAppAttempts);
    this.overrideInit = overrideInit;
    this.overrideStart = overrideStart;
    mockContainerAllocator = mock(ContainerAllocator.class);
    mockCommitterEventHandler = mock(CommitterEventHandler.class);
    mockRMHeartbeatHandler = mock(RMHeartbeatHandler.class);
  }

  @Override
  public void init(Configuration conf) {
    if (!overrideInit) {
      super.init(conf);
    }
    this.conf = conf;
  }
  
  @Override 
  protected void downloadTokensAndSetupUGI(Configuration conf) {
    try {
      this.currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }
  
  @Override
  protected ContainerAllocator createContainerAllocator(
      final ClientService clientService, final AppContext context) {
    return mockContainerAllocator;
  }

  @Override
  protected EventHandler<CommitterEvent> createCommitterEventHandler(
      AppContext context, OutputCommitter committer) {
    return mockCommitterEventHandler;
  }

  @Override
  protected RMHeartbeatHandler getRMHeartbeatHandler() {
    return mockRMHeartbeatHandler;
  }

  @Override
  public void start() {
    if (overrideStart) {
      try {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String user = ugi.getShortUserName();
        this.credentials = ugi.getCredentials();
        stagingDirPath = MRApps.getStagingAreaDir(conf, user);
      } catch (Exception e) {
        fail(e.getMessage());
      }
    } else {
      super.start();
    }
  }

}
