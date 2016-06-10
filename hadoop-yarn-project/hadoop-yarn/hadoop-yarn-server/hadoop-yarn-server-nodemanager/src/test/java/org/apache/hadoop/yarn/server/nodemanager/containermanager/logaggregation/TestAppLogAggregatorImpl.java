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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Unit tests of AppLogAggregatorImpl class.
 */
public class TestAppLogAggregatorImpl {

  private static final File LOCAL_LOG_DIR = new File("target",
      TestAppLogAggregatorImpl.class.getName() + "-localLogDir");
  private static final File REMOTE_LOG_FILE = new File("target",
      TestAppLogAggregatorImpl.class.getName() + "-remoteLogFile");

  @Before
  public void setUp() throws IOException {
    if(LOCAL_LOG_DIR.exists()) {
      FileUtils.cleanDirectory(LOCAL_LOG_DIR);
    }
    if(REMOTE_LOG_FILE.exists()) {
      FileUtils.cleanDirectory(REMOTE_LOG_FILE);
    }
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(LOCAL_LOG_DIR);
    FileUtils.deleteQuietly(REMOTE_LOG_FILE);
  }

  @Test
  public void testAggregatorWithRetentionPolicyDisabledShouldUploadAllFiles()
      throws Exception {
    final ApplicationId applicationId =
        ApplicationId.newInstance(System.currentTimeMillis(), 0);
    final ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    final ContainerId containerId = ContainerId.newContainerId(attemptId, 0);

    // create artificial log files
    final File appLogDir = new File(LOCAL_LOG_DIR,
        ConverterUtils.toString(applicationId));
    final File containerLogDir = new File(appLogDir,
        ConverterUtils.toString(containerId));
    containerLogDir.mkdirs();
    final Set<File> logFiles = createContainerLogFiles(containerLogDir, 3);

    final long logRetentionSecs = 10000;
    final long recoveredLogInitedTime = -1;

    verifyLogAggregationWithExpectedFiles2DeleteAndUpload(
        applicationId, containerId, logRetentionSecs,
        recoveredLogInitedTime, logFiles, logFiles);
  }

  @Test
  public void testAggregatorWhenNoFileOlderThanRetentionPolicyShouldUploadAll()
      throws IOException {

    final ApplicationId applicationId =
        ApplicationId.newInstance(System.currentTimeMillis(), 0);
    final ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    final ContainerId containerId = ContainerId.newContainerId(attemptId, 0);

    // create artificial log files
    final File appLogDir = new File(LOCAL_LOG_DIR,
        ConverterUtils.toString(applicationId));
    final File containerLogDir = new File(appLogDir,
        ConverterUtils.toString(containerId));
    containerLogDir.mkdirs();
    final Set<File> logFiles = createContainerLogFiles(containerLogDir, 3);

    // set log retention period to 1 week.
    final long logRententionSec = 7 * 24 * 60 * 60;
    final long recoveredLogInitedTimeMillis =
        System.currentTimeMillis() - 60*60;

    verifyLogAggregationWithExpectedFiles2DeleteAndUpload(applicationId,
        containerId, logRententionSec, recoveredLogInitedTimeMillis,
        logFiles, new HashSet<File>());
  }

  @Test
  public void testAggregatorWhenAllFilesOlderThanRetentionShouldUploadNone()
      throws IOException {

    final ApplicationId applicationId =
        ApplicationId.newInstance(System.currentTimeMillis(), 0);
    final ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    final ContainerId containerId = ContainerId.newContainerId(attemptId, 0);

    // create artificial log files
    final File appLogDir = new File(LOCAL_LOG_DIR,
        ConverterUtils.toString(applicationId));
    final File containerLogDir = new File(appLogDir,
        ConverterUtils.toString(containerId));
    containerLogDir.mkdirs();
    final Set<File> logFiles = createContainerLogFiles(containerLogDir, 3);


    final long week = 7 * 24 * 60 * 60;
    final long recoveredLogInitedTimeMillis = System.currentTimeMillis() -
        2*week;
    verifyLogAggregationWithExpectedFiles2DeleteAndUpload(
        applicationId, containerId, week, recoveredLogInitedTimeMillis,
        logFiles, new HashSet<File>());
  }

  /**
   * Create the given number of log files under the container log directory.
   * @param containerLogDir the directory to create container log files
   * @param numOfFiles  the number of log files to create
   * @return the set of log files created
   */
  private static Set<File> createContainerLogFiles(File containerLogDir,
      int numOfFiles) throws IOException {
    assert(numOfFiles >= 0);
    assert(containerLogDir.exists());

    Set<File> containerLogFiles = new HashSet<>();
    for(int i = 0; i < numOfFiles; i++) {
      final File logFile = new File(containerLogDir, "logfile" + i);
      logFile.createNewFile();
      containerLogFiles.add(logFile);
    }
    return containerLogFiles;
  }

  /**
   * Verify if the application log aggregator, configured with given log
   * retention period and the recovered log initialization time of
   * the application, uploads and deletes the set of log files as expected.
   * @param appId    application id
   * @param containerId  container id
   * @param logRetentionSecs log retention period
   * @param recoveredLogInitedTimeMillis recovered log initialization time
   * @param expectedFilesToDelete   the set of files expected to be deleted
   * @param expectedFilesToUpload  the set of files expected to be uploaded.
   */
  public void verifyLogAggregationWithExpectedFiles2DeleteAndUpload(
      ApplicationId appId, ContainerId containerId, long logRetentionSecs,
      long recoveredLogInitedTimeMillis, Set<File> expectedFilesToDelete,
      Set<File> expectedFilesToUpload) throws IOException {

    final Set<String> filesExpected2Delete = new HashSet<>();
    for(File file: expectedFilesToDelete) {
      filesExpected2Delete.add(file.getAbsolutePath());
    }
    final Set<String> filesExpected2Upload = new HashSet<>();
    for(File file: expectedFilesToUpload) {
      filesExpected2Upload.add(file.getAbsolutePath());
    }

    // deletion service with verification to check files to delete
    DeletionService deletionServiceWithExpectedFiles =
        createDeletionServiceWithExpectedFile2Delete(filesExpected2Delete);

    final YarnConfiguration config = new YarnConfiguration();
    config.setLong(
        YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, logRetentionSecs);

    final AppLogAggregatorInTest appLogAggregator =
        createAppLogAggregator(appId, LOCAL_LOG_DIR.getAbsolutePath(),
            config, recoveredLogInitedTimeMillis,
            deletionServiceWithExpectedFiles);
    appLogAggregator.startContainerLogAggregation(
        new ContainerLogContext(containerId, ContainerType.TASK, 0));
    // set app finished flag first
    appLogAggregator.finishLogAggregation();
    appLogAggregator.run();

    // verify uploaded files
    ArgumentCaptor<LogValue> logValCaptor =
        ArgumentCaptor.forClass(LogValue.class);
    verify(appLogAggregator.logWriter).append(any(LogKey.class),
        logValCaptor.capture());
    Set<String> filesUploaded = new HashSet<>();
    LogValue logValue = logValCaptor.getValue();
    for(File file: logValue.getPendingLogFilesToUploadForThisContainer()) {
      filesUploaded.add(file.getAbsolutePath());
    }
    verifyFilesUploaded(filesUploaded , filesExpected2Upload);
  }


  private static void verifyFilesUploaded(Set<String> filesUploaded,
      Set<String> filesExpected) {
    final String errMsgPrefix = "The set of files uploaded are not the same " +
        "as expected";
    if(filesUploaded.size() != filesUploaded.size()) {
      fail(errMsgPrefix + ": actual size: " + filesUploaded.size() + " vs " +
          "expected size: " + filesExpected.size());
    }
    for(String file: filesExpected) {
      if(!filesUploaded.contains(file)) {
        fail(errMsgPrefix + ": expecting " + file);
      }
    }
  }

  private static AppLogAggregatorInTest createAppLogAggregator(
      ApplicationId applicationId, String rootLogDir,
      YarnConfiguration config, long recoveredLogInitedTimeMillis,
      DeletionService deletionServiceWithFilesToExpect)
      throws IOException {

    final Dispatcher dispatcher = createNullDispatcher();
    final NodeId nodeId = NodeId.newInstance("localhost", 0);
    final String userId = "AppLogAggregatorTest";
    final UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(userId);
    final LocalDirsHandlerService dirsService =
        createLocalDirsHandlerService(config, rootLogDir);
    final DeletionService deletionService = deletionServiceWithFilesToExpect;
    final LogAggregationContext logAggregationContext = null;
    final Map<ApplicationAccessType, String> appAcls = new HashMap<>();

    final Context context = createContext(config);
    final FileContext fakeLfs = mock(FileContext.class);
    final Path remoteLogDirForApp = new Path(REMOTE_LOG_FILE.getAbsolutePath());

    return new AppLogAggregatorInTest(dispatcher, deletionService,
        config, applicationId, ugi, nodeId, dirsService,
        remoteLogDirForApp, appAcls, logAggregationContext,
        context, fakeLfs, recoveredLogInitedTimeMillis);
  }

  /**
   * Create a deletionService that verifies the paths of container log files
   * passed to the delete method of DeletionService by AppLogAggregatorImpl.
   * This approach is taken due to lack of support of varargs captor in the
   * current mockito version 1.8.5 (The support is added in 1.10.x).
   **/
  private static DeletionService createDeletionServiceWithExpectedFile2Delete(
      final Set<String> expectedPathsForDeletion) {
    DeletionService deletionServiceWithExpectedFiles = mock(DeletionService
        .class);
    // verify paths passed to first invocation of delete method against
    // expected paths
    doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
          Set<String> paths = new HashSet<>();
          Object[] args = invocationOnMock.getArguments();
          for(int i = 2; i < args.length; i++) {
            Path path = (Path) args[i];
            paths.add(path.toUri().getRawPath());
          }
          verifyFilesToDelete(expectedPathsForDeletion, paths);
          return null;
        }
      }).doNothing().when(deletionServiceWithExpectedFiles).delete(
          any(String.class), any(Path.class), Matchers.<Path>anyVararg());

    return deletionServiceWithExpectedFiles;
  }

  private static void verifyFilesToDelete(Set<String> files2ToDelete,
      Set<String> filesExpected) {
    final String errMsgPrefix = "The set of paths for deletion are not the " +
        "same as expected";
    if(files2ToDelete.size() != filesExpected.size()) {
      fail(errMsgPrefix + ": actual size: " + files2ToDelete.size() + " vs " +
          "expected size: " + filesExpected.size());
    }
    for(String file: filesExpected) {
      if(!files2ToDelete.contains(file)) {
        fail(errMsgPrefix + ": expecting " + file);
      }
    }
  }

  private static Dispatcher createNullDispatcher() {
    return new Dispatcher() {
      @Override
      public EventHandler getEventHandler() {
        return new EventHandler() {
          @Override
          public void handle(Event event) {
            // do nothing
          }
        };
      }

      @Override
      public void register(Class<? extends Enum> eventType,
          EventHandler handler) {
        // do nothing
      }
    };
  }

  private static LocalDirsHandlerService createLocalDirsHandlerService(
      YarnConfiguration conf, final String rootLogDir) {
    LocalDirsHandlerService dirsHandlerService = new LocalDirsHandlerService() {
      @Override
      public List<String> getLogDirsForRead() {
        return new ArrayList<String>() {
          {
            add(rootLogDir);
          }
        };
      }
      @Override
      public List<String> getLogDirsForCleanup() {
        return new ArrayList<String>() {
          {
            add(rootLogDir);
          }
        };
      }
    };

    dirsHandlerService.init(conf);
    // appLogAggregator only calls LocalDirsHandlerServer for local directories
    // so it is ok to not start the service.
    return dirsHandlerService;
  }

  private static Context createContext(YarnConfiguration conf) {
    return new NodeManager.NMContext(
        new NMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInNM(),
        null,
        new ApplicationACLsManager(conf),
        new NMNullStateStoreService(), false);
  }

  private static final class AppLogAggregatorInTest extends
      AppLogAggregatorImpl {

    final DeletionService deletionService;
    final ApplicationId applicationId;
    final LogWriter logWriter;
    final ArgumentCaptor<LogValue> logValue;

    public AppLogAggregatorInTest(Dispatcher dispatcher,
        DeletionService deletionService, Configuration conf,
        ApplicationId appId, UserGroupInformation ugi, NodeId nodeId,
        LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
        Map<ApplicationAccessType, String> appAcls,
        LogAggregationContext logAggregationContext, Context context,
        FileContext lfs, long recoveredLogInitedTime) throws IOException {
      super(dispatcher, deletionService, conf, appId, ugi, nodeId,
          dirsHandler, remoteNodeLogFileForApp, appAcls,
          logAggregationContext, context, lfs, recoveredLogInitedTime);
      this.applicationId = appId;
      this.deletionService = deletionService;

      this.logWriter = getSpiedLogWriter(conf, ugi, remoteNodeLogFileForApp);
      this.logValue = ArgumentCaptor.forClass(LogValue.class);
    }

    @Override
    protected LogWriter createLogWriter() {
      return this.logWriter;
    }

    private LogWriter getSpiedLogWriter(Configuration conf,
        UserGroupInformation ugi, Path remoteAppLogFile) throws IOException {
      return spy(new LogWriter(conf, remoteAppLogFile, ugi));
    }
  }
}