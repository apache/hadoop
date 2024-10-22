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

package org.apache.hadoop.yarn.server.nodemanager;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings.AssignedResources;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.MockLocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDefaultContainerExecutor {

  private static Path BASE_TMP_PATH = new Path("target",
      TestDefaultContainerExecutor.class.getSimpleName());

  private YarnConfiguration yarnConfiguration;

  private DefaultContainerExecutor containerExecutor;

  private Container mockContainer;

  private NumaResourceAllocator numaResourceAllocator;

  @AfterClass
  public static void deleteTmpFiles() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    try {
      lfs.delete(BASE_TMP_PATH, true);
    } catch (FileNotFoundException e) {
    }
  }

  byte[] createTmpFile(Path dst, Random r, int len)
      throws IOException {
    // use unmodified local context
    FileContext lfs = FileContext.getLocalFSFileContext();
    dst = lfs.makeQualified(dst);
    lfs.mkdir(dst.getParent(), null, true);
    byte[] bytes = new byte[len];
    FSDataOutputStream out = null;
    try {
      out = lfs.create(dst, EnumSet.of(CREATE, OVERWRITE));
      r.nextBytes(bytes);
      out.write(bytes);
    } finally {
      if (out != null) out.close();
    }
    return bytes;
  }

  @Test
  public void testDirPermissions() throws Exception {
    deleteTmpFiles();

    final String user = "somebody";
    final String appId = "app_12345_123";
    final FsPermission userCachePerm = new FsPermission(
        DefaultContainerExecutor.USER_PERM);
    final FsPermission appCachePerm = new FsPermission(
        DefaultContainerExecutor.APPCACHE_PERM);
    final FsPermission fileCachePerm = new FsPermission(
        DefaultContainerExecutor.FILECACHE_PERM);
    final FsPermission appDirPerm = new FsPermission(
        DefaultContainerExecutor.APPDIR_PERM);

    List<String> localDirs = new ArrayList<String>();
    localDirs.add(new Path(BASE_TMP_PATH, "localDirA").toString());
    localDirs.add(new Path(BASE_TMP_PATH, "localDirB").toString());
    List<String> logDirs = new ArrayList<String>();
    logDirs.add(new Path(BASE_TMP_PATH, "logDirA").toString());
    logDirs.add(new Path(BASE_TMP_PATH, "logDirB").toString());

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    DefaultContainerExecutor executor = new DefaultContainerExecutor(lfs);
    executor.setConf(conf);
    executor.init(null);

    try {
      executor.createUserLocalDirs(localDirs, user);
      executor.createUserCacheDirs(localDirs, user);
      executor.createAppDirs(localDirs, user, appId);

      for (String dir : localDirs) {
        FileStatus stats = lfs.getFileStatus(
            new Path(new Path(dir, ContainerLocalizer.USERCACHE), user));
        Assert.assertEquals(userCachePerm, stats.getPermission());
      }

      for (String dir : localDirs) {
        Path userCachePath = new Path(
            new Path(dir, ContainerLocalizer.USERCACHE), user);
        Path appCachePath = new Path(userCachePath,
            ContainerLocalizer.APPCACHE);
        FileStatus stats = lfs.getFileStatus(appCachePath);
        Assert.assertEquals(appCachePerm, stats.getPermission());
        stats = lfs.getFileStatus(
            new Path(userCachePath, ContainerLocalizer.FILECACHE));
        Assert.assertEquals(fileCachePerm, stats.getPermission());
        stats = lfs.getFileStatus(new Path(appCachePath, appId));
        Assert.assertEquals(appDirPerm, stats.getPermission());
      }

      String[] permissionsArray = { "000", "111", "555", "710", "777" };

      for (String perm : permissionsArray ) {
        conf.set(YarnConfiguration.NM_DEFAULT_CONTAINER_EXECUTOR_LOG_DIRS_PERMISSIONS, perm);
        executor.clearLogDirPermissions();
        FsPermission logDirPerm = new FsPermission(
            executor.getLogDirPermissions());
        executor.createAppLogDirs(appId, logDirs, user);

        for (String dir : logDirs) {
          FileStatus stats = lfs.getFileStatus(new Path(dir, appId));
          Assert.assertEquals(logDirPerm, stats.getPermission());
          lfs.delete(new Path(dir, appId), true);
        }
      }
    } finally {
      deleteTmpFiles();
    }
  }

  private void writeStringToRelativePath(FileContext fc, Path p, String str)
      throws IOException {
    p = p.makeQualified(fc.getDefaultFileSystem().getUri(),
        new Path(new File(".").getAbsolutePath()));
    try (FSDataOutputStream os = fc.create(p).build()) {
      os.writeUTF(str);
    }
  }

  private String readStringFromPath(FileContext fc, Path p) throws IOException {
    try (FSDataInputStream is = fc.open(p)) {
      return is.readUTF();
    }
  }

  @Test
  public void testLaunchContainerCopyFilesWithoutHTTPS() throws Exception {
    testLaunchContainerCopyFiles(false);
  }

  @Test
  public void testLaunchContainerCopyFilesWithHTTPS() throws Exception {
    testLaunchContainerCopyFiles(true);
  }

  private void testLaunchContainerCopyFiles(boolean https) throws Exception {
    if (Shell.WINDOWS) {
      BASE_TMP_PATH =
          new Path(new File("target").getAbsolutePath(),
              TestDefaultContainerExecutor.class.getSimpleName());
    }

    Path localDir = new Path(BASE_TMP_PATH, "localDir");
    List<String> localDirs = new ArrayList<String>();
    localDirs.add(localDir.toString());
    List<String> logDirs = new ArrayList<String>();
    Path logDir = new Path(BASE_TMP_PATH, "logDir");
    logDirs.add(logDir.toString());

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.toString());
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir.toString());

    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    deleteTmpFiles();
    lfs.mkdir(BASE_TMP_PATH, FsPermission.getDefault(), true);
    DefaultContainerExecutor dce = new DefaultContainerExecutor(lfs);
    dce.setConf(conf);

    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String, String>();
    env.put("LANG", "C");

    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.toString()).thenReturn(containerId);
    when(cId.getApplicationAttemptId()).thenReturn(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 0));
    when(context.getEnvironment()).thenReturn(env);

    Path scriptPath = new Path(BASE_TMP_PATH, "script");
    Path tokensPath = new Path(BASE_TMP_PATH, "tokens");
    Path keystorePath = new Path(BASE_TMP_PATH, "keystore");
    Path truststorePath = new Path(BASE_TMP_PATH, "truststore");
    writeStringToRelativePath(lfs, scriptPath, "script");
    writeStringToRelativePath(lfs, tokensPath, "tokens");
    if (https) {
      writeStringToRelativePath(lfs, keystorePath, "keystore");
      writeStringToRelativePath(lfs, truststorePath, "truststore");
    }

    Path workDir = localDir;
    Path pidFile = new Path(workDir, "pid.txt");

    dce.init(null);
    dce.activateContainer(cId, pidFile);
    ContainerStartContext.Builder ctxBuilder =
        new ContainerStartContext.Builder()
            .setContainer(container)
            .setNmPrivateContainerScriptPath(scriptPath)
            .setNmPrivateTokensPath(tokensPath)
            .setUser(appSubmitter)
            .setAppId(appId)
            .setContainerWorkDir(workDir)
            .setLocalDirs(localDirs)
            .setLogDirs(logDirs);
    if (https) {
      ctxBuilder.setNmPrivateTruststorePath(truststorePath)
          .setNmPrivateKeystorePath(keystorePath);
    }
    ContainerStartContext ctx = ctxBuilder.build();

    // #launchContainer will copy a number of files to this directory.
    // Ensure that it doesn't exist first
    lfs.delete(workDir, true);
    try {
      lfs.getFileStatus(workDir);
      Assert.fail("Expected FileNotFoundException on " + workDir);
    } catch (FileNotFoundException e) {
      // expected
    }

    dce.launchContainer(ctx);

    Path finalScriptPath = new Path(workDir,
        ContainerLaunch.CONTAINER_SCRIPT);
    Path finalTokensPath = new Path(workDir,
        ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    Path finalKeystorePath = new Path(workDir,
        ContainerLaunch.KEYSTORE_FILE);
    Path finalTrustorePath = new Path(workDir,
        ContainerLaunch.TRUSTSTORE_FILE);

    Assert.assertTrue(lfs.getFileStatus(workDir).isDirectory());
    Assert.assertTrue(lfs.getFileStatus(finalScriptPath).isFile());
    Assert.assertTrue(lfs.getFileStatus(finalTokensPath).isFile());
    if (https) {
      Assert.assertTrue(lfs.getFileStatus(finalKeystorePath).isFile());
      Assert.assertTrue(lfs.getFileStatus(finalTrustorePath).isFile());
    } else {
      try {
        lfs.getFileStatus(finalKeystorePath);
        Assert.fail("Expected FileNotFoundException on " + finalKeystorePath);
      } catch (FileNotFoundException e) {
        // expected
      }
      try {
        lfs.getFileStatus(finalTrustorePath);
        Assert.fail("Expected FileNotFoundException on " + finalKeystorePath);
      } catch (FileNotFoundException e) {
        // expected
      }
    }

    Assert.assertEquals("script", readStringFromPath(lfs, finalScriptPath));
    Assert.assertEquals("tokens", readStringFromPath(lfs, finalTokensPath));
    if (https) {
      Assert.assertEquals("keystore", readStringFromPath(lfs,
          finalKeystorePath));
      Assert.assertEquals("truststore", readStringFromPath(lfs,
          finalTrustorePath));
    }
  }

  @Test
  public void testContainerLaunchError()
      throws IOException, InterruptedException, ConfigurationException {

    if (Shell.WINDOWS) {
      BASE_TMP_PATH =
          new Path(new File("target").getAbsolutePath(),
            TestDefaultContainerExecutor.class.getSimpleName());
    }

    Path localDir = new Path(BASE_TMP_PATH, "localDir");
    List<String> localDirs = new ArrayList<String>();
    localDirs.add(localDir.toString());
    List<String> logDirs = new ArrayList<String>();
    Path logDir = new Path(BASE_TMP_PATH, "logDir");
    logDirs.add(logDir.toString());

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.toString());
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir.toString());
    
    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    DefaultContainerExecutor mockExec = spy(new DefaultContainerExecutor(lfs));
    mockExec.setConf(conf);
    doAnswer(
        new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            String diagnostics = (String) invocationOnMock.getArguments()[0];
            assertTrue("Invalid Diagnostics message: " + diagnostics,
                diagnostics.contains("No such file or directory"));
            return null;
          }
        }
    ).when(mockExec).logOutput(any(String.class));

    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String, String>();
    env.put("LANG", "C");

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    try {
      doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock)
            throws Throwable {
          ContainerDiagnosticsUpdateEvent event =
              (ContainerDiagnosticsUpdateEvent) invocationOnMock
                  .getArguments()[0];
          assertTrue("Invalid Diagnostics message: "
                  + event.getDiagnosticsUpdate(),
              event.getDiagnosticsUpdate().contains("No such file or directory")
          );
          return null;
        }
      }).when(container).handle(any(ContainerDiagnosticsUpdateEvent.class));

      when(cId.toString()).thenReturn(containerId);
      when(cId.getApplicationAttemptId()).thenReturn(
          ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 0));

      when(context.getEnvironment()).thenReturn(env);

      mockExec.createUserLocalDirs(localDirs, appSubmitter);
      mockExec.createUserCacheDirs(localDirs, appSubmitter);
      mockExec.createAppDirs(localDirs, appSubmitter, appId);
      mockExec.createAppLogDirs(appId, logDirs, appSubmitter);

      Path scriptPath = new Path("file:///bin/echo");
      Path tokensPath = new Path("file:///dev/null");
      Path keystorePath = new Path("file:///dev/null");
      Path truststorePath = new Path("file:///dev/null");
      if (Shell.WINDOWS) {
        File tmp = new File(BASE_TMP_PATH.toString(), "test_echo.cmd");
        BufferedWriter output = new BufferedWriter(new FileWriter(tmp));
        output.write("Exit 1");
        output.write("Echo No such file or directory 1>&2");
        output.close();
        scriptPath = new Path(tmp.getAbsolutePath());
        tmp = new File(BASE_TMP_PATH.toString(), "tokens");
        tmp.createNewFile();
        tokensPath = new Path(tmp.getAbsolutePath());
      }
      Path workDir = localDir;
      Path pidFile = new Path(workDir, "pid.txt");

      mockExec.init(null);
      mockExec.activateContainer(cId, pidFile);
      int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setNmPrivateContainerScriptPath(scriptPath)
          .setNmPrivateTokensPath(tokensPath)
          .setNmPrivateKeystorePath(keystorePath)
          .setNmPrivateTruststorePath(truststorePath)
          .setUser(appSubmitter)
          .setAppId(appId)
          .setContainerWorkDir(workDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .build());
      Assert.assertNotSame(0, ret);
    } finally {
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(localDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(logDir)
          .build());
    }
  }

  @Test(timeout = 30000)
  public void testStartLocalizer() throws IOException, InterruptedException,
      YarnException {

    final Path firstDir = new Path(BASE_TMP_PATH, "localDir1");
    List<String> localDirs = new ArrayList<String>();
    final Path secondDir = new Path(BASE_TMP_PATH, "localDir2");
    List<String> logDirs = new ArrayList<String>();
    final Path logDir = new Path(BASE_TMP_PATH, "logDir");
    final Path tokenDir = new Path(BASE_TMP_PATH, "tokenDir");
    FsPermission perms = new FsPermission((short)0770);

    Configuration conf = new Configuration();

    final FileContext mockLfs = spy(FileContext.getLocalFSFileContext(conf));
    final FileContext.Util mockUtil = spy(mockLfs.util());
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        return mockUtil;
      }
    }).when(mockLfs).util();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        Path dest = (Path) invocationOnMock.getArguments()[1];
        if (dest.toString().contains(firstDir.toString())) {
          // throw an Exception when copy token to the first local dir
          // to simulate no space on the first drive
          throw new IOException("No space on this drive " +
              dest.toString());
        } else {
          // copy token to the second local dir
          DataOutputStream tokenOut = null;
          try {
            Credentials credentials = new Credentials();
            tokenOut = mockLfs.create(dest,
                EnumSet.of(CREATE, OVERWRITE));
            credentials.writeTokenStorageToStream(tokenOut);
          } finally {
            if (tokenOut != null) {
              tokenOut.close();
            }
          }
        }
        return null;
      }}).when(mockUtil).copy(any(Path.class), any(Path.class),
        anyBoolean(), anyBoolean());

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        Path p = (Path) invocationOnMock.getArguments()[0];
        // let second local directory return more free space than
        // first local directory
        if (p.toString().contains(firstDir.toString())) {
          return new FsStatus(2000, 2000, 0);
        } else {
          return new FsStatus(1000, 0, 1000);
        }
      }
    }).when(mockLfs).getFsStatus(any(Path.class));

    DefaultContainerExecutor mockExec =
        spy(new DefaultContainerExecutor(mockLfs) {
          @Override
          public ContainerLocalizer createContainerLocalizer(String user,
              String appId, String locId, String tokenFileName,
              List<String> localDirs, FileContext localizerFc,
              String containerId) throws IOException {

            // Spy on the localizer and make it return valid heart-beat
            // responses even though there is no real NodeManager.
            ContainerLocalizer localizer =
                super.createContainerLocalizer(user, appId, locId,
                    tokenFileName, localDirs, localizerFc, appId);
            // in the above line passing appId in place of container Id as
            // container id is just for logging purposes and has not other
            // use
            ContainerLocalizer spyLocalizer = spy(localizer);
            LocalizationProtocol nmProxy = mock(LocalizationProtocol.class);
            try {
              when(nmProxy.heartbeat(isA(LocalizerStatus.class))).thenReturn(
                  new MockLocalizerHeartbeatResponse(LocalizerAction.DIE,
                      new ArrayList<ResourceLocalizationSpec>()));
            } catch (YarnException e) {
              throw new IOException(e);
            }
            when(spyLocalizer.getProxy(any()))
              .thenReturn(nmProxy);

            return spyLocalizer;
          }
        });
    mockExec.setConf(conf);
    localDirs.add(mockLfs.makeQualified(firstDir).toString());
    localDirs.add(mockLfs.makeQualified(secondDir).toString());
    logDirs.add(mockLfs.makeQualified(logDir).toString());
    conf.setStrings(YarnConfiguration.NM_LOCAL_DIRS,
        localDirs.toArray(new String[localDirs.size()]));
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir.toString());
    mockLfs.mkdir(tokenDir, perms, true);
    Path nmPrivateCTokensPath = new Path(tokenDir, "test.tokens");
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String locId = "LOC_ID";
    
    LocalDirsHandlerService  dirsHandler = mock(LocalDirsHandlerService.class);
    when(dirsHandler.getLocalDirs()).thenReturn(localDirs);
    when(dirsHandler.getLogDirs()).thenReturn(logDirs);

    try {
      mockExec.startLocalizer(new LocalizerStartContext.Builder()
          .setNmPrivateContainerTokens(nmPrivateCTokensPath)
          .setNmAddr(null)
          .setUser(appSubmitter)
          .setAppId(appId)
          .setLocId(locId)
          .setDirsHandler(dirsHandler)
          .build());

    } catch (IOException e) {
      Assert.fail("StartLocalizer failed to copy token file: "
          + StringUtils.stringifyException(e));
    } finally {
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(firstDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(secondDir)
          .build());
      mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(appSubmitter)
          .setSubDir(logDir)
          .build());
      deleteTmpFiles();
    }

    // Verify that the calls happen the expected number of times
    verify(mockUtil, times(1)).copy(any(Path.class), any(Path.class),
        anyBoolean(), anyBoolean());
    verify(mockLfs, times(2)).getFsStatus(any(Path.class));
  }

  @Test
  public void testPickDirectory() throws Exception {
    Configuration conf = new Configuration();
    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    DefaultContainerExecutor executor = new DefaultContainerExecutor(lfs);

    long[] availableOnDisk = new long[2];
    availableOnDisk[0] = 100;
    availableOnDisk[1] = 100;
    assertEquals(0, executor.pickDirectory(0L, availableOnDisk));
    assertEquals(0, executor.pickDirectory(99L, availableOnDisk));
    assertEquals(1, executor.pickDirectory(100L, availableOnDisk));
    assertEquals(1, executor.pickDirectory(101L, availableOnDisk));
    assertEquals(1, executor.pickDirectory(199L, availableOnDisk));

    long[] availableOnDisk2 = new long[5];
    availableOnDisk2[0] = 100;
    availableOnDisk2[1] = 10;
    availableOnDisk2[2] = 400;
    availableOnDisk2[3] = 200;
    availableOnDisk2[4] = 350;
    assertEquals(0, executor.pickDirectory(0L, availableOnDisk2));
    assertEquals(0, executor.pickDirectory(99L, availableOnDisk2));
    assertEquals(1, executor.pickDirectory(100L, availableOnDisk2));
    assertEquals(1, executor.pickDirectory(105L, availableOnDisk2));
    assertEquals(2, executor.pickDirectory(110L, availableOnDisk2));
    assertEquals(2, executor.pickDirectory(259L, availableOnDisk2));
    assertEquals(3, executor.pickDirectory(700L, availableOnDisk2));
    assertEquals(4, executor.pickDirectory(710L, availableOnDisk2));
    assertEquals(4, executor.pickDirectory(910L, availableOnDisk2));
  }

//  @Test
//  public void testInit() throws IOException, InterruptedException {
//    Configuration conf = new Configuration();
//    AbstractFileSystem spylfs =
//      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
//    // don't actually create dirs
//    //doNothing().when(spylfs).mkdir(Matchers.<Path>anyObject(),
//    //    Matchers.<FsPermission>anyObject(), anyBoolean());
//    FileContext lfs = FileContext.getFileContext(spylfs, conf);
//
//    Path basedir = new Path("target",
//        TestDefaultContainerExecutor.class.getSimpleName());
//    List<String> localDirs = new ArrayList<String>();
//    List<Path> localPaths = new ArrayList<Path>();
//    for (int i = 0; i < 4; ++i) {
//      Path p = new Path(basedir, i + "");
//      lfs.mkdir(p, null, true);
//      localPaths.add(p);
//      localDirs.add(p.toString());
//    }
//    final String user = "yak";
//    final String appId = "app_RM_0";
//    final Path logDir = new Path(basedir, "logs");
//    final Path nmLocal = new Path(basedir, "nmPrivate/" + user + "/" + appId);
//    final InetSocketAddress nmAddr = new InetSocketAddress("foobar", 8040);
//    System.out.println("NMLOCAL: " + nmLocal);
//    Random r = new Random();
//
//    /*
//    // XXX FileContext cannot be reasonably mocked to do this
//    // mock jobFiles copy
//    long fileSeed = r.nextLong();
//    r.setSeed(fileSeed);
//    System.out.println("SEED: " + seed);
//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
//    DataOutputBuffer fileCacheBytes = mockStream(spylfs, fileCachePath, r, 512);
//
//    // mock jobTokens copy
//    long jobSeed = r.nextLong();
//    r.setSeed(jobSeed);
//    System.out.println("SEED: " + seed);
//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
//    DataOutputBuffer jobTokenBytes = mockStream(spylfs, jobTokenPath, r, 512);
//    */
//
//    // create jobFiles
//    long fileSeed = r.nextLong();
//    r.setSeed(fileSeed);
//    System.out.println("SEED: " + fileSeed);
//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
//    byte[] fileCacheBytes = createTmpFile(fileCachePath, r, 512);
//
//    // create jobTokens
//    long jobSeed = r.nextLong();
//    r.setSeed(jobSeed);
//    System.out.println("SEED: " + jobSeed);
//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
//    byte[] jobTokenBytes = createTmpFile(jobTokenPath, r, 512);
//
//    DefaultContainerExecutor dce = new DefaultContainerExecutor(lfs);
//    Localization mockLocalization = mock(Localization.class);
//    ApplicationLocalizer spyLocalizer =
//      spy(new ApplicationLocalizer(lfs, user, appId, logDir,
//            localPaths));
//    // ignore cache localization
//    doNothing().when(spyLocalizer).localizeFiles(
//        Matchers.<Localization>anyObject(), Matchers.<Path>anyObject());
//    Path workingDir = lfs.getWorkingDirectory();
//    dce.initApplication(spyLocalizer, nmLocal, mockLocalization, localPaths);
//    lfs.setWorkingDirectory(workingDir);
//
//    for (Path localdir : localPaths) {
//      Path userdir = lfs.makeQualified(new Path(localdir,
//            new Path(ApplicationLocalizer.USERCACHE, user)));
//      // $localdir/$user
//      verify(spylfs).mkdir(userdir,
//          new FsPermission(ApplicationLocalizer.USER_PERM), true);
//      // $localdir/$user/appcache
//      Path jobdir = new Path(userdir, ApplicationLocalizer.appcache);
//      verify(spylfs).mkdir(jobdir,
//          new FsPermission(ApplicationLocalizer.appcache_PERM), true);
//      // $localdir/$user/filecache
//      Path filedir = new Path(userdir, ApplicationLocalizer.FILECACHE);
//      verify(spylfs).mkdir(filedir,
//          new FsPermission(ApplicationLocalizer.FILECACHE_PERM), true);
//      // $localdir/$user/appcache/$appId
//      Path appdir = new Path(jobdir, appId);
//      verify(spylfs).mkdir(appdir,
//          new FsPermission(ApplicationLocalizer.APPDIR_PERM), true);
//      // $localdir/$user/appcache/$appId/work
//      Path workdir = new Path(appdir, ApplicationLocalizer.WORKDIR);
//      verify(spylfs, atMost(1)).mkdir(workdir, FsPermission.getDefault(), true);
//    }
//    // $logdir/$appId
//    Path logdir = new Path(lfs.makeQualified(logDir), appId);
//    verify(spylfs).mkdir(logdir,
//        new FsPermission(ApplicationLocalizer.LOGDIR_PERM), true);
//  }

  @Before
  public void setUp() throws IOException, YarnException {
    yarnConfiguration = new YarnConfiguration();
    setNumaConfig();
    Context mockContext = createAndGetMockContext();
    NMStateStoreService nmStateStoreService =
            mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(nmStateStoreService);
    numaResourceAllocator = new NumaResourceAllocator(mockContext) {
      @Override
      public String executeNGetCmdOutput(Configuration config)
              throws YarnRuntimeException {
        return getNumaCmdOutput();
      }
    };

    numaResourceAllocator.init(yarnConfiguration);
    FileContext lfs = FileContext.getLocalFSFileContext();
    containerExecutor = new DefaultContainerExecutor(lfs) {
      @Override
      public Configuration getConf() {
        return yarnConfiguration;
      }
    };
    containerExecutor.setNumaResourceAllocator(numaResourceAllocator);
    mockContainer = mock(Container.class);
  }

  private void setNumaConfig() {
    yarnConfiguration.set(YarnConfiguration.NM_NUMA_AWARENESS_ENABLED, "true");
    yarnConfiguration.set(YarnConfiguration.NM_NUMA_AWARENESS_READ_TOPOLOGY, "true");
    yarnConfiguration.set(YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD, "/usr/bin/numactl");
  }


  private String getNumaCmdOutput() {
    // architecture of 8 cpu cores
    // randomly picked size of memory
    return "available: 2 nodes (0-1)\n\t"
            + "node 0 cpus: 0 2 4 6\n\t"
            + "node 0 size: 73717 MB\n\t"
            + "node 0 free: 73717 MB\n\t"
            + "node 1 cpus: 1 3 5 7\n\t"
            + "node 1 size: 73717 MB\n\t"
            + "node 1 free: 73717 MB\n\t"
            + "node distances:\n\t"
            + "node 0 1\n\t"
            + "0: 10 20\n\t"
            + "1: 20 10";
  }

  private Context createAndGetMockContext() {
    Context mockContext = mock(Context.class);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
            ConcurrentHashMap.class);
    mockContainer = mock(Container.class);
    when(mockContainer.getResourceMappings())
            .thenReturn(new ResourceMappings());
    when(mockContainers.get(any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(2048, 2));
    return mockContext;
  }

  private void testAllocateNumaResource(String containerId, Resource resource,
                                        String memNodes, String cpuNodes) throws Exception {
    when(mockContainer.getContainerId())
            .thenReturn(ContainerId.fromString(containerId));
    when(mockContainer.getResource()).thenReturn(resource);
    NumaResourceAllocation numaResourceAllocation =
            numaResourceAllocator.allocateNumaNodes(mockContainer);
    containerExecutor.setNumactl(containerExecutor.getConf().get(YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
            YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD));
    String[] commands = containerExecutor.getNumaCommands(numaResourceAllocation);
    assertEquals(Arrays.asList(commands), Arrays.asList("/usr/bin/numactl",
            "--interleave=" + memNodes, "--cpunodebind=" + cpuNodes));
  }

  @Test
  public void testAllocateNumaMemoryResource() throws Exception {
    // keeping cores constant for testing memory resources

    // allocates node 0 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000001",
            Resource.newInstance(2048, 2), "0", "0");

    // allocates node 1 for memory and cpu since allocator uses round robin assignment
    testAllocateNumaResource("container_1481156246874_0001_01_000002",
            Resource.newInstance(60000, 2), "1", "1");

    // allocates node 0,1 for memory since there is no sufficient memory in any one node
    testAllocateNumaResource("container_1481156246874_0001_01_000003",
            Resource.newInstance(80000, 2), "0,1", "0");

    // returns null since there are no sufficient resources available for the request
    when(mockContainer.getContainerId()).thenReturn(
            ContainerId.fromString("container_1481156246874_0001_01_000004"));
    when(mockContainer.getResource())
            .thenReturn(Resource.newInstance(80000, 2));
    Assert.assertNull(numaResourceAllocator.allocateNumaNodes(mockContainer));

    // allocates node 1 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000005",
            Resource.newInstance(1024, 2), "1", "1");
  }

  @Test
  public void testAllocateNumaCpusResource() throws Exception {
    // keeping memory constant

    // allocates node 0 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000001",
            Resource.newInstance(2048, 2), "0", "0");

    // allocates node 1 for memory and cpu since allocator uses round robin assignment
    testAllocateNumaResource("container_1481156246874_0001_01_000002",
            Resource.newInstance(2048, 2), "1", "1");

    // allocates node 0,1 for cpus since there is are no sufficient cpus available in any one node
    testAllocateNumaResource("container_1481156246874_0001_01_000003",
            Resource.newInstance(2048, 3), "0", "0,1");

    // returns null since there are no sufficient resources available for the request
    when(mockContainer.getContainerId()).thenReturn(
            ContainerId.fromString("container_1481156246874_0001_01_000004"));
    when(mockContainer.getResource()).thenReturn(Resource.newInstance(2048, 2));
    Assert.assertNull(numaResourceAllocator.allocateNumaNodes(mockContainer));

    // allocates node 1 for memory and cpu
    testAllocateNumaResource("container_1481156246874_0001_01_000005",
            Resource.newInstance(2048, 1), "1", "1");
  }

  @Test
  public void testReacquireContainer() throws Exception {
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<ContainerId, Container> mockContainers = mock(
            ConcurrentHashMap.class);
    Context mockContext = mock(Context.class);
    NMStateStoreService mock = mock(NMStateStoreService.class);
    when(mockContext.getNMStateStore()).thenReturn(mock);
    ResourceMappings resourceMappings = new ResourceMappings();
    AssignedResources assignedRscs = new AssignedResources();
    when(mockContainer.getResource())
            .thenReturn(Resource.newInstance(147434, 2));
    ContainerId cid = ContainerId.fromString("container_1481156246874_0001_01_000001");
    when(mockContainer.getContainerId()).thenReturn(cid);
    NumaResourceAllocation numaResourceAllocation =
            numaResourceAllocator.allocateNumaNodes(mockContainer);
    assignedRscs.updateAssignedResources(Arrays.asList(numaResourceAllocation));
    resourceMappings.addAssignedResources("numa", assignedRscs);
    when(mockContainer.getResourceMappings()).thenReturn(resourceMappings);
    when(mockContainers.get(any())).thenReturn(mockContainer);
    when(mockContext.getContainers()).thenReturn(mockContainers);

    // recovered numa resources should be added to the used resources and
    // remaining will be available for further allocation.

    ContainerReacquisitionContext containerReacquisitionContext =
            new ContainerReacquisitionContext.Builder()
                    .setContainerId(cid)
                    .setUser("user")
                    .setContainer(mockContainer)
                    .build();

    containerExecutor.reacquireContainer(containerReacquisitionContext);

    // reacquireContainer recovers all the numa resources ,
    // that should be free to use next
    testAllocateNumaResource("container_1481156246874_0001_01_000001",
            Resource.newInstance(147434, 2), "0,1", "1");
    when(mockContainer.getContainerId()).thenReturn(
            ContainerId.fromString("container_1481156246874_0001_01_000004"));
    when(mockContainer.getResource())
            .thenReturn(Resource.newInstance(1024, 2));

    // returns null since there are no sufficient resources available for the request
    Assert.assertNull(numaResourceAllocator.allocateNumaNodes(mockContainer));
  }

  @Test
  public void testConcatStringCommands() {
    // test one array of string as null
    assertEquals(containerExecutor.concatStringCommands(null, new String[]{"hello"})[0],
            new String[]{"hello"}[0]);
    // test both array of string as null
    Assert.assertNull(containerExecutor.concatStringCommands(null, null));
    // test case when both arrays are not null and of equal length
    String[] res = containerExecutor.concatStringCommands(new String[]{"one"},
            new String[]{"two"});
    assertEquals(res[0]+res[1], "one" + "two");
    // test both array of different length
    res = containerExecutor.concatStringCommands(new String[]{"one"},
            new String[]{"two", "three"});
    assertEquals(res[0] + res[1] + res[2], "one" + "two" + "three");

  }


}
