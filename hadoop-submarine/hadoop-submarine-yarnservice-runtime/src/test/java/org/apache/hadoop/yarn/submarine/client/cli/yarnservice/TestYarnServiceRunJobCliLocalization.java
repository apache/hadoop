/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.yarnservice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Class to test YarnService localization feature with the Run job CLI action.
 */
public class TestYarnServiceRunJobCliLocalization {
  private static final String ZIP_EXTENSION = ".zip";
  private TestYarnServiceRunJobCliCommons testCommons =
      new TestYarnServiceRunJobCliCommons();
  private MockClientContext mockClientContext;
  private RemoteDirectoryManager spyRdm;

  @Before
  public void before() throws IOException, YarnException {
    testCommons.setup();
    mockClientContext = YarnServiceCliTestUtils.getMockClientContext();
    spyRdm = setupSpyRemoteDirManager();
  }

  @After
  public void cleanup() throws IOException {
    testCommons.teardown();
  }

  private ParamBuilderForTest createCommonParamsBuilder() {
    return ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(DEFAULT_JOB_NAME)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(3)
        .withWorkerDockerImage(DEFAULT_WORKER_DOCKER_IMAGE)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withNumberOfPs(2)
        .withPsDockerImage(DEFAULT_PS_DOCKER_IMAGE)
        .withPsLaunchCommand(DEFAULT_PS_LAUNCH_CMD)
        .withPsResources(DEFAULT_PS_RESOURCES)
        .withVerbose();
  }

  private void assertFilesAreDeleted(File... files) {
    for (File file : files) {
      assertFalse("File should be deleted: " + file.getAbsolutePath(),
          file.exists());
    }
  }

  private RemoteDirectoryManager setupSpyRemoteDirManager() {
    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);
    return spyRdm;
  }

  private Path getStagingDir() throws IOException {
    return mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea(DEFAULT_JOB_NAME, true);
  }

  private RunJobCli createRunJobCliWithoutVerboseAssertion() {
    return new RunJobCli(mockClientContext);
  }

  private RunJobCli createRunJobCli() {
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());
    return runJobCli;
  }

  private String getFilePath(String localUrl, Path stagingDir) {
    return stagingDir.toUri().getPath()
        + "/" + new Path(localUrl).getName();
  }

  private String getFilePathWithSuffix(Path stagingDir, String localUrl,
      String suffix) {
    return stagingDir.toUri().getPath() + "/" + new Path(localUrl).getName()
        + suffix;
  }

  private void assertConfigFile(ConfigFile expected, ConfigFile actual) {
    assertEquals("ConfigFile does not equal to expected!", expected, actual);
  }

  private void assertNumberOfLocalizations(List<ConfigFile> files,
      int expected) {
    assertEquals("Number of localizations is not the expected!", expected,
        files.size());
  }

  private void verifyRdmCopyToRemoteLocalCalls(int expectedCalls)
      throws IOException {
    verify(spyRdm, times(expectedCalls)).copyRemoteToLocal(anyString(),
        anyString());
  }

  /**
   * Basic test.
   * In one hand, create local temp file/dir for hdfs URI in
   * local staging dir.
   * In the other hand, use MockRemoteDirectoryManager mock
   * implementation when check FileStatus or exists of HDFS file/dir
   * --localization hdfs:///user/yarn/script1.py:.
   * --localization /temp/script2.py:./
   * --localization /temp/script2.py:/opt/script.py
   */
  @Test
  public void testRunJobWithBasicLocalization() throws Exception {
    String remoteUrl = "hdfs:///user/yarn/script1.py";
    String containerLocal1 = ".";
    String localUrl = "/temp/script2.py";
    String containerLocal2 = "./";
    String containerLocal3 = "/opt/script.py";
    // Create local file, we need to put it under local temp dir
    File localFile1 = testCommons.getFileUtils().createFileInTempDir(localUrl);

    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = getStagingDir();
    testCommons.getFileUtils().createFileInDir(stagingDir, remoteUrl);

    String[] params = createCommonParamsBuilder()
        .withLocalization(remoteUrl, containerLocal1)
        .withLocalization(localFile1.getAbsolutePath(), containerLocal2)
        .withLocalization(localFile1.getAbsolutePath(), containerLocal3)
        .build();
    RunJobCli runJobCli = createRunJobCli();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    assertNumberOfServiceComponents(serviceSpec, 3);

    // No remote dir and HDFS file exists.
    // Ensure download never happened.
    verifyRdmCopyToRemoteLocalCalls(0);
    // Ensure local original files are not deleted
    assertTrue(localFile1.exists());

    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    assertNumberOfLocalizations(files, 3);

    ConfigFile expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.STATIC);
    expectedConfigFile.setSrcFile(remoteUrl);
    expectedConfigFile.setDestFile(new Path(remoteUrl).getName());
    assertConfigFile(expectedConfigFile, files.get(0));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.STATIC);
    expectedConfigFile.setSrcFile(getFilePath(localUrl, stagingDir));
    expectedConfigFile.setDestFile(new Path(localUrl).getName());
    assertConfigFile(expectedConfigFile, files.get(1));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.STATIC);
    expectedConfigFile.setSrcFile(getFilePath(localUrl, stagingDir));
    expectedConfigFile.setDestFile(new Path(containerLocal3).getName());
    assertConfigFile(expectedConfigFile, files.get(2));

    // Ensure env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(containerLocal3).getName()
        + ":" + containerLocal3 + ":rw";
    assertTrue(env.contains(expectedMounts));
  }

  private void assertNumberOfServiceComponents(Service serviceSpec,
      int expected) {
    assertEquals(expected, serviceSpec.getComponents().size());
  }

  /**
   * Non HDFS remote URI test.
   * --localization https://a/b/1.patch:.
   * --localization s3a://a/dir:/opt/mys3dir
   */
  @Test
  public void testRunJobWithNonHDFSRemoteLocalization() throws Exception {
    String remoteUri1 = "https://a/b/1.patch";
    String containerLocal1 = ".";
    String remoteUri2 = "s3a://a/s3dir";
    String containerLocal2 = "/opt/mys3dir";

    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = getStagingDir();
    testCommons.getFileUtils().createFileInDir(stagingDir, remoteUri1);
    File remoteDir1 =
        testCommons.getFileUtils().createDirectory(stagingDir, remoteUri2);
    testCommons.getFileUtils().createFileInDir(remoteDir1, "afile");

    String suffix1 = "_" + remoteDir1.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUri2);

    String[] params = createCommonParamsBuilder()
        .withLocalization(remoteUri1, containerLocal1)
        .withLocalization(remoteUri2, containerLocal2)
        .build();
    RunJobCli runJobCli = createRunJobCli();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    assertNumberOfServiceComponents(serviceSpec, 3);

    // Ensure download remote dir 2 times
    verifyRdmCopyToRemoteLocalCalls(2);

    // Ensure downloaded temp files are deleted
    assertFilesAreDeleted(
        testCommons.getFileUtils().getTempFileWithName(remoteUri1),
        testCommons.getFileUtils().getTempFileWithName(remoteUri2));

    // Ensure zip file are deleted
    assertFilesAreDeleted(
        testCommons.getFileUtils()
            .getTempFileWithName(remoteUri2 + "_" + suffix1 + ZIP_EXTENSION));

    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    assertNumberOfLocalizations(files, 2);

    ConfigFile expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.STATIC);
    expectedConfigFile.setSrcFile(getFilePath(remoteUri1, stagingDir));
    expectedConfigFile.setDestFile(new Path(remoteUri1).getName());
    assertConfigFile(expectedConfigFile, files.get(0));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, remoteUri2, suffix1 + ZIP_EXTENSION));
    expectedConfigFile.setDestFile(new Path(containerLocal2).getName());
    assertConfigFile(expectedConfigFile, files.get(1));

    // Ensure env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(remoteUri2).getName()
        + ":" + containerLocal2 + ":rw";
    assertTrue(env.contains(expectedMounts));
  }

  /**
   * Test HDFS dir localization.
   * --localization hdfs:///user/yarn/mydir:./mydir1
   * --localization hdfs:///user/yarn/mydir2:/opt/dir2:rw
   * --localization hdfs:///user/yarn/mydir:.
   * --localization hdfs:///user/yarn/mydir2:./
   */
  @Test
  public void testRunJobWithHdfsDirLocalization() throws Exception {
    String remoteUrl = "hdfs:///user/yarn/mydir";
    String containerPath = "./mydir1";
    String remoteUrl2 = "hdfs:///user/yarn/mydir2";
    String containerPath2 = "/opt/dir2";
    String containerPath3 = ".";
    String containerPath4 = "./";

    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = getStagingDir();
    File remoteDir1 =
        testCommons.getFileUtils().createDirectory(stagingDir, remoteUrl);
    testCommons.getFileUtils().createFileInDir(remoteDir1, "1.py");
    testCommons.getFileUtils().createFileInDir(remoteDir1, "2.py");

    File remoteDir2 =
        testCommons.getFileUtils().createDirectory(stagingDir, remoteUrl2);
    testCommons.getFileUtils().createFileInDir(remoteDir2, "3.py");
    testCommons.getFileUtils().createFileInDir(remoteDir2, "4.py");

    String suffix1 = "_" + remoteDir1.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUrl);
    String suffix2 = "_" + remoteDir2.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUrl2);

    String[] params = createCommonParamsBuilder()
        .withLocalization(remoteUrl, containerPath)
        .withLocalization(remoteUrl2, containerPath2)
        .withLocalization(remoteUrl, containerPath3)
        .withLocalization(remoteUrl2, containerPath4)
        .build();
    RunJobCli runJobCli = createRunJobCli();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    assertNumberOfServiceComponents(serviceSpec, 3);

    // Ensure download remote dir 4 times
    verifyRdmCopyToRemoteLocalCalls(4);

    // Ensure downloaded temp files are deleted
    assertFilesAreDeleted(
        testCommons.getFileUtils().getTempFileWithName(remoteUrl),
        testCommons.getFileUtils().getTempFileWithName(remoteUrl2));

    // Ensure zip file are deleted
    assertFilesAreDeleted(
        testCommons.getFileUtils()
            .getTempFileWithName(remoteUrl + suffix1 + ZIP_EXTENSION),
        testCommons.getFileUtils()
            .getTempFileWithName(remoteUrl2 + suffix2 + ZIP_EXTENSION));

    // Ensure files will be localized
    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    assertNumberOfLocalizations(files, 4);

    ConfigFile expectedConfigFile = new ConfigFile();
    // The hdfs dir should be download and compress and let YARN to uncompress
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, remoteUrl, suffix1 + ZIP_EXTENSION));
    // Relative path in container, but not "." or "./". Use its own name
    expectedConfigFile.setDestFile(new Path(containerPath).getName());
    assertConfigFile(expectedConfigFile, files.get(0));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, remoteUrl2, suffix2 + ZIP_EXTENSION));
    expectedConfigFile.setDestFile(new Path(containerPath2).getName());
    assertConfigFile(expectedConfigFile, files.get(1));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, remoteUrl, suffix1 + ZIP_EXTENSION));
    // Relative path in container ".", use remote path name
    expectedConfigFile.setDestFile(new Path(remoteUrl).getName());
    assertConfigFile(expectedConfigFile, files.get(2));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, remoteUrl2, suffix2 + ZIP_EXTENSION));
    // Relative path in container ".", use remote path name
    expectedConfigFile.setDestFile(new Path(remoteUrl2).getName());
    assertConfigFile(expectedConfigFile, files.get(3));

    // Ensure mounts env value is correct. Add one mount string
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");

    String expectedMounts =
        new Path(containerPath2).getName() + ":" + containerPath2 + ":rw";
    assertTrue(env.contains(expectedMounts));
  }

  /**
   * Test if file/dir to be localized whose size exceeds limit.
   * Max 10MB in configuration, mock remote will
   * always return file size 100MB.
   * This configuration will fail the job which has remoteUri
   * But don't impact local dir/file
   *
   * --localization https://a/b/1.patch:.
   * --localization s3a://a/dir:/opt/mys3dir
   * --localization /temp/script2.py:./
   */
  @Test
  public void testRunJobRemoteUriExceedLocalizationSize() throws Exception {
    String remoteUri1 = "https://a/b/1.patch";
    String containerLocal1 = ".";
    String remoteUri2 = "s3a://a/s3dir";
    String containerLocal2 = "/opt/mys3dir";
    String localUri1 = "/temp/script2";
    String containerLocal3 = "./";

    SubmarineConfiguration submarineConf = new SubmarineConfiguration();

    // Max 10MB, mock remote will always return file size 100MB.
    submarineConf.set(
        SubmarineConfiguration.LOCALIZATION_MAX_ALLOWED_FILE_SIZE_MB,
        "10");
    mockClientContext.setSubmarineConfig(submarineConf);

    assertFalse(SubmarineLogs.isVerbose());

    // create remote file in local staging dir to simulate
    Path stagingDir = getStagingDir();
    testCommons.getFileUtils().createFileInDir(stagingDir, remoteUri1);
    File remoteDir1 =
        testCommons.getFileUtils().createDirectory(stagingDir, remoteUri2);
    testCommons.getFileUtils().createFileInDir(remoteDir1, "afile");

    // create local file, we need to put it under local temp dir
    File localFile1 = testCommons.getFileUtils().createFileInTempDir(localUri1);

    try {
      RunJobCli runJobCli = createRunJobCli();
      String[] params = createCommonParamsBuilder()
          .withLocalization(remoteUri1, containerLocal1)
          .build();
      runJobCli.run(params);
    } catch (IOException e) {
      // Shouldn't have exception because it's within file size limit
      fail();
    }
    // we should download because fail fast
    verifyRdmCopyToRemoteLocalCalls(1);
    try {
      String[] params = createCommonParamsBuilder()
          .withLocalization(remoteUri1, containerLocal1)
          .withLocalization(remoteUri2, containerLocal2)
          .withLocalization(localFile1.getAbsolutePath(), containerLocal3)
          .build();

      reset(spyRdm);
      RunJobCli runJobCli = createRunJobCliWithoutVerboseAssertion();
      runJobCli.run(params);
    } catch (IOException e) {
      assertTrue(e.getMessage()
          .contains("104857600 exceeds configured max size:10485760"));
      // we shouldn't do any download because fail fast
      verifyRdmCopyToRemoteLocalCalls(0);
    }

    try {
      String[] params = createCommonParamsBuilder()
          .withLocalization(localFile1.getAbsolutePath(), containerLocal3)
          .build();
      RunJobCli runJobCli = createRunJobCliWithoutVerboseAssertion();
      runJobCli.run(params);
    } catch (IOException e) {
      assertTrue(e.getMessage()
          .contains("104857600 exceeds configured max size:10485760"));
      // we shouldn't do any download because fail fast
      verifyRdmCopyToRemoteLocalCalls(0);
    }
  }

  /**
   * Test remote Uri doesn't exist.
   * */
  @Test
  public void testRunJobWithNonExistRemoteUri() throws Exception {
    String remoteUri1 = "hdfs:///a/b/1.patch";
    String containerLocal1 = ".";
    String localUri1 = "/a/b/c";
    String containerLocal2 = "./";

    try {
      String[] params = createCommonParamsBuilder()
          .withLocalization(remoteUri1, containerLocal1)
          .build();
      RunJobCli runJobCli = createRunJobCli();
      runJobCli.run(params);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("doesn't exists"));
    }

    try {
      String[] params = createCommonParamsBuilder()
          .withLocalization(localUri1, containerLocal2)
          .build();
      RunJobCli runJobCli = createRunJobCliWithoutVerboseAssertion();
      runJobCli.run(params);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("doesn't exists"));
    }
  }

  /**
   * Test local dir
   * --localization /user/yarn/mydir:./mydir1
   * --localization /user/yarn/mydir2:/opt/dir2:rw
   * --localization /user/yarn/mydir2:.
   */
  @Test
  public void testRunJobWithLocalDirLocalization() throws Exception {
    String localUrl = "/user/yarn/mydir";
    String containerPath = "./mydir1";
    String localUrl2 = "/user/yarn/mydir2";
    String containerPath2 = "/opt/dir2";
    String containerPath3 = ".";

    // create local file
    File localDir1 = testCommons.getFileUtils().createDirInTempDir(localUrl);
    testCommons.getFileUtils().createFileInDir(localDir1, "1.py");
    testCommons.getFileUtils().createFileInDir(localDir1, "2.py");

    File localDir2 = testCommons.getFileUtils().createDirInTempDir(localUrl2);
    testCommons.getFileUtils().createFileInDir(localDir2, "3.py");
    testCommons.getFileUtils().createFileInDir(localDir2, "4.py");

    String suffix1 = "_" + localDir1.lastModified()
        + "-" + localDir1.length();
    String suffix2 = "_" + localDir2.lastModified()
        + "-" + localDir2.length();

    String[] params = createCommonParamsBuilder()
        .withLocalization(localDir1.getAbsolutePath(), containerPath)
        .withLocalization(localDir2.getAbsolutePath(), containerPath2)
        .withLocalization(localDir2.getAbsolutePath(), containerPath3)
        .build();
    RunJobCli runJobCli = createRunJobCli();
    runJobCli.run(params);

    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    assertNumberOfServiceComponents(serviceSpec, 3);

    // we shouldn't do any download
    verifyRdmCopyToRemoteLocalCalls(0);

    // Ensure local original files are not deleted
    assertTrue(localDir1.exists());
    assertTrue(localDir2.exists());

    // Ensure zip file are deleted
    assertFalse(
        testCommons.getFileUtils()
            .getTempFileWithName(localUrl + suffix1 + ZIP_EXTENSION)
            .exists());
    assertFalse(
        testCommons.getFileUtils()
            .getTempFileWithName(localUrl2 + suffix2 + ZIP_EXTENSION)
            .exists());

    // Ensure dirs will be zipped and localized
    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    assertNumberOfLocalizations(files, 3);

    Path stagingDir = getStagingDir();
    ConfigFile expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, localUrl, suffix1 + ZIP_EXTENSION));
    expectedConfigFile.setDestFile(new Path(containerPath).getName());
    assertConfigFile(expectedConfigFile, files.get(0));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, localUrl2, suffix2 + ZIP_EXTENSION));
    expectedConfigFile.setDestFile(new Path(containerPath2).getName());
    assertConfigFile(expectedConfigFile, files.get(1));

    expectedConfigFile = new ConfigFile();
    expectedConfigFile.setType(ConfigFile.TypeEnum.ARCHIVE);
    expectedConfigFile.setSrcFile(
        getFilePathWithSuffix(stagingDir, localUrl2, suffix2 + ZIP_EXTENSION));
    expectedConfigFile.setDestFile(new Path(localUrl2).getName());
    assertConfigFile(expectedConfigFile, files.get(2));

    // Ensure mounts env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(containerPath2).getName()
        + ":" + containerPath2 + ":rw";

    assertTrue(env.contains(expectedMounts));
  }
}
