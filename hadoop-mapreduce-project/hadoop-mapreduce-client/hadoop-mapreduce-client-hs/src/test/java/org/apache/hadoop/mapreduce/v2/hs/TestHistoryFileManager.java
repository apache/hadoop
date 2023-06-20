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

package org.apache.hadoop.mapreduce.v2.hs;


import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.util.UUID;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.mockito.Mockito.*;

public class TestHistoryFileManager {
  private static MiniDFSCluster dfsCluster = null;
  private static MiniDFSCluster dfsCluster2 = null;
  private static String coreSitePath;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
    coreSitePath = "." + File.separator + "target" + File.separator +
            "test-classes" + File.separator + "core-site.xml";
    Configuration conf = new HdfsConfiguration();
    Configuration conf2 = new HdfsConfiguration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    conf2.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, conf.get(
        MiniDFSCluster.HDFS_MINIDFS_BASEDIR, MiniDFSCluster.getBaseDirectory())
        + "_2");
    dfsCluster2 = new MiniDFSCluster.Builder(conf2).build();
  }

  @AfterClass
  public static void cleanUpClass() throws Exception {
    dfsCluster.shutdown();
    dfsCluster2.shutdown();
  }

  @After
  public void cleanTest() throws Exception {
    new File(coreSitePath).delete();
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.LEAVE);
    dfsCluster2.getFileSystem().setSafeMode(
        SafeModeAction.LEAVE);
  }

  private String getDoneDirNameForTest() {
    return "/" + name.getMethodName();
  }

  private String getIntermediateDoneDirNameForTest() {
    return "/intermediate_" + name.getMethodName();
  }

  private void testTryCreateHistoryDirs(Configuration conf, boolean expected)
      throws Exception {
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, getDoneDirNameForTest());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR, getIntermediateDoneDirNameForTest());
    HistoryFileManager hfm = new HistoryFileManager();
    hfm.conf = conf;
    Assert.assertEquals(expected, hfm.tryCreatingHistoryDirs(false));
  }

  @Test
  public void testCreateDirsWithoutFileSystem() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:1");
    testTryCreateHistoryDirs(conf, false);
  }

  @Test
  public void testCreateDirsWithFileSystem() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.LEAVE);
    Assert.assertFalse(dfsCluster.getFileSystem().isInSafeMode());
    testTryCreateHistoryDirs(dfsCluster.getConfiguration(0), true);
  }

  @Test
  public void testCreateDirsWithAdditionalFileSystem() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.LEAVE);
    dfsCluster2.getFileSystem().setSafeMode(
        SafeModeAction.LEAVE);
    Assert.assertFalse(dfsCluster.getFileSystem().isInSafeMode());
    Assert.assertFalse(dfsCluster2.getFileSystem().isInSafeMode());

    // Set default configuration to the first cluster
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
            dfsCluster.getURI().toString());
    FileOutputStream os = new FileOutputStream(coreSitePath);
    conf.writeXml(os);
    os.close();

    testTryCreateHistoryDirs(dfsCluster2.getConfiguration(0), true);

    // Directories should be created only in the default file system (dfsCluster)
    Assert.assertTrue(dfsCluster.getFileSystem()
            .exists(new Path(getDoneDirNameForTest())));
    Assert.assertTrue(dfsCluster.getFileSystem()
            .exists(new Path(getIntermediateDoneDirNameForTest())));
    Assert.assertFalse(dfsCluster2.getFileSystem()
            .exists(new Path(getDoneDirNameForTest())));
    Assert.assertFalse(dfsCluster2.getFileSystem()
            .exists(new Path(getIntermediateDoneDirNameForTest())));
  }

  @Test
  public void testCreateDirsWithFileSystemInSafeMode() throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    testTryCreateHistoryDirs(dfsCluster.getConfiguration(0), false);
  }

  private void testCreateHistoryDirs(Configuration conf, Clock clock)
      throws Exception {
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, "/" + UUID.randomUUID());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR, "/" + UUID.randomUUID());
    HistoryFileManager hfm = new HistoryFileManager();
    hfm.conf = conf;
    hfm.createHistoryDirs(clock, 500, 2000);
  }

  @Test
  public void testCreateDirsWithFileSystemBecomingAvailBeforeTimeout()
      throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          dfsCluster.getFileSystem().setSafeMode(
              SafeModeAction.LEAVE);
          Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
        } catch (Exception ex) {
          Assert.fail(ex.toString());
        }
      }
    }.start();
    testCreateHistoryDirs(dfsCluster.getConfiguration(0),
        SystemClock.getInstance());
  }

  @Test(expected = YarnRuntimeException.class)
  public void testCreateDirsWithFileSystemNotBecomingAvailBeforeTimeout()
      throws Exception {
    dfsCluster.getFileSystem().setSafeMode(
        SafeModeAction.ENTER);
    Assert.assertTrue(dfsCluster.getFileSystem().isInSafeMode());
    final ControlledClock clock = new ControlledClock();
    clock.setTime(1);
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          clock.setTime(3000);
        } catch (Exception ex) {
          Assert.fail(ex.toString());
        }
      }
    }.start();
    testCreateHistoryDirs(dfsCluster.getConfiguration(0), clock);
  }

  @Test
  public void testScanDirectory() throws Exception {

    Path p = new Path("any");
    FileContext fc = mock(FileContext.class);
    when(fc.makeQualified(p)).thenReturn(p);
    when(fc.listStatus(p)).thenThrow(new FileNotFoundException());

    List<FileStatus> lfs = HistoryFileManager.scanDirectory(p, fc, null);

    //primarily, succcess is that an exception was not thrown.  Also nice to
    //check this
    Assert.assertNotNull(lfs);

  }

  @Test
  public void testHistoryFileInfoSummaryFileNotExist() throws Exception {
    HistoryFileManagerTest hmTest = new HistoryFileManagerTest();
    String job = "job_1410889000000_123456";
    Path summaryFile = new Path(job + ".summary");
    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(job)));
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR,
        "/" + UUID.randomUUID());
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR,
        "/" + UUID.randomUUID());
    hmTest.serviceInit(conf);
    HistoryFileInfo info = hmTest.getHistoryFileInfo(null, null,
        summaryFile, jobIndexInfo, false);
    info.moveToDone();
    Assert.assertFalse(info.didMoveFail());
  }

  @Test
  public void testHistoryFileInfoLoadOversizedJobShouldReturnUnParsedJob()
      throws Exception {
    HistoryFileManagerTest hmTest = new HistoryFileManagerTest();

    int allowedMaximumTasks = 5;
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.setInt(JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX, allowedMaximumTasks);

    hmTest.init(conf);

    // set up a job of which the number of tasks is greater than maximum allowed
    String jobId = "job_1410889000000_123456";
    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(jobId)));
    jobIndexInfo.setNumMaps(allowedMaximumTasks);
    jobIndexInfo.setNumReduces(allowedMaximumTasks);


    HistoryFileInfo info = hmTest.getHistoryFileInfo(null, null, null,
        jobIndexInfo, false);

    Job job = info.loadJob();
    Assert.assertTrue("Should return an instance of UnparsedJob to indicate" +
        " the job history file is not parsed", job instanceof UnparsedJob);
  }

  @Test
  public void testHistoryFileInfoLoadNormalSizedJobShouldReturnCompletedJob()
      throws Exception {
    HistoryFileManagerTest hmTest = new HistoryFileManagerTest();

    final int numOfTasks = 100;
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.setInt(JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX,
        numOfTasks + numOfTasks + 1);

    hmTest.init(conf);

    // set up a job of which the number of tasks is smaller than the maximum
    // allowed, and therefore will be fully loaded.
    final String jobId = "job_1416424547277_0002";
    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(jobId)));
    jobIndexInfo.setNumMaps(numOfTasks);
    jobIndexInfo.setNumReduces(numOfTasks);


    final String historyFile = getClass().getClassLoader().getResource(
        "job_2.0.3-alpha-FAILED.jhist").getFile();
    final Path historyFilePath = FileSystem.getLocal(conf).makeQualified(
        new Path(historyFile));
    HistoryFileInfo info = hmTest.getHistoryFileInfo(historyFilePath, null,
        null, jobIndexInfo, false);

    Job job = info.loadJob();
    Assert.assertTrue("Should return an instance of CompletedJob as " +
        "a result of parsing the job history file of the job",
        job instanceof CompletedJob);
  }

  @Test
  public void testHistoryFileInfoShouldReturnCompletedJobIfMaxNotConfiged()
      throws Exception {
    HistoryFileManagerTest hmTest = new HistoryFileManagerTest();

    Configuration conf = dfsCluster.getConfiguration(0);
    conf.setInt(JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX, -1);

    hmTest.init(conf);

    final String jobId = "job_1416424547277_0002";
    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(jobId)));
    jobIndexInfo.setNumMaps(100);
    jobIndexInfo.setNumReduces(100);

    final String historyFile = getClass().getClassLoader().getResource(
        "job_2.0.3-alpha-FAILED.jhist").getFile();
    final Path historyFilePath = FileSystem.getLocal(conf).makeQualified(
        new Path(historyFile));
    HistoryFileInfo info = hmTest.getHistoryFileInfo(historyFilePath, null,
        null, jobIndexInfo, false);

    Job job = info.loadJob();
    Assert.assertTrue("Should return an instance of CompletedJob as " +
            "a result of parsing the job history file of the job",
        job instanceof CompletedJob);

  }

  /**
   * This test sets up a scenario where the history files have already been
   * moved to the "done" directory (so the "intermediate" directory is empty),
   * but then moveToDone() is called again on the same history file. It
   * validates that the second moveToDone() still succeeds rather than throws a
   * FileNotFoundException.
   */
  @Test
  public void testMoveToDoneAlreadyMovedSucceeds() throws Exception {
    HistoryFileManagerTest historyFileManager = new HistoryFileManagerTest();
    long jobTimestamp = 1535436603000L;
    String job = "job_" + jobTimestamp + "_123456789";

    String intermediateDirectory = "/" + UUID.randomUUID();
    String doneDirectory = "/" + UUID.randomUUID();
    Configuration conf = dfsCluster.getConfiguration(0);
    conf.set(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR,
        intermediateDirectory);
    conf.set(JHAdminConfig.MR_HISTORY_DONE_DIR, doneDirectory);

    Path intermediateHistoryFilePath = new Path(intermediateDirectory + "/"
        + job + ".jhist");
    Path intermediateConfFilePath = new Path(intermediateDirectory + "/"
        + job + "_conf.xml");
    Path doneHistoryFilePath = new Path(doneDirectory + "/"
        + JobHistoryUtils.timestampDirectoryComponent(jobTimestamp) + "/123456/"
        + job + ".jhist");
    Path doneConfFilePath = new Path(doneDirectory + "/"
        + JobHistoryUtils.timestampDirectoryComponent(jobTimestamp)
        + "/123456/" + job + "_conf.xml");

    dfsCluster.getFileSystem().createNewFile(doneHistoryFilePath);
    dfsCluster.getFileSystem().createNewFile(doneConfFilePath);

    historyFileManager.serviceInit(conf);

    JobIndexInfo jobIndexInfo = new JobIndexInfo();
    jobIndexInfo.setJobId(TypeConverter.toYarn(JobID.forName(job)));
    jobIndexInfo.setFinishTime(jobTimestamp);
    HistoryFileInfo info = historyFileManager.getHistoryFileInfo(
        intermediateHistoryFilePath, intermediateConfFilePath, null,
        jobIndexInfo, false);
    info.moveToDone();

    Assert.assertFalse(info.isMovePending());
    Assert.assertEquals(doneHistoryFilePath.toString(),
        info.getHistoryFile().toUri().getPath());
    Assert.assertEquals(doneConfFilePath.toString(),
        info.getConfFile().toUri().getPath());
  }

  static class HistoryFileManagerTest extends HistoryFileManager {
    public HistoryFileManagerTest() {
      super();
    }
    public HistoryFileInfo getHistoryFileInfo(Path historyFile,
        Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo,
        boolean isInDone) {
      return new HistoryFileInfo(historyFile, confFile, summaryFile,
          jobIndexInfo, isInDone);
    }
  }
}
