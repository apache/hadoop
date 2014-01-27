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

import java.util.Map;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.JobListCache;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import org.apache.hadoop.mapreduce.v2.app.job.Job;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestJobHistory {

  JobHistory jobHistory = null;

  @Test
  public void testRefreshLoadedJobCache() throws Exception {
    HistoryFileManager historyManager = mock(HistoryFileManager.class);
    jobHistory = spy(new JobHistory());
    doReturn(historyManager).when(jobHistory).createHistoryFileManager();

    Configuration conf = new Configuration();
    // Set the cache size to 2
    conf.set(JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE, "2");
    jobHistory.init(conf);
    jobHistory.start();

    CachedHistoryStorage storage = spy((CachedHistoryStorage) jobHistory
        .getHistoryStorage());

    Job[] jobs = new Job[3];
    JobId[] jobIds = new JobId[3];

    for (int i = 0; i < 3; i++) {
      jobs[i] = mock(Job.class);
      jobIds[i] = mock(JobId.class);
      when(jobs[i].getID()).thenReturn(jobIds[i]);
    }

    HistoryFileInfo fileInfo = mock(HistoryFileInfo.class);
    when(historyManager.getFileInfo(any(JobId.class))).thenReturn(fileInfo);
    when(fileInfo.loadJob()).thenReturn(jobs[0]).thenReturn(jobs[1])
        .thenReturn(jobs[2]);

    // getFullJob will put the job in the cache if it isn't there
    for (int i = 0; i < 3; i++) {
      storage.getFullJob(jobs[i].getID());
    }

    Map<JobId, Job> jobCache = storage.getLoadedJobCache();
    // job0 should have been purged since cache size is 2
    assertFalse(jobCache.containsKey(jobs[0].getID()));
    assertTrue(jobCache.containsKey(jobs[1].getID())
        && jobCache.containsKey(jobs[2].getID()));

    // Setting cache size to 3
    conf.set(JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE, "3");
    doReturn(conf).when(storage).createConf();

    when(fileInfo.loadJob()).thenReturn(jobs[0]).thenReturn(jobs[1])
        .thenReturn(jobs[2]);

    jobHistory.refreshLoadedJobCache();

    for (int i = 0; i < 3; i++) {
      storage.getFullJob(jobs[i].getID());
    }

    jobCache = storage.getLoadedJobCache();

    // All three jobs should be in cache since its size is now 3
    for (int i = 0; i < 3; i++) {
      assertTrue(jobCache.containsKey(jobs[i].getID()));
    }
  }

  @Test
  public void testRefreshJobRetentionSettings() throws IOException,
      InterruptedException {
    String root = "mockfs://foo/";
    String historyDoneDir = root + "mapred/history/done";

    long now = System.currentTimeMillis();
    long someTimeYesterday = now - (25l * 3600 * 1000);
    long timeBefore200Secs = now - (200l * 1000);

    // Get yesterday's date in YY/MM/DD format
    String timestampComponent = JobHistoryUtils
        .timestampDirectoryComponent(someTimeYesterday);

    // Create a folder under yesterday's done dir
    Path donePathYesterday = new Path(historyDoneDir, timestampComponent + "/"
        + "000000");
    FileStatus dirCreatedYesterdayStatus = new FileStatus(0, true, 0, 0,
        someTimeYesterday, donePathYesterday);

    // Get today's date in YY/MM/DD format
    timestampComponent = JobHistoryUtils
        .timestampDirectoryComponent(timeBefore200Secs);

    // Create a folder under today's done dir
    Path donePathToday = new Path(historyDoneDir, timestampComponent + "/"
        + "000000");
    FileStatus dirCreatedTodayStatus = new FileStatus(0, true, 0, 0,
        timeBefore200Secs, donePathToday);

    // Create a jhist file with yesterday's timestamp under yesterday's done dir
    Path fileUnderYesterdayDir = new Path(donePathYesterday.toString(),
        "job_1372363578825_0015-" + someTimeYesterday + "-user-Sleep+job-"
            + someTimeYesterday + "-1-1-SUCCEEDED-default.jhist");
    FileStatus fileUnderYesterdayDirStatus = new FileStatus(10, false, 0, 0,
        someTimeYesterday, fileUnderYesterdayDir);

    // Create a jhist file with today's timestamp under today's done dir
    Path fileUnderTodayDir = new Path(donePathYesterday.toString(),
        "job_1372363578825_0016-" + timeBefore200Secs + "-user-Sleep+job-"
            + timeBefore200Secs + "-1-1-SUCCEEDED-default.jhist");
    FileStatus fileUnderTodayDirStatus = new FileStatus(10, false, 0, 0,
        timeBefore200Secs, fileUnderTodayDir);

    HistoryFileManager historyManager = spy(new HistoryFileManager());
    jobHistory = spy(new JobHistory());

    List<FileStatus> fileStatusList = new LinkedList<FileStatus>();
    fileStatusList.add(dirCreatedYesterdayStatus);
    fileStatusList.add(dirCreatedTodayStatus);

    // Make the initial delay of history job cleaner as 4 secs
    doReturn(4).when(jobHistory).getInitDelaySecs();
    doReturn(historyManager).when(jobHistory).createHistoryFileManager();

    List<FileStatus> list1 = new LinkedList<FileStatus>();
    list1.add(fileUnderYesterdayDirStatus);
    doReturn(list1).when(historyManager).scanDirectoryForHistoryFiles(
        eq(donePathYesterday), any(FileContext.class));

    List<FileStatus> list2 = new LinkedList<FileStatus>();
    list2.add(fileUnderTodayDirStatus);
    doReturn(list2).when(historyManager).scanDirectoryForHistoryFiles(
        eq(donePathToday), any(FileContext.class));

    doReturn(fileStatusList).when(historyManager)
        .getHistoryDirsForCleaning(Mockito.anyLong());
    doReturn(true).when(historyManager).deleteDir(any(FileStatus.class));

    JobListCache jobListCache = mock(JobListCache.class);
    HistoryFileInfo fileInfo = mock(HistoryFileInfo.class);
    doReturn(jobListCache).when(historyManager).createJobListCache();
    when(jobListCache.get(any(JobId.class))).thenReturn(fileInfo);

    doNothing().when(fileInfo).delete();

    // Set job retention time to 24 hrs and cleaner interval to 2 secs
    Configuration conf = new Configuration();
    conf.setLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS, 24l * 3600 * 1000);
    conf.setLong(JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS, 2 * 1000);

    jobHistory.init(conf);

    jobHistory.start();

    assertEquals(2 * 1000l, jobHistory.getCleanerInterval());

    // Only yesterday's jhist file should get deleted
    verify(fileInfo, timeout(20000).times(1)).delete();

    fileStatusList.remove(dirCreatedYesterdayStatus);
    // Now reset job retention time to 10 secs
    conf.setLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS, 10 * 1000);
    // Set cleaner interval to 1 sec
    conf.setLong(JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS, 1 * 1000);

    doReturn(conf).when(jobHistory).createConf();
    // Do refresh job retention settings
    jobHistory.refreshJobRetentionSettings();

    // Cleaner interval should be updated
    assertEquals(1 * 1000l, jobHistory.getCleanerInterval());
    // Today's jhist file will also be deleted now since it falls below the
    // retention threshold
    verify(fileInfo, timeout(20000).times(2)).delete();
  }

  @Test
  public void testRefreshLoadedJobCacheUnSupportedOperation() {
    jobHistory = spy(new JobHistory());
    HistoryStorage storage = new HistoryStorage() {

      @Override
      public void setHistoryFileManager(HistoryFileManager hsManager) {
        // TODO Auto-generated method stub

      }

      @Override
      public JobsInfo getPartialJobs(Long offset, Long count, String user,
          String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
          JobState jobState) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Job getFullJob(JobId jobId) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Map<JobId, Job> getAllPartialJobs() {
        // TODO Auto-generated method stub
        return null;
      }
    };

    doReturn(storage).when(jobHistory).createHistoryStorage();

    jobHistory.init(new Configuration());

    jobHistory.start();

    Throwable th = null;
    try {
      jobHistory.refreshLoadedJobCache();
    } catch (Exception e) {
      th = e;
    }

    assertTrue(th instanceof UnsupportedOperationException);
  }
  
  @After
  public void cleanUp() {
    if (jobHistory != null) {
      jobHistory.stop();
    }
  }
}
