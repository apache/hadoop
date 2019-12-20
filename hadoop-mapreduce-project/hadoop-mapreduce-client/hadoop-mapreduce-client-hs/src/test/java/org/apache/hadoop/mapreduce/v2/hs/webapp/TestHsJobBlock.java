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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.*;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.hs.CompletedJob;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.UnparsedJob;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the HsJobBlock generated for oversized jobs in JHS.
 */
public class TestHsJobBlock {

  @Test
  public void testHsJobBlockForOversizeJobShouldDisplayWarningMessage() {
    int maxAllowedTaskNum = 100;

    Configuration config = new Configuration();
    config.setInt(JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX, maxAllowedTaskNum);

    JobHistory jobHistory =
        new JobHistoryStubWithAllOversizeJobs(maxAllowedTaskNum);
    jobHistory.init(config);

    HsJobBlock jobBlock = new HsJobBlock(jobHistory) {
      // override this so that job block can fetch a job id.
      @Override
      public Map<String, String> moreParams() {
        Map<String, String> map = new HashMap<>();
        map.put(AMParams.JOB_ID, "job_0000_0001");
        return map;
      }
    };

    // set up the test block to render HsJobBLock to
    OutputStream outputStream = new ByteArrayOutputStream();
    HtmlBlock.Block block = createBlockToCreateTo(outputStream);

    jobBlock.render(block);

    block.getWriter().flush();
    String out = outputStream.toString();
    Assert.assertTrue("Should display warning message for jobs that have too " +
        "many tasks", out.contains("Any job larger than " + maxAllowedTaskNum +
            " will not be loaded"));
  }

  @Test
  public void testHsJobBlockForNormalSizeJobShouldNotDisplayWarningMessage() {

    Configuration config = new Configuration();
    config.setInt(JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX, -1);

    JobHistory jobHistory = new JobHitoryStubWithAllNormalSizeJobs();
    jobHistory.init(config);

    HsJobBlock jobBlock = new HsJobBlock(jobHistory) {
      // override this so that the job block can fetch a job id.
      @Override
      public Map<String, String> moreParams() {
        Map<String, String> map = new HashMap<>();
        map.put(AMParams.JOB_ID, "job_0000_0001");
        return map;
      }

      // override this to avoid view context lookup in render()
      @Override
      public ResponseInfo info(String about) {
        return new ResponseInfo().about(about);
      }

      // override this to avoid view context lookup in render()
      @Override
      public String url(String... parts) {
        return StringHelper.ujoin("", parts);
      }
    };

    // set up the test block to render HsJobBLock to
    OutputStream outputStream = new ByteArrayOutputStream();
    HtmlBlock.Block block = createBlockToCreateTo(outputStream);

    jobBlock.render(block);

    block.getWriter().flush();
    String out = outputStream.toString();

    Assert.assertTrue("Should display job overview for the job.",
        out.contains("ApplicationMaster"));
  }

  private static HtmlBlock.Block createBlockToCreateTo(
      OutputStream outputStream) {
    PrintWriter printWriter = new PrintWriter(outputStream);
    HtmlBlock html = new HtmlBlockForTest();
    return new BlockForTest(html, printWriter, 10, false) {
      @Override
      protected void subView(Class<? extends SubView> cls) {
      }
    };
  };

  /**
   * A JobHistory stub that treat all jobs as oversized and therefore will
   * not parse their job history files but return a UnparseJob instance.
   */
  static class JobHistoryStubWithAllOversizeJobs extends JobHistory {
    private final int maxAllowedTaskNum;

    public JobHistoryStubWithAllOversizeJobs(int maxAllowedTaskNum) {
      this.maxAllowedTaskNum = maxAllowedTaskNum;
    }

    @Override
    protected HistoryFileManager createHistoryFileManager() {
      HistoryFileManager historyFileManager;
      try {
        HistoryFileInfo historyFileInfo =
            createUnparsedJobHistoryFileInfo(maxAllowedTaskNum);

        historyFileManager = mock(HistoryFileManager.class);
        when(historyFileManager.getFileInfo(any(JobId.class))).thenReturn(
            historyFileInfo);
      } catch (IOException ex) {
        // this should never happen
        historyFileManager = super.createHistoryFileManager();
      }
      return historyFileManager;
    }

    private static HistoryFileInfo createUnparsedJobHistoryFileInfo(
        int maxAllowedTaskNum) throws IOException {
      HistoryFileInfo fileInfo = mock(HistoryFileInfo.class);

      // create an instance of UnparsedJob for a large job
      UnparsedJob unparsedJob = mock(UnparsedJob.class);
      when(unparsedJob.getMaxTasksAllowed()).thenReturn(maxAllowedTaskNum);
      when(unparsedJob.getTotalMaps()).thenReturn(maxAllowedTaskNum);
      when(unparsedJob.getTotalReduces()).thenReturn(maxAllowedTaskNum);

      when(fileInfo.loadJob()).thenReturn(unparsedJob);

      return fileInfo;
    }
  }

  /**
   * A JobHistory stub that treats all jobs as normal size and therefore will
   * return a CompletedJob on HistoryFileInfo.loadJob().
   */
  static class JobHitoryStubWithAllNormalSizeJobs extends  JobHistory {
    @Override
    public HistoryFileManager createHistoryFileManager() {
      HistoryFileManager historyFileManager;
      try {
        HistoryFileInfo historyFileInfo = createParsedJobHistoryFileInfo();

        historyFileManager = mock(HistoryFileManager.class);
        when(historyFileManager.getFileInfo(any(JobId.class))).thenReturn(
            historyFileInfo);
      } catch (IOException ex) {
        // this should never happen
        historyFileManager = super.createHistoryFileManager();
      }
      return historyFileManager;

    }

    private static HistoryFileInfo createParsedJobHistoryFileInfo()
        throws IOException {
      HistoryFileInfo fileInfo = mock(HistoryFileInfo.class);
      CompletedJob job = createFakeCompletedJob();
      when(fileInfo.loadJob()).thenReturn(job);
      return fileInfo;
    }


    private static CompletedJob createFakeCompletedJob() {
      CompletedJob job = mock(CompletedJob.class);

      when(job.getTotalMaps()).thenReturn(0);
      when(job.getCompletedMaps()).thenReturn(0);
      when(job.getTotalReduces()).thenReturn(0);
      when(job.getCompletedReduces()).thenReturn(0);

      JobId jobId = createFakeJobId();
      when(job.getID()).thenReturn(jobId);

      JobReport jobReport = mock(JobReport.class);
      when(jobReport.getSubmitTime()).thenReturn(-1L);
      when(jobReport.getStartTime()).thenReturn(-1L);
      when(jobReport.getFinishTime()).thenReturn(-1L);
      when(job.getReport()).thenReturn(jobReport);

      when(job.getAMInfos()).thenReturn(new ArrayList<AMInfo>());
      when(job.getDiagnostics()).thenReturn(new ArrayList<String>());
      when(job.getName()).thenReturn("fake completed job");
      when(job.getQueueName()).thenReturn("default");
      when(job.getUserName()).thenReturn("junit");
      when(job.getState()).thenReturn(JobState.ERROR);
      when(job.getAllCounters()).thenReturn(new Counters());
      when(job.getTasks()).thenReturn(new HashMap<TaskId, Task>());

      return job;
    }

    private static JobId createFakeJobId() {
      JobId jobId = new JobIdPBImpl();
      jobId.setId(0);

      ApplicationId appId = mock(ApplicationId.class);
      when(appId.getClusterTimestamp()).thenReturn(0L);
      when(appId.getId()).thenReturn(0);

      jobId.setAppId(appId);

      return jobId;
    }
  }

}
