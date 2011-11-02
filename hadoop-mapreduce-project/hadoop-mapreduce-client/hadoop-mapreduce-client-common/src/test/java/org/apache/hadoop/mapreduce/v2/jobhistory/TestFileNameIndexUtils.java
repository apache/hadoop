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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

import org.junit.Assert;
import org.junit.Test;

public class TestFileNameIndexUtils {

  private static final String JOB_HISTORY_FILE_FORMATTER = "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + FileNameIndexUtils.DELIMITER + "%s"
    + JobHistoryUtils.JOB_HISTORY_FILE_EXTENSION;

  private static final String JOB_ID = "job_1317928501754_0001";
  private static final String SUBMIT_TIME = "1317928742025";
  private static final String USER_NAME = "username";
  private static final String USER_NAME_WITH_DELIMITER = "user"
    + FileNameIndexUtils.DELIMITER + "name";
  private static final String USER_NAME_WITH_DELIMITER_ESCAPE = "user"
    + FileNameIndexUtils.DELIMITER_ESCAPE + "name";
  private static final String JOB_NAME = "mapreduce";
  private static final String JOB_NAME_WITH_DELIMITER = "map"
    + FileNameIndexUtils.DELIMITER + "reduce";
  private static final String JOB_NAME_WITH_DELIMITER_ESCAPE = "map"
    + FileNameIndexUtils.DELIMITER_ESCAPE + "reduce";
  private static final String FINISH_TIME = "1317928754958";
  private static final String NUM_MAPS = "1";
  private static final String NUM_REDUCES = "1";
  private static final String JOB_STATUS = "SUCCEEDED";

  @Test
  public void testUserNamePercentEncoding() throws IOException{
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME_WITH_DELIMITER);
    info.setJobName(JOB_NAME);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    Assert.assertTrue("User name not encoded correctly into job history file",
        jobHistoryFile.contains(USER_NAME_WITH_DELIMITER_ESCAPE));
  }

  @Test
  public void testUserNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME_WITH_DELIMITER_ESCAPE,
        JOB_NAME,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    Assert.assertEquals("User name doesn't match",
        USER_NAME_WITH_DELIMITER, info.getUser());
  }

  @Test
  public void testJobNamePercentEncoding() throws IOException {
    JobIndexInfo info = new JobIndexInfo();
    JobID oldJobId = JobID.forName(JOB_ID);
    JobId jobId = TypeConverter.toYarn(oldJobId);
    info.setJobId(jobId);
    info.setSubmitTime(Long.parseLong(SUBMIT_TIME));
    info.setUser(USER_NAME);
    info.setJobName(JOB_NAME_WITH_DELIMITER);
    info.setFinishTime(Long.parseLong(FINISH_TIME));
    info.setNumMaps(Integer.parseInt(NUM_MAPS));
    info.setNumReduces(Integer.parseInt(NUM_REDUCES));
    info.setJobStatus(JOB_STATUS);

    String jobHistoryFile = FileNameIndexUtils.getDoneFileName(info);
    Assert.assertTrue("Job name not encoded correctly into job history file",
        jobHistoryFile.contains(JOB_NAME_WITH_DELIMITER_ESCAPE));
  }

  @Test
  public void testJobNamePercentDecoding() throws IOException {
    String jobHistoryFile = String.format(JOB_HISTORY_FILE_FORMATTER,
        JOB_ID,
        SUBMIT_TIME,
        USER_NAME,
        JOB_NAME_WITH_DELIMITER_ESCAPE,
        FINISH_TIME,
        NUM_MAPS,
        NUM_REDUCES,
        JOB_STATUS);

    JobIndexInfo info = FileNameIndexUtils.getIndexInfo(jobHistoryFile);
    Assert.assertEquals("Job name doesn't match",
        JOB_NAME_WITH_DELIMITER, info.getJobName());
  }
}
