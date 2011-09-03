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

package org.apache.hadoop.mapreduce.v2.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.MRConstants;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestMRApps {

  @Test public void testJobIDtoString() {
    JobId jid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jid.setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    assertEquals("job_0_0_0", MRApps.toString(jid));
  }

  @Test public void testToJobID() {
    JobId jid = MRApps.toJobID("job_1_1_1");
    assertEquals(1, jid.getAppId().getClusterTimestamp());
    assertEquals(1, jid.getAppId().getId());
    assertEquals(1, jid.getId());
  }

  @Test(expected=YarnException.class) public void testJobIDShort() {
    MRApps.toJobID("job_0_0");
  }

  //TODO_get.set
  @Test public void testTaskIDtoString() {
    TaskId tid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    tid.setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    tid.getJobId().setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    tid.setTaskType(TaskType.MAP);
    TaskType type = tid.getTaskType();
    System.err.println(type);
    type = TaskType.REDUCE;
    System.err.println(type);
    System.err.println(tid.getTaskType());
    assertEquals("task_0_0_0_m_0", MRApps.toString(tid));
    tid.setTaskType(TaskType.REDUCE);
    assertEquals("task_0_0_0_r_0", MRApps.toString(tid));
  }

  @Test public void testToTaskID() {
    TaskId tid = MRApps.toTaskID("task_1_2_3_r_4");
    assertEquals(1, tid.getJobId().getAppId().getClusterTimestamp());
    assertEquals(2, tid.getJobId().getAppId().getId());
    assertEquals(3, tid.getJobId().getId());
    assertEquals(TaskType.REDUCE, tid.getTaskType());
    assertEquals(4, tid.getId());

    tid = MRApps.toTaskID("task_1_2_3_m_4");
    assertEquals(TaskType.MAP, tid.getTaskType());
  }

  @Test(expected=YarnException.class) public void testTaskIDShort() {
    MRApps.toTaskID("task_0_0_0_m");
  }

  @Test(expected=YarnException.class) public void testTaskIDBadType() {
    MRApps.toTaskID("task_0_0_0_x_0");
  }

  //TODO_get.set
  @Test public void testTaskAttemptIDtoString() {
    TaskAttemptId taid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taid.setTaskId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class));
    taid.getTaskId().setTaskType(TaskType.MAP);
    taid.getTaskId().setJobId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class));
    taid.getTaskId().getJobId().setAppId(RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class));
    assertEquals("attempt_0_0_0_m_0_0", MRApps.toString(taid));
  }

  @Test public void testToTaskAttemptID() {
    TaskAttemptId taid = MRApps.toTaskAttemptID("attempt_0_1_2_m_3_4");
    assertEquals(0, taid.getTaskId().getJobId().getAppId().getClusterTimestamp());
    assertEquals(1, taid.getTaskId().getJobId().getAppId().getId());
    assertEquals(2, taid.getTaskId().getJobId().getId());
    assertEquals(3, taid.getTaskId().getId());
    assertEquals(4, taid.getId());
  }

  @Test(expected=YarnException.class) public void testTaskAttemptIDShort() {
    MRApps.toTaskAttemptID("attempt_0_0_0_m_0");
  }

  @Test public void testGetJobFileWithUser() {
    Configuration conf = new Configuration();
    conf.set(MRConstants.APPS_STAGING_DIR_KEY, "/my/path/to/staging");
    String jobFile = MRApps.getJobFile(conf, "dummy-user", new JobID("dummy-job", 12345));
    assertNotNull("getJobFile results in null.", jobFile);
    assertEquals("jobFile with specified user is not as expected.",
        "/my/path/to/staging/dummy-user/.staging/job_dummy-job_12345/job.xml", jobFile);
  }

}
