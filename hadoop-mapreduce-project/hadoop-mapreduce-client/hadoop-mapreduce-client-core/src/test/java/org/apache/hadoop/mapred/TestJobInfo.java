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

package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * test class JobInfo
 * 
 * 
 */
public class TestJobInfo {
  @Test (timeout=5000)
  public void testJobInfo() throws IOException {
    JobID jid = new JobID("001", 1);
    Text user = new Text("User");
    Path path = new Path("/tmp/test");
    JobInfo info = new JobInfo(jid, user, path);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    info.write(new DataOutputStream(out));

    JobInfo copyinfo = new JobInfo();
    copyinfo.readFields(new DataInputStream(new ByteArrayInputStream(out
        .toByteArray())));
    assertEquals(info.getJobID().toString(), copyinfo.getJobID().toString());
    assertEquals(info.getJobSubmitDir().getName(), copyinfo.getJobSubmitDir()
        .getName());
    assertEquals(info.getUser().toString(), copyinfo.getUser().toString());

  }
  
  @Test(timeout = 5000)
  public void testTaskID() throws IOException, InterruptedException {
    JobID jobid = new JobID("1014873536921", 6);
    TaskID tid = new TaskID(jobid, TaskType.MAP, 0);
    org.apache.hadoop.mapred.TaskID tid1 =
        org.apache.hadoop.mapred.TaskID.downgrade(tid);
    org.apache.hadoop.mapred.TaskReport treport =
        new org.apache.hadoop.mapred.TaskReport(tid1, 0.0f,
          State.FAILED.toString(), null, TIPStatus.FAILED, 100, 100,
          new org.apache.hadoop.mapred.Counters());
    Assert
      .assertEquals(treport.getTaskId(), "task_1014873536921_0006_m_000000");
    Assert.assertEquals(treport.getTaskID().toString(),
      "task_1014873536921_0006_m_000000");
  }
}
