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
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.hs.CompletedTask;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCompletedTask{

  @Test
  public void testTaskStartTimes() {
    
    TaskId taskId = Mockito.mock(TaskId.class); 
    TaskInfo taskInfo = Mockito.mock(TaskInfo.class);
    Map<TaskAttemptID, TaskAttemptInfo> taskAttempts
      = new TreeMap<TaskAttemptID, TaskAttemptInfo>();
    
    TaskAttemptID id = new TaskAttemptID("0", 0, TaskType.MAP, 0, 0);
    TaskAttemptInfo info = Mockito.mock(TaskAttemptInfo.class);
    Mockito.when(info.getAttemptId()).thenReturn(id);
    Mockito.when(info.getStartTime()).thenReturn(10l);
    taskAttempts.put(id, info);
    
    id = new TaskAttemptID("1", 0, TaskType.MAP, 1, 1);
    info = Mockito.mock(TaskAttemptInfo.class);
    Mockito.when(info.getAttemptId()).thenReturn(id);
    Mockito.when(info.getStartTime()).thenReturn(20l);
    taskAttempts.put(id, info);
    
    
    Mockito.when(taskInfo.getAllTaskAttempts()).thenReturn(taskAttempts);
    CompletedTask task = new CompletedTask(taskId, taskInfo);
    TaskReport report = task.getReport();

    // Make sure the startTime returned by report is the lesser of the 
    // attempy launch times
    Assert.assertTrue(report.getStartTime() == 10);
  }
}
