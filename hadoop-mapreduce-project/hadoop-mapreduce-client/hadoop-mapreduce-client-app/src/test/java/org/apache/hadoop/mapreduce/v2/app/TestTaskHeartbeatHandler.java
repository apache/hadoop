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

package org.apache.hadoop.mapreduce.v2.app;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Test;


public class TestTaskHeartbeatHandler {
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testTimeout() throws InterruptedException {
    EventHandler mockHandler = mock(EventHandler.class);
    Clock clock = new SystemClock();
    TaskHeartbeatHandler hb = new TaskHeartbeatHandler(mockHandler, clock, 1);
    
    
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.TASK_TIMEOUT, 10); //10 ms
    conf.setInt(MRJobConfig.TASK_TIMEOUT_CHECK_INTERVAL_MS, 10); //10 ms
    
    hb.init(conf);
    hb.start();
    try {
      ApplicationId appId = ApplicationId.newInstance(0l, 5);
      JobId jobId = MRBuilderUtils.newJobId(appId, 4);
      TaskId tid = MRBuilderUtils.newTaskId(jobId, 3, TaskType.MAP);
      TaskAttemptId taid = MRBuilderUtils.newTaskAttemptId(tid, 2);
      hb.register(taid);
      Thread.sleep(100);
      //Events only happen when the task is canceled
      verify(mockHandler, times(2)).handle(any(Event.class));
    } finally {
      hb.stop();
    }
  }

}
