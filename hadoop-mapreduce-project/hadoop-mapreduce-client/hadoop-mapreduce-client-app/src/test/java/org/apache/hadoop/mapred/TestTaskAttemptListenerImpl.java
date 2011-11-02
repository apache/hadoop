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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskHeartbeatHandler;
import org.junit.Test;

public class TestTaskAttemptListenerImpl {
  public static class MockTaskAttemptListenerImpl extends TaskAttemptListenerImpl {

    public MockTaskAttemptListenerImpl(AppContext context,
        JobTokenSecretManager jobTokenSecretManager,
        TaskHeartbeatHandler hbHandler) {
      super(context, jobTokenSecretManager);
      this.taskHeartbeatHandler = hbHandler;
    }
    
    @Override
    protected void registerHeartbeatHandler() {
      //Empty
    }

    @Override
    protected void startRpcServer() {
      //Empty
    }
    
    @Override
    protected void stopRpcServer() {
      //Empty
    }
  }
  
  @Test
  public void testGetTask() throws IOException {
    AppContext appCtx = mock(AppContext.class);
    JobTokenSecretManager secret = mock(JobTokenSecretManager.class); 
    TaskHeartbeatHandler hbHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptListenerImpl listener = 
      new MockTaskAttemptListenerImpl(appCtx, secret, hbHandler);
    Configuration conf = new Configuration();
    listener.init(conf);
    listener.start();
    JVMId id = new JVMId("foo",1, true, 1);
    WrappedJvmID wid = new WrappedJvmID(id.getJobId(), id.isMap, id.getId());

    //The JVM ID has not been registered yet so we should kill it.
    JvmContext context = new JvmContext();
    context.jvmId = id; 
    JvmTask result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);
    
    //Now register the JVM, and see
    listener.registerPendingTask(wid);
    result = listener.getTask(context);
    assertNull(result);
    
    TaskAttemptId attemptID = mock(TaskAttemptId.class);
    Task task = mock(Task.class);
    //Now put a task with the ID
    listener.registerLaunchedTask(attemptID, task, wid);
    verify(hbHandler).register(attemptID);
    result = listener.getTask(context);
    assertNotNull(result);
    assertFalse(result.shouldDie);
    
    //Verify that if we call it again a second time we are told to die.
    result = listener.getTask(context);
    assertNotNull(result);
    assertTrue(result.shouldDie);
    
    listener.unregister(attemptID, wid);
    listener.stop();
  }
}
