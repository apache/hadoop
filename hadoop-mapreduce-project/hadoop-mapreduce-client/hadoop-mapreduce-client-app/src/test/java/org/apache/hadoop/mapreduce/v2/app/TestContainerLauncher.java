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

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.junit.Test;

public class TestContainerLauncher {

  static final Log LOG = LogFactory
      .getLog(TestContainerLauncher.class);

  @Test
  public void testSlowNM() throws Exception {
    test(false);
  }

  @Test
  public void testSlowNMWithInterruptsSwallowed() throws Exception {
    test(true);
  }

  private void test(boolean swallowInterrupts) throws Exception {

    MRApp app = new MRAppWithSlowNM(swallowInterrupts);

    Configuration conf = new Configuration();
    int maxAttempts = 1;
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, maxAttempts);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);

    // Set low timeout for NM commands
    conf.setInt(ContainerLauncher.MR_AM_NM_COMMAND_TIMEOUT, 3000);

    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());

    Task task = tasks.values().iterator().next();
    app.waitForState(task, TaskState.SCHEDULED);

    Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator()
        .next().getAttempts();
    Assert.assertEquals("Num attempts is not correct", maxAttempts, attempts
        .size());

    TaskAttempt attempt = attempts.values().iterator().next();
    app.waitForState(attempt, TaskAttemptState.ASSIGNED);

    app.waitForState(job, JobState.FAILED);

    LOG.info("attempt.getDiagnostics: " + attempt.getDiagnostics());
    Assert.assertTrue(attempt.getDiagnostics().toString().contains(
        "Container launch failed for container_0_0000_01_000000 : "));
    Assert.assertTrue(attempt.getDiagnostics().toString().contains(
        ": java.lang.InterruptedException"));

    app.stop();
  }

  private static class MRAppWithSlowNM extends MRApp {

    final boolean swallowInterrupts;

    public MRAppWithSlowNM(boolean swallowInterrupts) {
      super(1, 0, false, "TestContainerLauncher", true);
      this.swallowInterrupts = swallowInterrupts;
    }

    @Override
    protected ContainerLauncher createContainerLauncher(AppContext context) {
      return new ContainerLauncherImpl(context) {
        @Override
        protected ContainerManager getCMProxy(ContainerId containerID,
            String containerManagerBindAddr, ContainerToken containerToken)
            throws IOException {
          try {
            synchronized (this) {
              wait(); // Just hang the thread simulating a very slow NM.
            }
          } catch (InterruptedException e) {
            LOG.info(e);
            if (!swallowInterrupts) {
              throw new IOException(e);
            } else {
              Thread.currentThread().interrupt();
            }
          }
          return null;
        }
      };
    };
  }
}
