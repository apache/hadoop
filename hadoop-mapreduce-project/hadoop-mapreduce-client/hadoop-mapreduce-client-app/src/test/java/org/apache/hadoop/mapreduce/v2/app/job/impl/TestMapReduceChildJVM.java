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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.junit.Test;

public class TestMapReduceChildJVM {

  private static final Log LOG = LogFactory.getLog(TestMapReduceChildJVM.class);

  @Test (timeout = 30000)
  public void testCommandLine() throws Exception {

    MyMRApp app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    Assert.assertEquals(
      "[" + envVar("JAVA_HOME") + "/bin/java" +
      " -Djava.net.preferIPv4Stack=true" +
      " -Dhadoop.metrics.log.level=WARN" +
      "  -Xmx200m -Djava.io.tmpdir=" + envVar("PWD") + "/tmp" +
      " -Dlog4j.configuration=container-log4j.properties" +
      " -Dyarn.app.mapreduce.container.log.dir=<LOG_DIR>" +
      " -Dyarn.app.mapreduce.container.log.filesize=0" +
      " -Dhadoop.root.logger=INFO,CLA" +
      " org.apache.hadoop.mapred.YarnChild 127.0.0.1" +
      " 54321" +
      " attempt_0_0000_m_000000_0" +
      " 0" +
      " 1><LOG_DIR>/stdout" +
      " 2><LOG_DIR>/stderr ]", app.myCommandLine);
  }

  private static final class MyMRApp extends MRApp {

    private String myCommandLine;

    public MyMRApp(int maps, int reduces, boolean autoComplete,
        String testName, boolean cleanOnStart) {
      super(maps, reduces, autoComplete, testName, cleanOnStart);
    }

    @Override
    protected ContainerLauncher createContainerLauncher(AppContext context) {
      return new MockContainerLauncher() {
        @Override
        public void handle(ContainerLauncherEvent event) {
          if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {
            ContainerRemoteLaunchEvent launchEvent = (ContainerRemoteLaunchEvent) event;
            ContainerLaunchContext launchContext = launchEvent.getContainer();
            String cmdString = launchContext.getCommands().toString();
            LOG.info("launchContext " + cmdString);
            myCommandLine = cmdString;
          }
          super.handle(event);
        }
      };
    }
  }

  /**
   * Returns platform-specific string for retrieving the value of an environment
   * variable with the given name.  On Unix, this returns $name.  On Windows,
   * this returns %name%.
   * 
   * @param name String environment variable name
   * @return String for retrieving value of environment variable
   */
  private static String envVar(String name) {
    return Shell.WINDOWS ? '%' + name + '%' : '$' + name;
  }
}
