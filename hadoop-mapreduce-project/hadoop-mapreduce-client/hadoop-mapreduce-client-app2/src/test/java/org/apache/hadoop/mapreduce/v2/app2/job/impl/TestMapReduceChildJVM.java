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

package org.apache.hadoop.mapreduce.v2.app2.job.impl;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.MRApp;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.junit.Test;

public class TestMapReduceChildJVM {

  private static final Log LOG = LogFactory.getLog(TestMapReduceChildJVM.class);

  @Test
  public void testCommandLine() throws Exception {

    MyMRApp app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    // TODO XXX: Change classname after renaming back to YarnChild
    Assert.assertEquals(
      "[exec $JAVA_HOME/bin/java" +
      " -Djava.net.preferIPv4Stack=true" +
      " -Dhadoop.metrics.log.level=WARN" +
      "  -Xmx200m -Djava.io.tmpdir=$PWD/tmp" +
      " -Dlog4j.configuration=container-log4j.properties" +
      " -Dyarn.app.mapreduce.container.log.dir=<LOG_DIR>" +
      " -Dyarn.app.mapreduce.container.log.filesize=0" +
      " -Dhadoop.root.logger=INFO,CLA" +
      " org.apache.hadoop.mapred.YarnChild2 127.0.0.1" +
      " 54321" +
      " job_0_0000" +
      " MAP" +
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
        public void handle(NMCommunicatorEvent event) {
          if (event.getType() == NMCommunicatorEventType.CONTAINER_LAUNCH_REQUEST) {
            NMCommunicatorLaunchRequestEvent launchEvent = (NMCommunicatorLaunchRequestEvent) event;
            ContainerLaunchContext launchContext = launchEvent.getContainerLaunchContext();
            String cmdString = launchContext.getCommands().toString();
            LOG.info("launchContext " + cmdString);
            myCommandLine = cmdString;
          }
          super.handle(event);
        }
      };
    }
  }
}
