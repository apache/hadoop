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

import java.util.Map;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.junit.Test;

public class TestMapReduceChildJVM {

  private static final Log LOG = LogFactory.getLog(TestMapReduceChildJVM.class);

  @Test (timeout = 30000)
  public void testCommandLine() throws Exception {

    MyMRApp app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    Assert.assertEquals(
      "[" + MRApps.crossPlatformify("JAVA_HOME") + "/bin/java" +
      " -Djava.net.preferIPv4Stack=true" +
      " -Dhadoop.metrics.log.level=WARN" +
      "  -Xmx200m -Djava.io.tmpdir=" + MRApps.crossPlatformify("PWD") + "/tmp" +
      " -Dlog4j.configuration=container-log4j.properties" +
      " -Dyarn.app.container.log.dir=<LOG_DIR>" +
      " -Dyarn.app.container.log.filesize=0" +
      " -Dhadoop.root.logger=INFO,CLA" +
      " org.apache.hadoop.mapred.YarnChild 127.0.0.1" +
      " 54321" +
      " attempt_0_0000_m_000000_0" +
      " 0" +
      " 1><LOG_DIR>/stdout" +
      " 2><LOG_DIR>/stderr ]", app.myCommandLine);
    
    Assert.assertTrue("HADOOP_ROOT_LOGGER not set for job",
      app.cmdEnvironment.containsKey("HADOOP_ROOT_LOGGER"));
    Assert.assertEquals("INFO,console",
      app.cmdEnvironment.get("HADOOP_ROOT_LOGGER"));
    Assert.assertTrue("HADOOP_CLIENT_OPTS not set for job",
      app.cmdEnvironment.containsKey("HADOOP_CLIENT_OPTS"));
    Assert.assertEquals("", app.cmdEnvironment.get("HADOOP_CLIENT_OPTS"));
  }
  
  @Test (timeout = 30000)
  public void testCommandLineWithLog4JConifg() throws Exception {

    MyMRApp app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, true);
    String testLogPropertieFile = "test-log4j.properties";
    String testLogPropertiePath = "../"+"test-log4j.properties";
    conf.set(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, testLogPropertiePath);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();

    Assert.assertEquals(
      "[" + MRApps.crossPlatformify("JAVA_HOME") + "/bin/java" +
      " -Djava.net.preferIPv4Stack=true" +
      " -Dhadoop.metrics.log.level=WARN" +
      "  -Xmx200m -Djava.io.tmpdir=" + MRApps.crossPlatformify("PWD") + "/tmp" +
      " -Dlog4j.configuration=" + testLogPropertieFile +
      " -Dyarn.app.container.log.dir=<LOG_DIR>" +
      " -Dyarn.app.container.log.filesize=0" +
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
    private Map<String, String> cmdEnvironment;

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
            ContainerLaunchContext launchContext =
                launchEvent.getContainerLaunchContext();
            String cmdString = launchContext.getCommands().toString();
            LOG.info("launchContext " + cmdString);
            myCommandLine = cmdString;
            cmdEnvironment = launchContext.getEnvironment();
          }
          super.handle(event);
        }
      };
    }
  }
  
  @Test
  public void testEnvironmentVariables() throws Exception {
    MyMRApp app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    Configuration conf = new Configuration();
    conf.set(JobConf.MAPRED_MAP_TASK_ENV, "HADOOP_CLIENT_OPTS=test");
    conf.setStrings(MRJobConfig.MAP_LOG_LEVEL, "WARN");
    conf.setBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, false);
    Job job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    
    Assert.assertTrue("HADOOP_ROOT_LOGGER not set for job",
      app.cmdEnvironment.containsKey("HADOOP_ROOT_LOGGER"));
    Assert.assertEquals("WARN,console",
      app.cmdEnvironment.get("HADOOP_ROOT_LOGGER"));
    Assert.assertTrue("HADOOP_CLIENT_OPTS not set for job",
      app.cmdEnvironment.containsKey("HADOOP_CLIENT_OPTS"));
    Assert.assertEquals("test", app.cmdEnvironment.get("HADOOP_CLIENT_OPTS"));

    // Try one more.
    app = new MyMRApp(1, 0, true, this.getClass().getName(), true);
    conf = new Configuration();
    conf.set(JobConf.MAPRED_MAP_TASK_ENV, "HADOOP_ROOT_LOGGER=trace");
    job = app.submit(conf);
    app.waitForState(job, JobState.SUCCEEDED);
    app.verifyCompleted();
    
    Assert.assertTrue("HADOOP_ROOT_LOGGER not set for job",
      app.cmdEnvironment.containsKey("HADOOP_ROOT_LOGGER"));
    Assert.assertEquals("trace",
      app.cmdEnvironment.get("HADOOP_ROOT_LOGGER"));
  }
}
