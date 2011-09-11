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


import java.io.File;
import java.io.FileWriter;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.mapred.Task;

import junit.framework.TestCase;

/**
 * The test case tests whether {@link LinuxTaskController} passes all required
 * jvm properties in its initializeJob() and truncateLogsAsUser() methods,
 * which launches jvm through the native task-controller.
 */
public class TestLinuxTaskControllerLaunchArgs extends TestCase {
  private static final Log LOG = LogFactory.getLog(
                                   TestLinuxTaskControllerLaunchArgs.class);
  private static File testDir = new File(System.getProperty("test.build.data",
                "/tmp"), TestLinuxTaskControllerLaunchArgs.class.getName());
  private static File fakeTaskController = new File(testDir, "faketc.sh");
  private static File mapredLocal = new File(testDir, "mapred/local");
  private TaskController ltc;
  private boolean initialized = false;
  private String user = new String("testuser");
  private InetSocketAddress addr = new InetSocketAddress("localhost", 3209);

  Configuration conf = new Configuration();

  // Do-nothing fake {@link MapTask} class
  public static class MyMapTask extends MapTask {
    @Override
    public void write(DataOutput out) throws IOException {
      // nothing
    }
  }

  
  // The shell script is used to fake the native task-controller.
  // It checks the arguments for required java properties and args.
  protected void createFakeTCScript() throws Exception {
    FileWriter out = new FileWriter(fakeTaskController);
    out.write("#!/bin/bash\n");
    // setup() calls with zero args and expects 1 in return.
    out.write("if [ $# -eq 0 ]; then exit 1; fi\n");

    // Check for java, classpath, h.log.dir, h.root.logger and java.library.path
    out.write("for LARG in \"$@\"\n");
    out.write("do case \"$LARG\" in\n");
    out.write("*/java) LTC_ARG1=1;;\n");
    out.write("-classpath) LTC_ARG2=1;;\n");
    out.write("-Dhadoop.log.dir*) LTC_ARG3=1;;\n");
    out.write("-Dhadoop.root.logger*) LTC_ARG4=1;;\n");
    out.write("-Djava.library.path*) LTC_ARG5=1;;\n");
    out.write("esac; done\n");
    out.write("LTC_ARGS=$((LTC_ARG1+LTC_ARG2+LTC_ARG3+LTC_ARG4+LTC_ARG5))\n");
    out.write("if [ $LTC_ARGS -eq 5 ]; then exit 0; else exit 22; fi\n");
    out.close();
    fakeTaskController.setExecutable(true);
  }

  protected void initMyTest() throws Exception {
    testDir.mkdirs();
    mapredLocal.mkdirs();
    createFakeTCScript();
    conf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, mapredLocal.toString());

    // Set the task-controller binary path.
    conf.set("mapreduce.tasktracker.task-controller.exe", fakeTaskController.toString());
    ltc = new LinuxTaskController();
    ltc.setConf(conf);

    // LinuxTaskController runs task-controller in setup() with no 
    // argument and expects 1 in return
    try {
      ltc.setup(new LocalDirAllocator(mapredLocal.toString()),
                 new LocalStorage(new String[]{mapredLocal.toString()}));
    } catch (IOException ie) {
      fail("Error running task-controller from setup().");
    }

    initialized = true;
  }


  /**
   * LinuxTaskController runs task-controller and it runs JobLocalizer
   * in initializeJob(). task-controller should be prodived with all
   * necessary java properties to launch JobLocalizer successfully.
   */
  public void testLTCCallInitializeJob() throws Exception {
    if (!initialized) {
      initMyTest();
    }
    
    try {
      ltc.initializeJob(user, new String("jobid"), new Path("/cred.xml"),
                                       new Path("/job.xml"), null, addr);
    } catch (IOException ie) {
      fail("Missing argument when running task-controller from " +
                                                   "initializeJob().\n");
    }
  }

  /**
   * LinuxTaskController runs task-controller and it runs TaskLogsTruncater
   * in truncateLogsAsUser(). task-controller should be prodived with all
   * necessary java properties to launch JobLocalizer successfully.
   */
  public void testLTCCallTruncateLogsAsUser() throws Exception {
    if (!initialized) {
      initMyTest();
    }

    List<Task> tasks = new ArrayList<Task>();
    tasks.add(new MyMapTask());

    try {
      ltc.truncateLogsAsUser(user, tasks);
    } catch (IOException ie) {
      fail("Missing argument when running task-controller from " +
                                               "truncateLogsAsUser()\n");
    }
  }
}
