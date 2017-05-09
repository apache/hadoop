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
package org.apache.hadoop.yarn.sls;

import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * This is a base class to ease the implementation of SLS-based tests.
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
@SuppressWarnings("VisibilityModifier")
public class BaseSLSRunnerTest {

  @Parameter(value = 0)
  public String schedulerType;

  @Parameter(value = 1)
  public String traceType;

  @Parameter(value = 2)
  public String traceLocation;

  @Parameter(value = 3)
  public String nodeFile;

  protected SLSRunner sls;

  @After
  public void tearDown() throws InterruptedException {
    sls.stop();
  }

  public void runSLS(Configuration conf, long timeout) throws Exception {
    File tempDir = new File("target", UUID.randomUUID().toString());
    final List<Throwable> exceptionList =
        Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace();
            exceptionList.add(e);
          }
        });

    // start the simulator
    File slsOutputDir = new File(tempDir.getAbsolutePath() + "/slsoutput/");

    String[] args;

    switch (traceType) {
    case "OLD_SLS":
      args = new String[] {"-inputsls", traceLocation, "-output",
          slsOutputDir.getAbsolutePath()};
      break;
    case "OLD_RUMEN":
      args = new String[] {"-inputrumen", traceLocation, "-output",
          slsOutputDir.getAbsolutePath()};
      break;
    default:
      args = new String[] {"-tracetype", traceType, "-tracelocation",
          traceLocation, "-output", slsOutputDir.getAbsolutePath()};
    }

    if (nodeFile != null) {
      args = ArrayUtils.addAll(args, new String[] {"-nodes", nodeFile});
    }

    conf.set(YarnConfiguration.RM_SCHEDULER, schedulerType);
    sls = new SLSRunner(conf);
    sls.run(args);

    // wait for timeout seconds before stop, unless there is an uncaught
    // exception in which
    // case fail fast.
    while (timeout >= 0) {
      Thread.sleep(1000);

      if (!exceptionList.isEmpty()) {
        sls.stop();
        Assert.fail("TestSLSRunner catched exception from child thread "
            + "(TaskRunner.Task): " + exceptionList);
        break;
      }
      timeout--;
    }
  }

}
