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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestSLSRunner {

  @Test
  @SuppressWarnings("all")
  public void testSimulatorRunning() throws Exception {
    File tempDir = new File("target", UUID.randomUUID().toString());
    final List<Throwable> exceptionList =
        Collections.synchronizedList(new ArrayList<Throwable>());

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptionList.add(e);
      }
    });

    // start the simulator
    File slsOutputDir = new File(tempDir.getAbsolutePath() + "/slsoutput/");
    String args[] = new String[]{
            "-inputrumen", "src/main/data/2jobs2min-rumen-jh.json",
            "-output", slsOutputDir.getAbsolutePath()};
    SLSRunner.main(args);

    // wait for 20 seconds before stop
    int count = 20;
    while (count >= 0) {
      Thread.sleep(1000);

      if (! exceptionList.isEmpty()) {
        SLSRunner.getRunner().stop();
        Assert.fail("TestSLSRunner catched exception from child thread " +
            "(TaskRunner.Task): " + exceptionList.get(0).getMessage());
        break;
      }
      count--;
    }

    SLSRunner.getRunner().stop();
  }

}
