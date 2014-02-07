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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class TestDSFailedAppMaster extends ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(TestDSFailedAppMaster.class);

  @Override
  public void run() throws YarnException, IOException {
    super.run();

    // for the 2nd attempt.
    if (appAttemptID.getAttemptId() == 2) {
      // should reuse the earlier running container, so numAllocatedContainers
      // should be set to 1. And should ask no more containers, so
      // numRequestedContainers should be set to 0.
      if (numAllocatedContainers.get() != 1
          || numRequestedContainers.get() != 0) {
        LOG.info("NumAllocatedContainers is " + numAllocatedContainers.get()
            + " and NumRequestedContainers is " + numAllocatedContainers.get()
            + ".Application Master failed. exiting");
        System.exit(200);
      }
    }
  }

  public static void main(String[] args) {
    boolean result = false;
    try {
      TestDSFailedAppMaster appMaster = new TestDSFailedAppMaster();
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      if (appMaster.appAttemptID.getAttemptId() == 1) {
        try {
          // sleep some time, wait for the AM to launch a container.
          Thread.sleep(3000);
        } catch (InterruptedException e) {}
        // fail the first am.
        System.exit(100);
      }
      result = appMaster.finish();
    } catch (Throwable t) {
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }
}
