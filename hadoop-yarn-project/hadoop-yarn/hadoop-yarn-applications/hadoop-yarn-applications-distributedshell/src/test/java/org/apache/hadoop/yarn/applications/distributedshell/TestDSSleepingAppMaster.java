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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDSSleepingAppMaster extends ApplicationMaster{

  private static final Logger LOG = LoggerFactory
      .getLogger(TestDSSleepingAppMaster.class);
  private static final long SLEEP_TIME = 5000;

  public static void main(String[] args) {
    boolean result = false;
    try {
      TestDSSleepingAppMaster appMaster = new TestDSSleepingAppMaster();
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      if (appMaster.appAttemptID.getAttemptId() <= 2) {
        try {
          // sleep some time
          Thread.sleep(SLEEP_TIME);
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
