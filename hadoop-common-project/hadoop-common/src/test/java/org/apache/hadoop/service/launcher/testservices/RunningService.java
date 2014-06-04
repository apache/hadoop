/*
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

package org.apache.hadoop.service.launcher.testservices;

import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningService extends AbstractService implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(RunningService.class);
  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.RunningService"; 
  public static final int DELAY = 2000;

  /**
   * Property on delay times
   */
  public static final String DELAY_TIME = "delay.time";
  public static final String FAIL_IN_RUN = "fail.runnable";
  public static final String FAILURE_MESSAGE = "FAIL_IN_RUN";

  public boolean interrupted;
  
  public RunningService() {
    super("RunningService");
  }

  @Override
  protected void serviceStart() throws Exception {
    Thread thread = new Thread(this);
    thread.setName(getName());
    thread.start();
  }

  @Override
  public void run() {
    try {
      Thread.sleep(getConfig().getInt(DELAY_TIME, DELAY));
      if (getConfig().getBoolean(FAIL_IN_RUN, false)) {
        noteFailure(new Exception(FAILURE_MESSAGE));
      }
    } catch (InterruptedException e) {
      interrupted = true;
      LOG.info("Interrupted");
    }
    stop();
  }
}
