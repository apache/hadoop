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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningService extends AbstractService implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(RunningService.class);
  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.RunningService";
  public static final int DELAY = 100;

  /**
   * Property on delay times.
   */
  public static final String DELAY_TIME = "delay.time";
  public static final String FAIL_IN_RUN = "fail.runnable";
  public static final String FAILURE_MESSAGE = "FAIL_IN_RUN";
  private boolean interrupted;

  public int delayTime = DELAY;
  public boolean failInRun;

  public RunningService() {
    super("RunningService");
  }

  public RunningService(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    delayTime = getConfig().getInt(DELAY_TIME, delayTime);
    failInRun = getConfig().getBoolean(FAIL_IN_RUN, failInRun);
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
      Thread.sleep(delayTime);
      if (failInRun) {
        noteFailure(new Exception(FAILURE_MESSAGE));
      }
    } catch (InterruptedException e) {
      interrupted = true;
      LOG.info("Interrupted");
    }
    stop();
  }

  public boolean isInterrupted() {
    return interrupted;
  }

}
