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

package org.apache.slider.server.appmaster.monkey;

import org.apache.slider.server.appmaster.actions.ActionHalt;
import org.apache.slider.server.appmaster.actions.QueueAccess;

import java.util.concurrent.TimeUnit;

/**
 * Kill the AM
 */
public class ChaosKillAM implements ChaosTarget {

  public static final int DELAY = 1000;
  private final QueueAccess queues;
  private final int exitCode;

  public ChaosKillAM(QueueAccess queues, int exitCode) {
    this.queues = queues;
    this.exitCode = exitCode;
  }

  /**
   * Trigger a delayed halt
   */
  @Override
  public void chaosAction() {
    queues.schedule(new ActionHalt(exitCode, "Chaos invoked halt", DELAY,
        TimeUnit.MILLISECONDS));
  }
}
