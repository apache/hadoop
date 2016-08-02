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

package org.apache.slider.server.appmaster.actions;

import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Action to tell a queue executor to stop -after handing this on/executing it
 */
public class ActionStopQueue extends AsyncAction {
  private static final Logger log =
      LoggerFactory.getLogger(ActionStopQueue.class);
  
  public ActionStopQueue(long delay) {
    super("stop queue", delay);
  }

  public ActionStopQueue(long delay,
      TimeUnit timeUnit) {
    super("stop queue", delay, timeUnit);
  }

  public ActionStopQueue(String name,
      long delay,
      TimeUnit timeUnit) {
    super(name, delay, timeUnit);
  }

  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState) throws Exception {
    log.warn("STOP");
  }
}
