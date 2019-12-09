/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a state change listener that logs events at INFO level
 */
@Public
@Evolving
public class LoggingStateChangeListener implements ServiceStateChangeListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(LoggingStateChangeListener.class);

  private final Logger log;

  /**
   * Log events to the given log
   * @param log destination for events
   */
  public LoggingStateChangeListener(Logger log) {
    //force an NPE if a null log came in
    log.isDebugEnabled();
    this.log = log;
  }

  /**
   * Log events to the static log for this class
   */
  public LoggingStateChangeListener() {
    this(LOG);
  }

  /**
   * Callback for a state change event: log it
   * @param service the service that has changed.
   */
  @Override
  public void stateChanged(Service service) {
    log.info("Entry to state "  + service.getServiceState()
                 + " for " + service.getName());
  }
}
