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

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.service.BreakableService;

/**
 * Laucher test service that does not take CLI arguments
 */
public class FailureTestService extends BreakableService {
  public final int delay;

  /**
   * The exception to be raised on a failure -may be null
   */
  public final Exception exceptionToRaise;

  public FailureTestService(boolean failOnInit,
      boolean failOnStart,
      boolean failOnStop,
      int delay, Exception exceptionToRaise) {
    super(failOnInit, failOnStart, failOnStop);
    this.delay = delay;
    this.exceptionToRaise = exceptionToRaise;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (delay>0) {
      Thread.sleep(delay);
    }
    super.serviceStop();
  }

  @Override
  protected Exception createFailureException(String action) {
    if (exceptionToRaise!=null) {
      return exceptionToRaise;
    } else {
      return super.createFailureException(action);
    }
  }
}
