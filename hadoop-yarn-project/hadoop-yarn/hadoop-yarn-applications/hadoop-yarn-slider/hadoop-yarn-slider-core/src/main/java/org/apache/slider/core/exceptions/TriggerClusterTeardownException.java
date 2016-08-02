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

package org.apache.slider.core.exceptions;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

/**
 * An Exception to be thrown for an explicit "shut down the cluster" operation
 * raised by the application state or other parts of the AM
 */
public class TriggerClusterTeardownException extends SliderException {

  private final FinalApplicationStatus finalApplicationStatus;
  
  public TriggerClusterTeardownException(int code,
      FinalApplicationStatus finalApplicationStatus, String message,
      Object... args) {
    super(code, message, args);
    this.finalApplicationStatus = finalApplicationStatus;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }
}
