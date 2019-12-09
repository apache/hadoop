/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.api.records;

import io.swagger.annotations.ApiModel;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The current state of an service.
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "The current state of an service.")
public enum ServiceState {
  ACCEPTED, STARTED, STABLE, STOPPED, FAILED, FLEX, UPGRADING,
  UPGRADING_AUTO_FINALIZE, EXPRESS_UPGRADING, SUCCEEDED, CANCEL_UPGRADING;

  public static boolean isUpgrading(ServiceState state) {
    return state.equals(UPGRADING) || state.equals(UPGRADING_AUTO_FINALIZE)
        || state.equals(EXPRESS_UPGRADING);
  }
}
