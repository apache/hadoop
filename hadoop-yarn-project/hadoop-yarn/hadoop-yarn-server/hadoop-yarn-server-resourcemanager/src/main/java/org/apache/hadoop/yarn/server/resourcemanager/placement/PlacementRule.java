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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;

public abstract class PlacementRule {

  public String getName() {
    return this.getClass().getName();
  }

  public abstract boolean initialize(
      CapacitySchedulerContext schedulerContext) throws IOException;

  /**
   * Get queue for a given application
   * 
   * @param asc application submission context
   * @param user userName
   * 
   * @throws YarnException
   *           if any error happens
   * 
   * @return <p>
   *         non-null value means it is determined
   *         </p>
   *         <p>
   *         null value means it is undetermined, so next {@link PlacementRule}
   *         in the {@link PlacementManager} will take care
   *         </p>
   */
  public abstract ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException;
}