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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;

/**
 * This exception indicate that the reservation that has been attempted, would
 * exceed the physical resources available in the {@link Plan} at the moment.
 */
@Public
@Unstable
public class ResourceOverCommitException extends PlanningException {

  private static final long serialVersionUID = 7070699407526521032L;

  public ResourceOverCommitException(String message) {
    super(message);
  }

  public ResourceOverCommitException(Throwable cause) {
    super(cause);
  }

  public ResourceOverCommitException(String message, Throwable cause) {
    super(message, cause);
  }

}
