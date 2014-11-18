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

/**
 * Exception thrown when an update to an existing reservation is performed
 * by a user that is not the reservation owner. 
 */
@Public
@Unstable
public class MismatchedUserException extends PlanningException {

  private static final long serialVersionUID = 8313222590561668413L;

  public MismatchedUserException(String message) {
    super(message);
  }

  public MismatchedUserException(Throwable cause) {
    super(cause);
  }

  public MismatchedUserException(String message, Throwable cause) {
    super(message, cause);
  }

}