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

package org.apache.hadoop.yarn.exceptions;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * This exception is thrown when details of an unknown resource type
 * are requested.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ResourceNotFoundException extends YarnRuntimeException {
  private static final long serialVersionUID = 10081982L;
  private static final String MESSAGE = "The resource manager encountered a "
      + "problem that should not occur under normal circumstances. "
      + "Please report this error to the Hadoop community by opening a "
      + "JIRA ticket at http://issues.apache.org/jira and including the "
      + "following information:%n* Resource type requested: %s%n* Resource "
      + "object: %s%n* The stack trace for this exception: %s%n"
      + "After encountering this error, the resource manager is "
      + "in an inconsistent state. It is safe for the resource manager "
      + "to be restarted as the error encountered should be transitive. "
      + "If high availability is enabled, failing over to "
      + "a standby resource manager is also safe.";

  public ResourceNotFoundException(Resource resource, String type) {
    this(String.format(MESSAGE, type, resource,
        ExceptionUtils.getStackTrace(new Exception())));
  }

  public ResourceNotFoundException(Resource resource, String type,
      Throwable cause) {
    super(String.format(MESSAGE, type, resource,
        ExceptionUtils.getStackTrace(cause)), cause);
  }

  public ResourceNotFoundException(String message) {
    super(message);
  }
}
