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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

/**
 * Exception to be thrown when Client submit an application without
 * providing {@link ApplicationId} in {@link ApplicationSubmissionContext}.
 */
@Public
@Unstable
public class ApplicationIdNotProvidedException extends YarnException{

  private static final long serialVersionUID = 911754350L;

  public ApplicationIdNotProvidedException(Throwable cause) {
    super(cause);
  }

  public ApplicationIdNotProvidedException(String message) {
    super(message);
  }

  public ApplicationIdNotProvidedException(String message, Throwable cause) {
    super(message, cause);
  }
}
