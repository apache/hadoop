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

/**
 * This exception is thrown when the client requests information about
 * ResourceProfiles in the
 * {@link org.apache.hadoop.yarn.api.ApplicationClientProtocol} but resource
 * profiles is not enabled on the RM.
 *
 */
public class ResourceProfilesNotEnabledException extends YarnException {

  private static final long serialVersionUID = 13498237L;

  public ResourceProfilesNotEnabledException(Throwable cause) {
    super(cause);
  }

  public ResourceProfilesNotEnabledException(String message) {
    super(message);
  }

  public ResourceProfilesNotEnabledException(String message, Throwable cause) {
    super(message, cause);
  }
}
