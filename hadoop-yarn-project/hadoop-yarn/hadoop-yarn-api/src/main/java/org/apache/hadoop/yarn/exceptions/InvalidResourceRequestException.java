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

import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * This exception is thrown when a resource requested via
 * {@link ResourceRequest} in the
 * {@link ApplicationMasterProtocol#allocate(AllocateRequest)} API is out of the
 * range of the configured lower and upper limits on resources.
 * 
 */
public class InvalidResourceRequestException extends YarnException {
  public static final String LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE =
          "Invalid resource request! Cannot allocate containers as "
                  + "requested resource is less than 0! "
                  + "Requested resource type=[%s], " + "Requested resource=%s";

  public static final String GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE =
          "Invalid resource request! Cannot allocate containers as "
                  + "requested resource is greater than " +
                  "maximum allowed allocation. "
                  + "Requested resource type=[%s], "
                  + "Requested resource=%s, maximum allowed allocation=%s, "
                  + "please note that maximum allowed allocation is calculated "
                  + "by scheduler based on maximum resource of registered "
                  + "NodeManagers, which might be less than configured "
                  + "maximum allocation=%s";

  public static final String UNKNOWN_REASON_MESSAGE_TEMPLATE =
          "Invalid resource request! "
                  + "Cannot allocate containers for an unknown reason! "
                  + "Requested resource type=[%s], Requested resource=%s";

  public enum InvalidResourceType {
    LESS_THAN_ZERO, GREATER_THEN_MAX_ALLOCATION, UNKNOWN;
  }

  private static final long serialVersionUID = 13498237L;
  private final InvalidResourceType invalidResourceType;

  public InvalidResourceRequestException(Throwable cause) {
    super(cause);
    this.invalidResourceType = InvalidResourceType.UNKNOWN;
  }

  public InvalidResourceRequestException(String message) {
    this(message, InvalidResourceType.UNKNOWN);
  }

  public InvalidResourceRequestException(String message,
      InvalidResourceType invalidResourceType) {
    super(message);
    this.invalidResourceType = invalidResourceType;
  }

  public InvalidResourceRequestException(String message, Throwable cause) {
    super(message, cause);
    this.invalidResourceType = InvalidResourceType.UNKNOWN;
  }

  public InvalidResourceType getInvalidResourceType() {
    return invalidResourceType;
  }
}
