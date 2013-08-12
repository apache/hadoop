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
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * This exception is thrown when an application provides an invalid
 * {@link ResourceBlacklistRequest} specification for blacklisting of resources
 * in {@link ApplicationMasterProtocol#allocate(AllocateRequest)} API.
 * 
 * Currently this exceptions is thrown when an application tries to
 * blacklist {@link ResourceRequest#ANY}.
 */
public class InvalidResourceBlacklistRequestException extends YarnException {

  private static final long serialVersionUID = 384957911L;

  public InvalidResourceBlacklistRequestException(Throwable cause) {
    super(cause);
  }

  public InvalidResourceBlacklistRequestException(String message) {
    super(message);
  }

  public InvalidResourceBlacklistRequestException(String message, Throwable cause) {
    super(message, cause);
  }

}
