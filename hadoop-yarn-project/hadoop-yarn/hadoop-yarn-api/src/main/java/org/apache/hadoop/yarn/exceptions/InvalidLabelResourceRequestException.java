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
 * {@link ApplicationMasterProtocol#allocate(AllocateRequest)} when requested
 * label is not having permission to access.
 *
 */
public class InvalidLabelResourceRequestException
    extends InvalidResourceRequestException {

  private static final long serialVersionUID = 13498237L;

  public InvalidLabelResourceRequestException(Throwable cause) {
    super(cause);
  }

  public InvalidLabelResourceRequestException(String message) {
    super(message);
  }

  public InvalidLabelResourceRequestException(String message, Throwable cause) {
    super(message, cause);
  }

}
