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

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.CLIENT_CORRELATION_ID_PATTERN;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_CLIENT_CORRELATION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_CLIENT_CORRELATION_ID_LENGTH;

public class TrackingContext {
  private String clientCorrelationID;
  private String clientRequestID;
  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.fs.azurebfs.services.AbfsClient.class);

  public TrackingContext(String clientCorrelationID) {
    //validation
    if ((clientCorrelationID.length() > MAX_CLIENT_CORRELATION_ID_LENGTH) ||
        (!clientCorrelationID.matches(CLIENT_CORRELATION_ID_PATTERN))) {
      this.clientCorrelationID = DEFAULT_FS_AZURE_CLIENT_CORRELATION_ID;
      LOG.debug("Invalid config provided; correlation id not included in header.");
    }
    else if (clientCorrelationID.length() > 0) {
      this.clientCorrelationID = clientCorrelationID + ":";
    }
    else {
      this.clientCorrelationID = DEFAULT_FS_AZURE_CLIENT_CORRELATION_ID;
    }
  }

  public void setClientRequestID() {
    clientRequestID = UUID.randomUUID().toString();
  }

  public String toString() {
    return clientCorrelationID + clientRequestID;
  }
}
