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

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

public class TrackingContext {

  private final String clientCorrelationID;

  private final String fileSystemID;

  private String clientRequestID;

  private String streamID;

  private int retryCount;

  private static final Logger LOG = LoggerFactory.getLogger(
      org.apache.hadoop.fs.azurebfs.services.AbfsClient.class);

  public static final int MAX_CLIENT_CORRELATION_ID_LENGTH = 72;

  public static final String CLIENT_CORRELATION_ID_PATTERN = "[a-zA-Z0-9-]*";

  public TrackingContext(String clientCorrelationID) {
    //validation
    if ((clientCorrelationID.length() > MAX_CLIENT_CORRELATION_ID_LENGTH)
        || (!clientCorrelationID.matches(CLIENT_CORRELATION_ID_PATTERN))) {
      this.clientCorrelationID = EMPTY_STRING;
      LOG.debug(
          "Invalid config provided; correlation id not included in header.");
    } else if (clientCorrelationID.length() > 0) {
      this.clientCorrelationID = clientCorrelationID + ":";
    } else {
      this.clientCorrelationID = EMPTY_STRING;
    }
    fileSystemID = UUID.randomUUID().toString();
    streamID = EMPTY_STRING;
  }

  public void setRetryCount(int count) {
    retryCount = count;
  }

  public void setClientRequestID() {
    clientRequestID = UUID.randomUUID().toString();
  }

  public void setStreamID(String stream) {
    streamID = stream + StringUtils.right(UUID.randomUUID().toString(), 12);
  }

  public String toString() {
    return clientCorrelationID + clientRequestID + ":" + fileSystemID + ":"
        + streamID + ":" + retryCount;
  }

}
