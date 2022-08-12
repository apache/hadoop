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

import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import java.util.UUID;
import java.util.List;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

public class TracingMetricContext extends TracingContext{
  private final AbfsCounters abfsCounters;
  private String header = EMPTY_STRING;

  private final String clientCorrelationID;  // passed over config by client
  private final String fileSystemID;  // GUID for fileSystem instance
  private String clientRequestId = EMPTY_STRING;
  private Listener listener = null;
  private TracingHeaderFormat tracingHeaderFormat;

  public TracingMetricContext(String clientCorrelationID, String fileSystemID,
      FSOperationType opType, boolean needsPrimaryReqId,
      TracingHeaderFormat tracingHeaderFormat, Listener listener,
      AbfsCounters abfsCounters) {
    super(clientCorrelationID, fileSystemID, opType, needsPrimaryReqId, tracingHeaderFormat, listener);
    this.clientCorrelationID = clientCorrelationID;
    this.fileSystemID = fileSystemID;
    this.tracingHeaderFormat = tracingHeaderFormat;
    this.abfsCounters = abfsCounters;
    this.listener = listener;
  }

  private String getFooterMetrics(){
    List<AbfsReadFooterMetrics> readFooterMetricsList = abfsCounters.getAbfsReadFooterMetrics();
    String readFooterMetric = "";
    if (!readFooterMetricsList.isEmpty()) {
      readFooterMetric = AbfsReadFooterMetrics.getFooterMetrics(readFooterMetricsList, readFooterMetric);
    }
    return readFooterMetric;
  }

  @Override
  public void constructHeader(AbfsHttpOperation httpOperation){
    clientRequestId = UUID.randomUUID().toString();
    switch (tracingHeaderFormat) {
    case INTERNAL_METRIC_FORMAT:
      header = clientCorrelationID + ":" + clientRequestId + ":" + fileSystemID
          + ":" + "BO:" + abfsCounters.getAbfsBackoffMetrics().toString()
          + "FO:" + getFooterMetrics();
      break;
    case INTERNAL_FOOTER_METRIC_FORMAT:
      header = clientCorrelationID + ":" + clientRequestId + ":" + fileSystemID
          + ":" + "FO:" + getFooterMetrics();
      break;
    case INTERNAL_BACKOFF_METRIC_FORMAT:
      header = clientCorrelationID + ":" + clientRequestId + ":" + fileSystemID
          + ":" + "BO:" + abfsCounters.getAbfsBackoffMetrics().toString();
      break;
    default:
      header = "";
      break;
    }
    httpOperation.setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, header);
  }
}
