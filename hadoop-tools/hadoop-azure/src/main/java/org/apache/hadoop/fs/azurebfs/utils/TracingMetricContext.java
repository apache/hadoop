package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import java.util.UUID;
import java.util.List;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

public class TracingMetricContext extends TracingContext{
  private AbfsCounters abfsCounters;
  private String header = EMPTY_STRING;

  private final String clientCorrelationID;  // passed over config by client
  private final String fileSystemID;  // GUID for fileSystem instance
  private String clientRequestId = EMPTY_STRING;
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
          + ":" + "BO:" + abfsCounters.getAbfsBackoffMetrics().toString() +
          "FO:" + getFooterMetrics();
      break;
    case INTERNAL_FOOTER_METRIC_FORMAT:
      header = clientCorrelationID + ":" + clientRequestId + ":" + fileSystemID
          + ":" + "FO:" + getFooterMetrics();
      break;
    case INTERNAL_BACKOFF_METRIC_FORMAT:
      header = clientCorrelationID + ":" + clientRequestId + ":" + fileSystemID
          + ":" + "BO:" + abfsCounters.getAbfsBackoffMetrics().toString();
      break;
    }
  }
}
