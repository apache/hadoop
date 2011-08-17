package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationReport;

public interface GetApplicationReportResponse {
  public abstract ApplicationReport getApplicationReport();
  public abstract void setApplicationReport(ApplicationReport ApplicationReport);
}
