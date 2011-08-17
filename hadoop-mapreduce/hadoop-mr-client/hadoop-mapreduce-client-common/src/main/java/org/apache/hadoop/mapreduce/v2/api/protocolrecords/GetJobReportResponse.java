package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.JobReport;

public interface GetJobReportResponse {
  public abstract JobReport getJobReport();
  
  public abstract void setJobReport(JobReport jobReport);
}
