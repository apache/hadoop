package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;

public interface GetTaskReportsResponse {
  public abstract List<TaskReport> getTaskReportList();
  public abstract TaskReport getTaskReport(int index);
  public abstract int getTaskReportCount();
  
  public abstract void addAllTaskReports(List<TaskReport> taskReports);
  public abstract void addTaskReport(TaskReport taskReport);
  public abstract void removeTaskReport(int index);
  public abstract void clearTaskReports();
}
