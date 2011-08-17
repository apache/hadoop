package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;

public interface GetAllApplicationsResponse {
  List<ApplicationReport> getApplicationList();
  void setApplicationList(List<ApplicationReport> applications);
}
