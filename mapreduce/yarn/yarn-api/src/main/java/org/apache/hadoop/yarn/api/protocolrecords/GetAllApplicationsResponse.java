package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Application;

public interface GetAllApplicationsResponse {
  List<Application> getApplicationList();
  void setApplicationList(List<Application> applications);
}
