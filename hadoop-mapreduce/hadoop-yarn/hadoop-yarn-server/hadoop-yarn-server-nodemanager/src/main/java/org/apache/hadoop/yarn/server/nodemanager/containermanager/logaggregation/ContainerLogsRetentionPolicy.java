package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

public enum ContainerLogsRetentionPolicy {
  APPLICATION_MASTER_ONLY, AM_AND_FAILED_CONTAINERS_ONLY, ALL_CONTAINERS 
}
