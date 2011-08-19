package org.apache.hadoop.yarn.api.records;

public enum QueueACL {
  SUBMIT_JOB,
  ADMINISTER_QUEUE,    
  ADMINISTER_JOBS;            // currently unused
}