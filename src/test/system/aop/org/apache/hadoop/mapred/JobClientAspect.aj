package org.apache.hadoop.mapred;

public privileged aspect JobClientAspect {

  public JobSubmissionProtocol JobClient.getProtocol() {
    return jobSubmitClient;
  }
}
