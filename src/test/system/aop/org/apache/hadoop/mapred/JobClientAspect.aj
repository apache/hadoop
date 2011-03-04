package org.apache.hadoop.mapred;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobID;

public privileged aspect JobClientAspect {

  public JobSubmissionProtocol JobClient.getProtocol() {
    return jobSubmitClient;
  }
  
  public void JobClient.killJob(JobID id) throws IOException {
    jobSubmitClient.killJob(
        org.apache.hadoop.mapred.JobID.downgrade(id));
  }
}
