package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.test.system.JTProtocol;
import org.apache.hadoop.mapreduce.test.system.JobInfo;
import org.apache.hadoop.mapreduce.test.system.TTInfo;
import org.apache.hadoop.mapreduce.test.system.TaskInfo;

/**
 * Aspect which injects the basic protocol functionality which is to be
 * implemented by all the services which implement {@link ClientProtocol}
 * 
 * Aspect also injects default implementation for the {@link JTProtocol}
 */

public aspect JTProtocolAspect {

  // Make the ClientProtocl extend the JTprotocol
  declare parents : JobSubmissionProtocol extends JTProtocol;

  /*
   * Start of default implementation of the methods in JTProtocol
   */

  public Configuration JTProtocol.getDaemonConf() throws IOException {
    return null;
  }

  public JobInfo JTProtocol.getJobInfo(JobID jobID) throws IOException {
    return null;
  }

  public TaskInfo JTProtocol.getTaskInfo(TaskID taskID) throws IOException {
    return null;
  }

  public TTInfo JTProtocol.getTTInfo(String trackerName) throws IOException {
    return null;
  }

  public JobInfo[] JTProtocol.getAllJobInfo() throws IOException {
    return null;
  }

  public TaskInfo[] JTProtocol.getTaskInfo(JobID jobID) throws IOException {
    return null;
  }

  public TTInfo[] JTProtocol.getAllTTInfo() throws IOException {
    return null;
  }
  
  public boolean JTProtocol.isJobRetired(JobID jobID) throws IOException {
    return false;
  }
  
  public String JTProtocol.getJobHistoryLocationForRetiredJob(JobID jobID) throws IOException {
    return "";
  }
}
