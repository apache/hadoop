/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.fs.Path;

public class ChukwaJobTrackerInstrumentation extends org.apache.hadoop.mapred.JobTrackerInstrumentation {

	  protected final JobTracker tracker;
	  private static ChukwaAgentController chukwaClient = null;
	  private static Log log = LogFactory.getLog(JobTrackerInstrumentation.class);
	  private static HashMap<JobID, Long> jobConfs = null;
	  private static HashMap<JobID, Long> jobHistories = null;

	  public ChukwaJobTrackerInstrumentation(JobTracker jt, JobConf conf) {
          super(jt,conf);
	      tracker = jt;
	      if(chukwaClient==null) {
		      chukwaClient = new ChukwaAgentController();
	      }
	      if(jobConfs==null) {
	    	  jobConfs = new HashMap<JobID, Long>();
	      }
	      if(jobHistories==null) {
	    	  jobHistories = new HashMap<JobID, Long>();
	      }
	  }

	  public void launchMap(TaskAttemptID taskAttemptID) {
		  
	  }

	  public void completeMap(TaskAttemptID taskAttemptID) {
		  
	  }

	  public void launchReduce(TaskAttemptID taskAttemptID) {
		  
	  }

	  public void completeReduce(TaskAttemptID taskAttemptID) {
		  
	  }

	  public void submitJob(JobConf conf, JobID id) {
          String chukwaJobConf = tracker.getLocalJobFilePath(id);
          try {
              String jobFileName = JobHistory.JobInfo.getJobHistoryFileName(conf, id);
              Path jobHistoryPath = JobHistory.JobInfo.getJobHistoryLogLocation(jobFileName);
              String jobConfPath = JobHistory.JobInfo.getLocalJobFilePath(id);
              long adaptorID = chukwaClient.add("org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped", "JobConf", "0 "+jobConfPath, 0);
              jobConfs.put(id, adaptorID);
              if(jobHistoryPath.toString().matches("^hdfs://")) {
                  adaptorID = chukwaClient.add("org.apache.hadoop.chukwa.datacollection.adaptor.HDFSAdaptor", "JobHistory", "0 "+jobHistoryPath.toString(), 0);
              } else {
                  adaptorID = chukwaClient.add("org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped", "JobHistory", "0 "+jobHistoryPath.toString().substring(5), 0);            	  
              }
              jobHistories.put(id, adaptorID);
          } catch(Exception ex) {
        	  
          }
      }

	  public void completeJob(JobConf conf, JobID id) {
          try {
             if (jobHistories.containsKey(id)) {
                 chukwaClient.remove(jobHistories.get(id));
             }
             if (jobConfs.containsKey(id)) {
                 chukwaClient.remove(jobConfs.get(id));
             }
          } catch(Throwable e) {
            log.warn("could not remove adaptor for this job: " + id.toString(),e);
            e.printStackTrace();
          }
	  }

}
