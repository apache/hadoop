/**
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

import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.MapTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;

/**
 * {@link JobStory} represents the runtime information available for a
 * completed Map-Reduce job.
 */
public class FakeJobs implements JobStory {
	String jobName ;
	long submissionTime = 0;
	int maps = 0;
	int reduces = 0;
	Random random = new Random();

	public FakeJobs (String name, long submissionTime, int nmaps, int nreduces) {
		jobName = name;
		this.submissionTime = submissionTime;
		this.maps = nmaps;
		this.reduces = nreduces;

	}
	public String getName() {
		return jobName;
	}
	public org.apache.hadoop.mapreduce.JobID getJobID() {
	  return null;
	}
	/**
	 * Get the user who ran the job.
	 * @return the user who ran the job
	 */
	public String getUser() {
		return "mumak";
	}

	/**
	 * Get the job submission time.
	 * @return the job submission time
	 */
	public long getSubmissionTime(){

		return submissionTime;
	}

	/**
	 * Get the number of maps in the {@link JobStory}.
	 * @return the number of maps in the <code>Job</code>
	 */
	public int getNumberMaps() {

		return maps;
	}

	/**
	 * Get the number of reduce in the {@link JobStory}.
	 * @return the number of reduces in the <code>Job</code>
	 */
	public int getNumberReduces() {

		return reduces;

	}
	/**
	 * Get the input splits for the job.
	 * @return the input splits for the job
	 */
  public InputSplit[] getInputSplits() {
    InputSplit[] retval = new InputSplit[getNumberMaps()];
    FileSplit tmp = new FileSplit(new Path("/"), 0, 0, new String[0]);

    for (int i = 0; i < retval.length; ++i) {
      retval[i] = tmp;
    }

    return retval;
  }

	/**
	 * Get {@link TaskInfo} for a given task.
	 * @param taskType {@link TaskType} of the task
	 * @param taskNumber Partition number of the task
	 * @return the <code>TaskInfo</code> for the given task
	 */
	public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {

		return null;	

	}  
	/**
	 * Get {@link TaskAttemptInfo} for a given task-attempt.
	 * @param taskType {@link TaskType} of the task-attempt
	 * @param taskNumber Partition number of the task-attempt
	 * @param taskAttemptNumber Attempt number of the task
	 * @return the <code>TaskAttemptInfo</code> for the given task-attempt
	 */
	public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, 
			int taskNumber, 
			int taskAttemptNumber) {
		int bytesin = random.nextInt()%10000;
		int recsin = bytesin/10 ;
		int bytesout = (int)(bytesin*1.5);
		int recsout = bytesout/10 ;
		int maxMem = 1000000;
		long mapRunTime = 5678;
		long reduceShuffleTime = 0;
		long reduceSortTime = 0;
		long reduceRunTime = 1234;
		TaskInfo task = new TaskInfo(bytesin, recsin, bytesout, recsout, maxMem);
		TaskAttemptInfo tAInfo = null;
		if (taskType == TaskType.MAP) {
			tAInfo = new MapTaskAttemptInfo(TaskStatus.State.SUCCEEDED,
					task, mapRunTime);
		} else if (taskType == TaskType.REDUCE) {
			tAInfo = new ReduceTaskAttemptInfo(TaskStatus.State.SUCCEEDED,
					task, reduceShuffleTime, reduceSortTime, reduceRunTime);
		} else {
		  throw new IllegalArgumentException("Unsupported TaskType "+taskType);
		}		
		return tAInfo;

	}
	public JobConf getJobConf() {
		JobConf jobConf = new JobConf();
		jobConf.setJobName(jobName);
		jobConf.setUser("mumak");
		jobConf.setNumMapTasks(maps);
		jobConf.setNumReduceTasks(reduces);
		return jobConf;
	}
	
  @Override
  public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
      int taskAttemptNumber, int locality) {
    return getTaskAttemptInfo(TaskType.MAP, taskNumber, taskAttemptNumber);
  }
  
  @Override
  public Pre21JobHistoryConstants.Values getOutcome() {
    return Pre21JobHistoryConstants.Values.SUCCESS;
  }
}
